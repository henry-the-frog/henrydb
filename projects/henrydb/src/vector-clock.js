// vector-clock.js — Vector Clocks for Distributed Event Ordering
//
// Captures causal ordering of events across distributed nodes.
// Can determine: A happened-before B, B happened-before A, or concurrent.
//
// Rules:
// - Local event: increment own counter
// - Send message: increment own counter, attach clock
// - Receive message: merge (pointwise max), then increment own counter
//
// Used in: DynamoDB (conflict detection), Riak (sibling versions), CRDTs

/**
 * VectorClock — tracks logical time across N processes.
 */
export class VectorClock {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._clock = new Map(); // nodeId → counter
    if (nodeId) this._clock.set(nodeId, 0);
  }

  /** Get the counter for a node */
  get(nodeId) { return this._clock.get(nodeId) || 0; }
  
  /** Get all entries */
  entries() { return [...this._clock.entries()]; }

  /** Increment local counter (local event or send) */
  increment() {
    this._clock.set(this.nodeId, this.get(this.nodeId) + 1);
    return this;
  }

  /** Merge with another clock (receive event): pointwise max */
  merge(other) {
    for (const [nodeId, counter] of other._clock) {
      this._clock.set(nodeId, Math.max(this.get(nodeId), counter));
    }
    return this;
  }

  /** Clone this clock */
  clone() {
    const vc = new VectorClock(this.nodeId);
    for (const [id, counter] of this._clock) {
      vc._clock.set(id, counter);
    }
    return vc;
  }

  /**
   * Compare with another clock.
   * Returns:
   *   'before'     — this happened-before other (this < other)
   *   'after'      — this happened-after other (this > other)
   *   'concurrent' — neither happened-before the other
   *   'equal'      — same logical time
   */
  compare(other) {
    let lessThan = false;
    let greaterThan = false;
    
    // Check all nodes in both clocks
    const allNodes = new Set([...this._clock.keys(), ...other._clock.keys()]);
    
    for (const nodeId of allNodes) {
      const a = this.get(nodeId);
      const b = other.get(nodeId);
      if (a < b) lessThan = true;
      if (a > b) greaterThan = true;
    }
    
    if (!lessThan && !greaterThan) return 'equal';
    if (lessThan && !greaterThan) return 'before';
    if (!lessThan && greaterThan) return 'after';
    return 'concurrent';
  }

  /** Check if this happened-before other */
  happenedBefore(other) { return this.compare(other) === 'before'; }
  
  /** Check if events are concurrent */
  isConcurrent(other) { return this.compare(other) === 'concurrent'; }

  toString() {
    const parts = [...this._clock.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([id, c]) => `${id}:${c}`);
    return `{${parts.join(', ')}}`;
  }
}

/**
 * DistributedNode — simulates a process that uses vector clocks.
 */
export class DistributedNode {
  constructor(id) {
    this.id = id;
    this.clock = new VectorClock(id);
    this.events = []; // [{type, clock, data}]
  }

  /** Record a local event */
  localEvent(data) {
    this.clock.increment();
    this.events.push({ type: 'local', clock: this.clock.clone(), data });
    return this.clock.clone();
  }

  /** Send a message (returns the clock to attach) */
  send(data) {
    this.clock.increment();
    const msgClock = this.clock.clone();
    this.events.push({ type: 'send', clock: msgClock, data });
    return msgClock;
  }

  /** Receive a message with an attached clock */
  receive(msgClock, data) {
    this.clock.merge(msgClock).increment();
    this.events.push({ type: 'receive', clock: this.clock.clone(), data });
    return this.clock.clone();
  }
}

/**
 * CausalHistory — detect conflicts using vector clocks.
 * Used in key-value stores for conflict detection.
 */
export class CausalHistory {
  constructor() {
    this._versions = new Map(); // key → [{value, clock}]
  }

  /**
   * Write a value with a causal context (vector clock).
   * If the new write descends from all existing versions, replace them.
   * If concurrent, keep as siblings (conflict).
   */
  write(key, value, clock) {
    const existing = this._versions.get(key) || [];
    
    // Filter out versions that the new clock descends from
    const survivors = existing.filter(v => {
      const cmp = clock.compare(v.clock);
      return cmp === 'concurrent'; // Keep concurrent siblings
    });
    
    survivors.push({ value, clock: clock.clone() });
    this._versions.set(key, survivors);
    
    return {
      siblings: survivors.length,
      conflict: survivors.length > 1,
    };
  }

  /**
   * Read a key. Returns all versions (siblings if conflicted).
   */
  read(key) {
    return this._versions.get(key) || [];
  }

  /**
   * Resolve a conflict by writing a merged value that descends from all siblings.
   */
  resolve(key, value, clock) {
    this._versions.set(key, [{ value, clock: clock.clone() }]);
  }
}
