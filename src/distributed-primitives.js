// vector-clock.js + lamport-clock.js + crdt-counter.js + gossip.js
// Distributed systems primitives combined for efficiency

// ---- Lamport Clock ----
export class LamportClock {
  constructor() { this._time = 0; }
  tick() { return ++this._time; }
  update(received) { this._time = Math.max(this._time, received) + 1; return this._time; }
  get time() { return this._time; }
}

// ---- Vector Clock ----
export class VectorClock {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._clock = {};
  }

  tick() { this._clock[this.nodeId] = (this._clock[this.nodeId] || 0) + 1; return { ...this._clock }; }

  merge(other) {
    for (const [node, time] of Object.entries(other)) {
      this._clock[node] = Math.max(this._clock[node] || 0, time);
    }
    this.tick();
    return { ...this._clock };
  }

  /** Returns 'before' | 'after' | 'concurrent' | 'equal' */
  compare(a, b) {
    let aBefore = false, bBefore = false;
    const allNodes = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const node of allNodes) {
      const va = a[node] || 0, vb = b[node] || 0;
      if (va < vb) aBefore = true;
      if (va > vb) bBefore = true;
    }
    if (!aBefore && !bBefore) return 'equal';
    if (aBefore && !bBefore) return 'before';
    if (!aBefore && bBefore) return 'after';
    return 'concurrent';
  }

  get clock() { return { ...this._clock }; }
}

// ---- G-Counter CRDT ----
export class GCounter {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._counts = {}; // nodeId → count
  }

  increment(amount = 1) {
    this._counts[this.nodeId] = (this._counts[this.nodeId] || 0) + amount;
  }

  get value() { return Object.values(this._counts).reduce((s, v) => s + v, 0); }

  merge(other) {
    for (const [node, count] of Object.entries(other._counts || other)) {
      this._counts[node] = Math.max(this._counts[node] || 0, count);
    }
  }

  get state() { return { ...this._counts }; }
}

// ---- PN-Counter CRDT (supports decrement) ----
export class PNCounter {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._pos = new GCounter(nodeId);
    this._neg = new GCounter(nodeId);
  }

  increment(amount = 1) { this._pos.increment(amount); }
  decrement(amount = 1) { this._neg.increment(amount); }
  get value() { return this._pos.value - this._neg.value; }

  merge(other) {
    this._pos.merge(other._pos);
    this._neg.merge(other._neg);
  }
}

// ---- Gossip Protocol ----
export class GossipProtocol {
  constructor(nodeId, peers = []) {
    this.nodeId = nodeId;
    this.peers = peers;
    this._data = new Map(); // key → { value, version, origin }
    this._inbox = [];
    this.stats = { sent: 0, received: 0, updates: 0 };
  }

  set(key, value) {
    const existing = this._data.get(key);
    const version = existing ? existing.version + 1 : 1;
    this._data.set(key, { value, version, origin: this.nodeId });
    this.stats.updates++;
  }

  get(key) { const d = this._data.get(key); return d ? d.value : undefined; }

  /** Create a gossip message to send to a random peer */
  createGossipMessage() {
    this.stats.sent++;
    return {
      from: this.nodeId,
      data: [...this._data.entries()].map(([k, v]) => ({ key: k, ...v })),
    };
  }

  /** Receive and merge a gossip message */
  receiveGossipMessage(message) {
    this.stats.received++;
    for (const item of message.data) {
      const existing = this._data.get(item.key);
      if (!existing || item.version > existing.version) {
        this._data.set(item.key, { value: item.value, version: item.version, origin: item.origin });
        this.stats.updates++;
      }
    }
  }

  get dataCount() { return this._data.size; }
}
