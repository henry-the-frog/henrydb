// crdt.js — Conflict-Free Replicated Data Types
//
// Data structures that can be replicated across nodes and merged
// without conflicts. No coordination protocol needed.
//
// Three CRDTs:
//   1. G-Counter — grow-only counter (increment only)
//   2. PN-Counter — positive-negative counter (increment + decrement)
//   3. OR-Set (Observed-Remove Set) — add + remove with unique tags
//
// Used in: Riak (OR-Set), Redis (CRDB), Automerge, Yjs

/**
 * G-Counter — Grow-only Counter.
 * Each node maintains its own counter. Total = sum of all counters.
 * Merge = pointwise max.
 */
export class GCounter {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._counts = new Map();
  }

  increment(n = 1) {
    this._counts.set(this.nodeId, (this._counts.get(this.nodeId) || 0) + n);
  }

  value() {
    let total = 0;
    for (const c of this._counts.values()) total += c;
    return total;
  }

  merge(other) {
    for (const [nodeId, count] of other._counts) {
      this._counts.set(nodeId, Math.max(this._counts.get(nodeId) || 0, count));
    }
  }

  clone() {
    const g = new GCounter(this.nodeId);
    for (const [k, v] of this._counts) g._counts.set(k, v);
    return g;
  }
}

/**
 * PN-Counter — Positive-Negative Counter.
 * Two G-Counters: one for increments (P), one for decrements (N).
 * Value = P.value() - N.value()
 */
export class PNCounter {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._p = new GCounter(nodeId); // Positive
    this._n = new GCounter(nodeId); // Negative
  }

  increment(n = 1) { this._p.increment(n); }
  decrement(n = 1) { this._n.increment(n); }
  value() { return this._p.value() - this._n.value(); }

  merge(other) {
    this._p.merge(other._p);
    this._n.merge(other._n);
  }

  clone() {
    const pn = new PNCounter(this.nodeId);
    pn._p = this._p.clone();
    pn._n = this._n.clone();
    return pn;
  }
}

/**
 * OR-Set (Observed-Remove Set).
 * Each add() generates a unique tag. Remove removes all currently observed tags.
 * Concurrent add+remove: add wins (the new tag wasn't observed by the remove).
 */
export class ORSet {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._elements = new Map(); // value → Set<tag>
    this._tagCounter = 0;
  }

  _newTag() {
    return `${this.nodeId}:${++this._tagCounter}`;
  }

  add(value) {
    if (!this._elements.has(value)) this._elements.set(value, new Set());
    this._elements.get(value).add(this._newTag());
  }

  remove(value) {
    // Remove all currently observed tags for this value
    this._elements.delete(value);
  }

  has(value) {
    const tags = this._elements.get(value);
    return tags ? tags.size > 0 : false;
  }

  values() {
    const result = [];
    for (const [value, tags] of this._elements) {
      if (tags.size > 0) result.push(value);
    }
    return result;
  }

  get size() { return this.values().length; }

  /**
   * Merge: union of all tags for each element.
   * An element is present if it has any tag.
   */
  merge(other) {
    for (const [value, otherTags] of other._elements) {
      if (!this._elements.has(value)) {
        this._elements.set(value, new Set(otherTags));
      } else {
        for (const tag of otherTags) {
          this._elements.get(value).add(tag);
        }
      }
    }
  }

  clone() {
    const s = new ORSet(this.nodeId);
    s._tagCounter = this._tagCounter;
    for (const [value, tags] of this._elements) {
      s._elements.set(value, new Set(tags));
    }
    return s;
  }
}

/**
 * LWW-Register (Last-Writer-Wins Register).
 * Simple register where the latest timestamp wins.
 */
export class LWWRegister {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._value = null;
    this._timestamp = 0;
  }

  set(value, timestamp = Date.now()) {
    if (timestamp >= this._timestamp) {
      this._value = value;
      this._timestamp = timestamp;
    }
  }

  get() { return this._value; }
  
  merge(other) {
    if (other._timestamp > this._timestamp) {
      this._value = other._value;
      this._timestamp = other._timestamp;
    }
  }
}
