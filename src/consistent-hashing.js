// consistent-hashing.js — Consistent hashing ring
// Maps keys to nodes on a hash ring. When a node is added/removed,
// only keys near that node are remapped. Uses virtual nodes for balance.

export class ConsistentHashRing {
  constructor(virtualNodes = 150) {
    this.virtualNodes = virtualNodes;
    this._ring = []; // sorted [{hash, node}]
    this._nodes = new Set();
  }

  addNode(node) {
    this._nodes.add(node);
    for (let i = 0; i < this.virtualNodes; i++) {
      this._ring.push({ hash: this._hash(`${node}#${i}`), node });
    }
    this._ring.sort((a, b) => a.hash - b.hash);
  }

  removeNode(node) {
    this._nodes.delete(node);
    this._ring = this._ring.filter(e => e.node !== node);
  }

  getNode(key) {
    if (this._ring.length === 0) return null;
    const h = this._hash(key);
    // Binary search for first entry with hash >= h
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < h) lo = mid + 1;
      else hi = mid;
    }
    return this._ring[lo % this._ring.length].node;
  }

  /** Get N unique nodes for replication */
  getNodes(key, count) {
    if (this._ring.length === 0) return [];
    const h = this._hash(key);
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < h) lo = mid + 1;
      else hi = mid;
    }

    const result = [];
    const seen = new Set();
    for (let i = 0; i < this._ring.length && result.length < count; i++) {
      const entry = this._ring[(lo + i) % this._ring.length];
      if (!seen.has(entry.node)) {
        seen.add(entry.node);
        result.push(entry.node);
      }
    }
    return result;
  }

  get nodeCount() { return this._nodes.size; }
  get ringSize() { return this._ring.length; }

  _hash(key) {
    let h = 0x811c9dc5;
    const s = String(key);
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return h >>> 0;
  }
}
