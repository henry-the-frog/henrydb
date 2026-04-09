// consistent-hash.js — Consistent hashing with virtual nodes
// Used in distributed databases for key-to-node mapping with minimal
// key redistribution when nodes join/leave (DynamoDB, Cassandra, Riak).

export class ConsistentHash {
  /**
   * @param {number} virtualNodes - Virtual nodes per physical node (default 150)
   */
  constructor(virtualNodes = 150) {
    this._vnodes = virtualNodes;
    this._ring = [];       // Sorted array of {hash, node}
    this._nodes = new Set();
  }

  get nodeCount() { return this._nodes.size; }

  /**
   * Add a node to the ring.
   */
  addNode(node) {
    if (this._nodes.has(node)) return;
    this._nodes.add(node);
    for (let i = 0; i < this._vnodes; i++) {
      const hash = this._hash(`${node}:${i}`);
      this._ring.push({ hash, node });
    }
    this._ring.sort((a, b) => a.hash - b.hash);
  }

  /**
   * Remove a node from the ring. Keys migrate to next node.
   */
  removeNode(node) {
    if (!this._nodes.has(node)) return;
    this._nodes.delete(node);
    this._ring = this._ring.filter(v => v.node !== node);
  }

  /**
   * Get the node responsible for a key.
   */
  getNode(key) {
    if (this._ring.length === 0) return null;
    const hash = this._hash(key);
    
    // Binary search for first vnode >= hash
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    
    // Wrap around
    if (lo >= this._ring.length) lo = 0;
    return this._ring[lo].node;
  }

  /**
   * Get N replica nodes for a key (for replication).
   */
  getNodes(key, n) {
    if (this._ring.length === 0) return [];
    const hash = this._hash(key);
    
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    
    const result = [];
    const seen = new Set();
    for (let i = 0; i < this._ring.length && result.length < n; i++) {
      const idx = (lo + i) % this._ring.length;
      const node = this._ring[idx].node;
      if (!seen.has(node)) {
        seen.add(node);
        result.push(node);
      }
    }
    return result;
  }

  /**
   * Get load distribution across nodes.
   */
  getDistribution(keys) {
    const dist = new Map();
    for (const node of this._nodes) dist.set(node, 0);
    for (const key of keys) {
      const node = this.getNode(key);
      dist.set(node, (dist.get(node) || 0) + 1);
    }
    return Object.fromEntries(dist);
  }

  // MurmurHash3 finalizer-based hash (better distribution for consistent hashing)
  _hash(str) {
    let h = 0;
    for (let i = 0; i < str.length; i++) {
      h = Math.imul(h ^ str.charCodeAt(i), 0x5bd1e995);
      h ^= h >>> 13;
    }
    // Avalanche
    h ^= h >>> 16;
    h = Math.imul(h, 0x85ebca6b);
    h ^= h >>> 13;
    h = Math.imul(h, 0xc2b2ae35);
    h ^= h >>> 16;
    return h >>> 0;
  }
}
