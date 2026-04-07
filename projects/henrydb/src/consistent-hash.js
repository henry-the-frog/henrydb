// consistent-hash.js — Consistent hashing ring for HenryDB
// Used for data distribution across nodes (sharding).
// Key insight: when adding/removing nodes, only K/N keys need to move
// (where K = total keys, N = total nodes).

/**
 * Consistent Hash Ring with virtual nodes.
 */
export class ConsistentHashRing {
  constructor(virtualNodes = 150) {
    this._virtualNodes = virtualNodes;
    this._ring = new Map(); // hash → { node, virtualId }
    this._sortedHashes = []; // Sorted hash positions on the ring
    this._nodes = new Set();
  }

  /**
   * Add a node to the ring.
   * Creates `virtualNodes` positions on the ring for better distribution.
   */
  addNode(node) {
    if (this._nodes.has(node)) return;
    this._nodes.add(node);
    
    for (let i = 0; i < this._virtualNodes; i++) {
      const hash = this._hash(`${node}:${i}`);
      this._ring.set(hash, { node, virtualId: i });
    }
    
    this._rebuildSorted();
  }

  /**
   * Remove a node from the ring.
   */
  removeNode(node) {
    if (!this._nodes.has(node)) return;
    this._nodes.delete(node);
    
    for (let i = 0; i < this._virtualNodes; i++) {
      const hash = this._hash(`${node}:${i}`);
      this._ring.delete(hash);
    }
    
    this._rebuildSorted();
  }

  /**
   * Get the node responsible for a given key.
   * Walks clockwise from the key's hash position to find the first node.
   */
  getNode(key) {
    if (this._sortedHashes.length === 0) return null;
    
    const hash = this._hash(String(key));
    
    // Binary search for the first hash >= key hash
    let lo = 0, hi = this._sortedHashes.length - 1;
    
    if (hash > this._sortedHashes[hi]) {
      // Wrap around to first node
      return this._ring.get(this._sortedHashes[0]).node;
    }
    
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._sortedHashes[mid] < hash) lo = mid + 1;
      else hi = mid;
    }
    
    return this._ring.get(this._sortedHashes[lo]).node;
  }

  /**
   * Get N nodes responsible for a key (for replication).
   */
  getNodes(key, count = 3) {
    if (this._sortedHashes.length === 0) return [];
    
    const hash = this._hash(String(key));
    const nodes = [];
    const seen = new Set();
    
    // Find starting position
    let startIdx = 0;
    for (let i = 0; i < this._sortedHashes.length; i++) {
      if (this._sortedHashes[i] >= hash) { startIdx = i; break; }
    }
    
    // Walk clockwise collecting unique nodes
    for (let i = 0; i < this._sortedHashes.length && nodes.length < count; i++) {
      const idx = (startIdx + i) % this._sortedHashes.length;
      const node = this._ring.get(this._sortedHashes[idx]).node;
      if (!seen.has(node)) {
        seen.add(node);
        nodes.push(node);
      }
    }
    
    return nodes;
  }

  /**
   * Get distribution of keys across nodes.
   */
  getDistribution(keys) {
    const dist = {};
    for (const node of this._nodes) dist[node] = 0;
    
    for (const key of keys) {
      const node = this.getNode(key);
      if (node) dist[node]++;
    }
    return dist;
  }

  get nodeCount() { return this._nodes.size; }
  get ringSize() { return this._sortedHashes.length; }

  _rebuildSorted() {
    this._sortedHashes = [...this._ring.keys()].sort((a, b) => a - b);
  }

  /**
   * FNV-1a hash (consistent with other HenryDB hash functions).
   */
  _hash(str) {
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }
}
