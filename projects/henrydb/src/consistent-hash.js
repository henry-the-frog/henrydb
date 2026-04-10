// consistent-hash.js — Consistent Hashing with Virtual Nodes
//
// Distributes keys across nodes on a hash ring.
// Virtual nodes (vnodes) ensure even distribution.
// Adding/removing nodes only redistributes ~1/N of keys.
//
// Used in: DynamoDB, Cassandra, Riak, memcached, CDNs.

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/**
 * ConsistentHashRing — distributes keys across nodes.
 */
export class ConsistentHashRing {
  /**
   * @param {number} vnodeCount - Virtual nodes per physical node (default 150)
   */
  constructor(vnodeCount = 150) {
    this._vnodeCount = vnodeCount;
    this._ring = [];           // Sorted array of {hash, nodeId}
    this._nodes = new Map();   // nodeId → metadata
  }

  get nodeCount() { return this._nodes.size; }
  get ringSize() { return this._ring.length; }

  /**
   * Add a physical node to the ring.
   */
  addNode(nodeId, metadata = {}) {
    if (this._nodes.has(nodeId)) return;
    this._nodes.set(nodeId, metadata);
    
    // Add virtual nodes
    for (let i = 0; i < this._vnodeCount; i++) {
      const hash = fnv1a(`${nodeId}:vnode:${i}`);
      this._ring.push({ hash, nodeId });
    }
    
    // Keep ring sorted
    this._ring.sort((a, b) => a.hash - b.hash);
  }

  /**
   * Remove a physical node and all its virtual nodes.
   */
  removeNode(nodeId) {
    this._nodes.delete(nodeId);
    this._ring = this._ring.filter(v => v.nodeId !== nodeId);
  }

  /**
   * Get the node responsible for a key.
   */
  getNode(key) {
    if (this._ring.length === 0) return null;
    const hash = fnv1a(String(key));
    
    // Binary search for the first ring position >= hash
    let lo = 0, hi = this._ring.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    
    // Wrap around if past the last position
    if (this._ring[lo].hash < hash) lo = 0;
    
    return this._ring[lo].nodeId;
  }

  /**
   * Get the N nodes responsible for a key (for replication).
   * Returns unique node IDs in ring order.
   */
  getNodes(key, count = 3) {
    if (this._ring.length === 0) return [];
    const hash = fnv1a(String(key));
    
    // Find starting position
    let lo = 0, hi = this._ring.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    if (this._ring[lo].hash < hash) lo = 0;
    
    // Walk the ring collecting unique nodes
    const result = [];
    const seen = new Set();
    for (let i = 0; i < this._ring.length && result.length < count; i++) {
      const idx = (lo + i) % this._ring.length;
      const nodeId = this._ring[idx].nodeId;
      if (!seen.has(nodeId)) {
        seen.add(nodeId);
        result.push(nodeId);
      }
    }
    
    return result;
  }

  /**
   * Get the distribution of keys across nodes (for diagnostics).
   * Returns a Map<nodeId, count>.
   */
  getDistribution(keys) {
    const dist = new Map();
    for (const nodeId of this._nodes.keys()) dist.set(nodeId, 0);
    
    for (const key of keys) {
      const node = this.getNode(key);
      dist.set(node, (dist.get(node) || 0) + 1);
    }
    
    return dist;
  }

  /**
   * Compute standard deviation of distribution (lower = more even).
   */
  distributionStdDev(keys) {
    const dist = this.getDistribution(keys);
    const counts = [...dist.values()];
    const mean = counts.reduce((a, b) => a + b, 0) / counts.length;
    const variance = counts.reduce((s, c) => s + (c - mean) ** 2, 0) / counts.length;
    return Math.sqrt(variance);
  }

  /**
   * Simulate adding a node: returns the fraction of keys that would move.
   */
  simulateAddNode(nodeId, keys) {
    // Get current assignments
    const before = {};
    for (const key of keys) before[key] = this.getNode(key);
    
    // Add node
    this.addNode(nodeId);
    
    // Count moved keys
    let moved = 0;
    for (const key of keys) {
      if (this.getNode(key) !== before[key]) moved++;
    }
    
    return { moved, total: keys.length, fraction: moved / keys.length };
  }
}
