// consistent-hash.js — Consistent hashing ring for distributed systems
// Uses SHA-256 for position computation. Virtual nodes for load balance.

import { sha256 } from './sha256.js';

/**
 * ConsistentHashRing — maps keys to nodes with minimal redistribution on changes.
 * 
 * Each physical node gets `virtualNodes` positions on the ring.
 * Keys are assigned to the first node clockwise from their hash position.
 * Adding/removing a node only redistributes ~1/N of keys (vs. rehashing all).
 * 
 * Used in: Amazon DynamoDB, Apache Cassandra, Akamai CDN, memcached.
 */
export class ConsistentHashRing {
  /**
   * @param {number} [virtualNodes=150] — number of virtual nodes per physical node
   */
  constructor(virtualNodes = 150) {
    this._vnodes = virtualNodes;
    this._ring = [];  // Sorted array of { position: number, node: string }
    this._nodes = new Set();
  }

  /** Get the number of physical nodes. */
  get nodeCount() { return this._nodes.size; }
  
  /** Get the number of virtual nodes on the ring. */
  get ringSize() { return this._ring.length; }

  /**
   * Add a physical node to the ring.
   * Creates `virtualNodes` positions on the ring.
   * @param {string} node — node identifier
   */
  addNode(node) {
    if (this._nodes.has(node)) return;
    this._nodes.add(node);
    
    for (let i = 0; i < this._vnodes; i++) {
      const position = this._hash(`${node}:${i}`);
      this._ring.push({ position, node });
    }
    
    // Keep ring sorted by position
    this._ring.sort((a, b) => a.position - b.position);
  }

  /**
   * Remove a physical node from the ring.
   * @param {string} node
   */
  removeNode(node) {
    if (!this._nodes.has(node)) return;
    this._nodes.delete(node);
    this._ring = this._ring.filter(entry => entry.node !== node);
  }

  /**
   * Get the node responsible for a key.
   * Finds the first node clockwise from the key's hash position.
   * @param {string} key
   * @returns {string|null} — node identifier, or null if ring is empty
   */
  getNode(key) {
    if (this._ring.length === 0) return null;
    
    const position = this._hash(key);
    
    // Binary search for the first position >= key's position
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this._ring[mid].position < position) lo = mid + 1;
      else hi = mid;
    }
    
    // Wrap around to the first node if past the end
    const idx = lo % this._ring.length;
    return this._ring[idx].node;
  }

  /**
   * Get N nodes responsible for a key (for replication).
   * Returns distinct physical nodes in clockwise order.
   * @param {string} key
   * @param {number} count — number of replicas
   * @returns {string[]}
   */
  getNodes(key, count) {
    if (this._ring.length === 0) return [];
    count = Math.min(count, this._nodes.size);
    
    const position = this._hash(key);
    const nodes = [];
    const seen = new Set();
    
    // Binary search for starting position
    let lo = 0, hi = this._ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this._ring[mid].position < position) lo = mid + 1;
      else hi = mid;
    }
    
    // Walk clockwise, collecting distinct physical nodes
    for (let i = 0; i < this._ring.length && nodes.length < count; i++) {
      const idx = (lo + i) % this._ring.length;
      const node = this._ring[idx].node;
      if (!seen.has(node)) {
        seen.add(node);
        nodes.push(node);
      }
    }
    
    return nodes;
  }

  /**
   * Get the distribution of keys across nodes.
   * @param {string[]} keys
   * @returns {Map<string, number>} — node → key count
   */
  getDistribution(keys) {
    const dist = new Map();
    for (const node of this._nodes) dist.set(node, 0);
    
    for (const key of keys) {
      const node = this.getNode(key);
      if (node) dist.set(node, (dist.get(node) || 0) + 1);
    }
    
    return dist;
  }

  /**
   * Compute how many keys would move when a node is added.
   * Doesn't actually modify the ring.
   * @param {string} newNode
   * @param {string[]} keys
   * @returns {{ moved: number, total: number, percent: number }}
   */
  simulateAddNode(newNode, keys) {
    // Get current assignments
    const before = new Map();
    for (const key of keys) before.set(key, this.getNode(key));
    
    // Add node temporarily
    this.addNode(newNode);
    let moved = 0;
    for (const key of keys) {
      if (this.getNode(key) !== before.get(key)) moved++;
    }
    this.removeNode(newNode);
    
    return { moved, total: keys.length, percent: (moved / keys.length * 100).toFixed(2) };
  }

  // ---- Internal ----

  /** Hash a string to a 32-bit position on the ring. */
  _hash(str) {
    const hash = sha256(str);
    // Use first 8 hex chars (32 bits) as position
    return parseInt(hash.slice(0, 8), 16);
  }
}
