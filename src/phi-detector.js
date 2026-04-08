// phi-detector.js — Phi Accrual Failure Detector
// Adaptively determines if a node has failed based on heartbeat arrival times.
// Uses exponential distribution to calculate suspicion level (phi).

export class PhiAccrualFailureDetector {
  constructor(threshold = 8, maxSampleSize = 200, minStdDevMs = 100) {
    this.threshold = threshold; // phi threshold for marking as failed
    this.maxSampleSize = maxSampleSize;
    this.minStdDevMs = minStdDevMs;
    this._intervals = []; // inter-arrival times
    this._lastHeartbeat = null;
  }

  /** Record a heartbeat arrival */
  heartbeat(now = Date.now()) {
    if (this._lastHeartbeat !== null) {
      const interval = now - this._lastHeartbeat;
      this._intervals.push(interval);
      if (this._intervals.length > this.maxSampleSize) this._intervals.shift();
    }
    this._lastHeartbeat = now;
  }

  /** Calculate phi (suspicion level) */
  phi(now = Date.now()) {
    if (this._lastHeartbeat === null || this._intervals.length < 2) return 0;
    
    const timeSinceLastHB = now - this._lastHeartbeat;
    const mean = this._mean();
    const stdDev = Math.max(this._stdDev(), this.minStdDevMs);
    
    // Phi = -log10(1 - F(timeSinceLastHB))
    // where F is the CDF of the normal distribution
    const y = (timeSinceLastHB - mean) / stdDev;
    const p = 1 / (1 + Math.exp(-y * 1.5976)); // Logistic approximation of normal CDF
    const phi = -Math.log10(1 - p);
    
    return Math.max(0, phi);
  }

  /** Check if node is considered alive */
  isAlive(now = Date.now()) {
    return this.phi(now) < this.threshold;
  }

  _mean() {
    return this._intervals.reduce((s, v) => s + v, 0) / this._intervals.length;
  }

  _stdDev() {
    const mean = this._mean();
    const variance = this._intervals.reduce((s, v) => s + (v - mean) ** 2, 0) / this._intervals.length;
    return Math.sqrt(variance);
  }

  get sampleSize() { return this._intervals.length; }
}

/**
 * B-link Tree — concurrent-safe B+tree variant.
 * Each node has a "link" pointer to its right sibling for lock-coupling.
 */
export class BLinkTree {
  constructor(order = 4) {
    this.order = order;
    this.root = this._createLeaf();
  }

  _createLeaf() { return { type: 'leaf', keys: [], values: [], link: null }; }
  _createInternal() { return { type: 'internal', keys: [], children: [], link: null, highKey: Infinity }; }

  insert(key, value) {
    const path = this._findPath(key);
    const leaf = path[path.length - 1];
    
    const idx = this._bisect(leaf.keys, key);
    if (idx < leaf.keys.length && leaf.keys[idx] === key) {
      leaf.values[idx] = value;
      return;
    }
    
    leaf.keys.splice(idx, 0, key);
    leaf.values.splice(idx, 0, value);
    
    if (leaf.keys.length >= this.order) {
      this._splitLeaf(leaf, path);
    }
  }

  search(key) {
    let node = this.root;
    while (node.type === 'internal') {
      // Follow link if key > highKey (concurrent modification)
      while (node.link && key >= node.highKey) node = node.link;
      const idx = this._findChild(node, key);
      node = node.children[idx];
    }
    // Follow leaf links
    while (node.link && node.keys.length > 0 && key > node.keys[node.keys.length - 1]) node = node.link;
    const idx = this._bisect(node.keys, key);
    return idx < node.keys.length && node.keys[idx] === key ? node.values[idx] : undefined;
  }

  /** Range scan using leaf links */
  range(lo, hi) {
    let node = this.root;
    while (node.type === 'internal') {
      const idx = this._findChild(node, lo);
      node = node.children[idx];
    }
    
    const results = [];
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        if (node.keys[i] >= lo && node.keys[i] <= hi) results.push([node.keys[i], node.values[i]]);
        if (node.keys[i] > hi) return results;
      }
      node = node.link;
    }
    return results;
  }

  _findPath(key) {
    const path = [];
    let node = this.root;
    while (node.type === 'internal') {
      path.push(node);
      const idx = this._findChild(node, key);
      node = node.children[idx];
    }
    path.push(node);
    return path;
  }

  _splitLeaf(leaf, path) {
    const mid = Math.floor(leaf.keys.length / 2);
    const newLeaf = this._createLeaf();
    newLeaf.keys = leaf.keys.splice(mid);
    newLeaf.values = leaf.values.splice(mid);
    newLeaf.link = leaf.link;
    leaf.link = newLeaf;
    
    if (path.length <= 1) {
      // Split root
      const newRoot = this._createInternal();
      newRoot.keys = [newLeaf.keys[0]];
      newRoot.children = [leaf, newLeaf];
      newRoot.highKey = Infinity;
      this.root = newRoot;
    } else {
      const parent = path[path.length - 2];
      const idx = this._findChild(parent, newLeaf.keys[0]);
      parent.keys.splice(idx, 0, newLeaf.keys[0]);
      parent.children.splice(idx + 1, 0, newLeaf);
    }
  }

  _findChild(node, key) {
    let i = 0;
    while (i < node.keys.length && key >= node.keys[i]) i++;
    return i;
  }

  _bisect(arr, key) {
    let lo = 0, hi = arr.length;
    while (lo < hi) { const mid = (lo + hi) >> 1; arr[mid] < key ? lo = mid + 1 : hi = mid; }
    return lo;
  }
}
