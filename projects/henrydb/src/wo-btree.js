// wo-btree.js — Write-Optimized B-tree (Bε-tree style)
// Internal nodes buffer insertions until full, then flush to children.
// Optimized for write-heavy workloads (LSM + B-tree hybrid).

export class WOBTree {
  constructor(order = 4, bufferSize = 8) {
    this.order = order;
    this.bufferSize = bufferSize;
    this.root = this._createLeaf();
    this.stats = { inserts: 0, flushes: 0, searches: 0 };
  }

  _createLeaf() { return { type: 'leaf', keys: [], values: [] }; }
  _createInternal() { return { type: 'internal', keys: [], children: [], buffer: [] }; }

  insert(key, value) {
    this.stats.inserts++;
    this._insertIntoNode(this.root, key, value);
    
    // Handle root split
    if (this.root.type === 'leaf' && this.root.keys.length >= this.order) {
      this._splitLeafRoot();
    }
  }

  _insertIntoNode(node, key, value) {
    if (node.type === 'leaf') {
      const idx = this._bisect(node.keys, key);
      if (idx < node.keys.length && node.keys[idx] === key) {
        node.values[idx] = value; // Update
      } else {
        node.keys.splice(idx, 0, key);
        node.values.splice(idx, 0, value);
      }
      return;
    }

    // Internal node: buffer the insert
    node.buffer.push({ key, value });
    
    // Flush if buffer is full
    if (node.buffer.length >= this.bufferSize) {
      this._flushBuffer(node);
    }
  }

  _flushBuffer(node) {
    this.stats.flushes++;
    const buf = node.buffer.sort((a, b) => a.key - b.key);
    node.buffer = [];
    
    for (const { key, value } of buf) {
      const childIdx = this._findChild(node, key);
      const child = node.children[childIdx];
      this._insertIntoNode(child, key, value);
      
      // Split child if needed
      if (child.type === 'leaf' && child.keys.length >= this.order) {
        this._splitLeafChild(node, childIdx);
      }
    }
  }

  _splitLeafRoot() {
    const leaf = this.root;
    const mid = Math.floor(leaf.keys.length / 2);
    const newLeaf = this._createLeaf();
    newLeaf.keys = leaf.keys.splice(mid);
    newLeaf.values = leaf.values.splice(mid);
    
    const newRoot = this._createInternal();
    newRoot.keys = [newLeaf.keys[0]];
    newRoot.children = [leaf, newLeaf];
    this.root = newRoot;
  }

  _splitLeafChild(parent, childIdx) {
    const child = parent.children[childIdx];
    const mid = Math.floor(child.keys.length / 2);
    const newLeaf = this._createLeaf();
    newLeaf.keys = child.keys.splice(mid);
    newLeaf.values = child.values.splice(mid);
    
    parent.keys.splice(childIdx, 0, newLeaf.keys[0]);
    parent.children.splice(childIdx + 1, 0, newLeaf);
  }

  search(key) {
    this.stats.searches++;
    return this._searchNode(this.root, key);
  }

  _searchNode(node, key) {
    if (node.type === 'leaf') {
      const idx = this._bisect(node.keys, key);
      return idx < node.keys.length && node.keys[idx] === key ? node.values[idx] : undefined;
    }

    // Check buffer first — return the LAST (most recent) entry for this key
    let bufferedValue;
    let found = false;
    for (const entry of node.buffer) {
      if (entry.key === key) { bufferedValue = entry.value; found = true; }
    }
    if (found) return bufferedValue;

    // Descend to child
    const childIdx = this._findChild(node, key);
    return this._searchNode(node.children[childIdx], key);
  }

  range(lo, hi) {
    // Flush all buffers first for consistent reads
    this._flushAll(this.root);
    return this._rangeLeaf(this.root, lo, hi);
  }

  _flushAll(node) {
    if (node.type === 'internal') {
      if (node.buffer.length > 0) this._flushBuffer(node);
      for (const child of node.children) this._flushAll(child);
    }
  }

  _rangeLeaf(node, lo, hi) {
    if (node.type === 'leaf') {
      return node.keys
        .map((k, i) => [k, node.values[i]])
        .filter(([k]) => k >= lo && k <= hi);
    }
    const results = [];
    for (const child of node.children) {
      results.push(...this._rangeLeaf(child, lo, hi));
    }
    return results;
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

  get size() {
    this._flushAll(this.root);
    return this._countLeaf(this.root);
  }

  _countLeaf(node) {
    if (node.type === 'leaf') return node.keys.length;
    return node.children.reduce((s, c) => s + this._countLeaf(c), 0);
  }
}
