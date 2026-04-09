// art.js — Adaptive Radix Tree (ART)
// Space-efficient trie variant that adapts node size based on children count.
// Used in: HyPer (TUM), DuckDB, ClickHouse for in-memory indexes.
//
// Node types:
//   Node4   (1-4 children)  — linear search
//   Node16  (5-16 children) — SIMD-friendly linear search
//   Node48  (17-48 children) — indirect array
//   Node256 (49-256 children) — direct array
//
// Key insight: most trie nodes are sparse, so Node4/16 save massive memory.

class Node4 {
  constructor() { this.keys = []; this.children = []; this.value = undefined; this.hasValue = false; }
  get type() { return 4; }
  get count() { return this.keys.length; }

  find(byte) {
    const idx = this.keys.indexOf(byte);
    return idx >= 0 ? this.children[idx] : null;
  }

  insert(byte, child) {
    this.keys.push(byte);
    this.children.push(child);
  }

  isFull() { return this.keys.length >= 4; }

  grow() {
    const n16 = new Node16();
    for (let i = 0; i < this.keys.length; i++) {
      n16.keys.push(this.keys[i]);
      n16.children.push(this.children[i]);
    }
    n16.value = this.value;
    n16.hasValue = this.hasValue;
    return n16;
  }
}

class Node16 {
  constructor() { this.keys = []; this.children = []; this.value = undefined; this.hasValue = false; }
  get type() { return 16; }
  get count() { return this.keys.length; }

  find(byte) {
    const idx = this.keys.indexOf(byte);
    return idx >= 0 ? this.children[idx] : null;
  }

  insert(byte, child) {
    this.keys.push(byte);
    this.children.push(child);
  }

  isFull() { return this.keys.length >= 16; }

  grow() {
    const n48 = new Node48();
    for (let i = 0; i < this.keys.length; i++) {
      n48.index[this.keys[i]] = i;
      n48.children[i] = this.children[i];
    }
    n48._count = this.keys.length;
    n48.value = this.value;
    n48.hasValue = this.hasValue;
    return n48;
  }
}

class Node48 {
  constructor() {
    this.index = new Int8Array(256).fill(-1);
    this.children = new Array(48).fill(null);
    this._count = 0;
    this.value = undefined;
    this.hasValue = false;
  }
  get type() { return 48; }
  get count() { return this._count; }

  find(byte) {
    const idx = this.index[byte];
    return idx >= 0 ? this.children[idx] : null;
  }

  insert(byte, child) {
    this.index[byte] = this._count;
    this.children[this._count] = child;
    this._count++;
  }

  isFull() { return this._count >= 48; }

  grow() {
    const n256 = new Node256();
    for (let b = 0; b < 256; b++) {
      if (this.index[b] >= 0) n256.children[b] = this.children[this.index[b]];
    }
    n256._count = this._count;
    n256.value = this.value;
    n256.hasValue = this.hasValue;
    return n256;
  }
}

class Node256 {
  constructor() {
    this.children = new Array(256).fill(null);
    this._count = 0;
    this.value = undefined;
    this.hasValue = false;
  }
  get type() { return 256; }
  get count() { return this._count; }

  find(byte) { return this.children[byte]; }

  insert(byte, child) {
    if (!this.children[byte]) this._count++;
    this.children[byte] = child;
  }

  isFull() { return false; } // Never full
  grow() { return this; }
}

export class AdaptiveRadixTree {
  constructor() {
    this._root = new Node4();
    this._size = 0;
  }

  get size() { return this._size; }

  /**
   * Insert a string key with value.
   */
  insert(key, value) {
    const bytes = this._toBytes(key);
    let node = this._root;
    let parent = null;
    let parentByte = -1;

    for (let i = 0; i < bytes.length; i++) {
      const byte = bytes[i];
      let child = node.find(byte);

      if (!child) {
        child = new Node4();
        if (node.isFull()) {
          const grown = node.grow();
          if (parent) {
            // Replace in parent
            this._replaceChild(parent, parentByte, grown);
          } else {
            this._root = grown;
          }
          node = grown;
        }
        node.insert(byte, child);
      }

      parent = node;
      parentByte = byte;
      node = child;
    }

    if (!node.hasValue) this._size++;
    node.value = value;
    node.hasValue = true;
  }

  /**
   * Get value for key.
   */
  get(key) {
    const node = this._findNode(key);
    return node && node.hasValue ? node.value : undefined;
  }

  /**
   * Check if key exists.
   */
  has(key) {
    const node = this._findNode(key);
    return node ? node.hasValue : false;
  }

  /**
   * Get all keys with given prefix.
   */
  prefixSearch(prefix) {
    const bytes = this._toBytes(prefix);
    let node = this._root;
    for (const byte of bytes) {
      node = node.find(byte);
      if (!node) return [];
    }
    const results = [];
    this._collect(node, prefix, results);
    return results;
  }

  _findNode(key) {
    const bytes = this._toBytes(key);
    let node = this._root;
    for (const byte of bytes) {
      node = node.find(byte);
      if (!node) return null;
    }
    return node;
  }

  _collect(node, prefix, results) {
    if (node.hasValue) results.push({ key: prefix, value: node.value });

    // Collect from children based on node type
    if (node.keys) {
      // Node4 or Node16
      for (let i = 0; i < node.keys.length; i++) {
        this._collect(node.children[i], prefix + String.fromCharCode(node.keys[i]), results);
      }
    } else if (node.index) {
      // Node48
      for (let b = 0; b < 256; b++) {
        if (node.index[b] >= 0) {
          this._collect(node.children[node.index[b]], prefix + String.fromCharCode(b), results);
        }
      }
    } else if (node.children) {
      // Node256
      for (let b = 0; b < 256; b++) {
        if (node.children[b]) {
          this._collect(node.children[b], prefix + String.fromCharCode(b), results);
        }
      }
    }
  }

  _replaceChild(parent, byte, newChild) {
    if (parent.keys) {
      const idx = parent.keys.indexOf(byte);
      if (idx >= 0) parent.children[idx] = newChild;
    } else if (parent.index) {
      const idx = parent.index[byte];
      if (idx >= 0) parent.children[idx] = newChild;
    } else {
      parent.children[byte] = newChild;
    }
  }

  _toBytes(str) {
    const bytes = [];
    for (let i = 0; i < str.length; i++) bytes.push(str.charCodeAt(i));
    return bytes;
  }

  /**
   * Get node type distribution.
   */
  getStats() {
    const counts = { node4: 0, node16: 0, node48: 0, node256: 0 };
    const q = [this._root];
    while (q.length > 0) {
      const node = q.shift();
      counts[`node${node.type}`]++;
      if (node.keys) {
        for (const c of node.children) if (c) q.push(c);
      } else if (node.index) {
        for (let b = 0; b < 256; b++) {
          if (node.index[b] >= 0 && node.children[node.index[b]]) q.push(node.children[node.index[b]]);
        }
      } else if (node.children) {
        for (const c of node.children) if (c) q.push(c);
      }
    }
    return { size: this._size, ...counts };
  }
}
