// trie.js — Prefix tree for autocomplete and prefix queries
// Used for: LIKE 'prefix%' optimization, autocomplete, IP routing tables

class TrieNode {
  constructor() {
    this.children = new Map();
    this.isEnd = false;
    this.value = null;
    this.count = 0; // Number of words passing through this node
  }
}

/**
 * Trie (Prefix Tree): O(m) insert/search where m = key length.
 */
export class Trie {
  constructor() {
    this._root = new TrieNode();
    this._size = 0;
  }

  /**
   * Insert a key-value pair.
   */
  insert(key, value = true) {
    let node = this._root;
    for (const ch of String(key)) {
      if (!node.children.has(ch)) {
        node.children.set(ch, new TrieNode());
      }
      node = node.children.get(ch);
      node.count++;
    }
    if (!node.isEnd) this._size++;
    node.isEnd = true;
    node.value = value;
  }

  /**
   * Search for an exact key.
   */
  search(key) {
    const node = this._findNode(String(key));
    return node && node.isEnd ? node.value : undefined;
  }

  /**
   * Check if any key starts with the given prefix.
   */
  hasPrefix(prefix) {
    return this._findNode(String(prefix)) !== null;
  }

  /**
   * Find all keys starting with a prefix.
   */
  findByPrefix(prefix, limit = Infinity) {
    const node = this._findNode(String(prefix));
    if (!node) return [];
    
    const results = [];
    this._collect(node, String(prefix), results, limit);
    return results;
  }

  /**
   * Delete a key.
   */
  delete(key) {
    const str = String(key);
    const path = [];
    let node = this._root;
    
    for (const ch of str) {
      if (!node.children.has(ch)) return false;
      path.push({ node, ch });
      node = node.children.get(ch);
    }
    
    if (!node.isEnd) return false;
    node.isEnd = false;
    node.value = null;
    this._size--;
    
    // Clean up empty nodes
    for (let i = path.length - 1; i >= 0; i--) {
      const { node: parent, ch } = path[i];
      const child = parent.children.get(ch);
      child.count--;
      if (child.count === 0 && !child.isEnd) {
        parent.children.delete(ch);
      }
    }
    
    return true;
  }

  get size() { return this._size; }

  _findNode(str) {
    let node = this._root;
    for (const ch of str) {
      if (!node.children.has(ch)) return null;
      node = node.children.get(ch);
    }
    return node;
  }

  _collect(node, prefix, results, limit) {
    if (results.length >= limit) return;
    if (node.isEnd) results.push({ key: prefix, value: node.value });
    for (const [ch, child] of node.children) {
      this._collect(child, prefix + ch, results, limit);
    }
  }
}

/**
 * Ring Buffer: fixed-size circular buffer for efficient logging.
 * O(1) push, O(1) access by index, overwrites oldest on overflow.
 */
export class RingBuffer {
  constructor(capacity = 1024) {
    this._capacity = capacity;
    this._buffer = new Array(capacity).fill(null);
    this._head = 0; // Next write position
    this._size = 0;
  }

  /**
   * Push an item. Overwrites oldest if full.
   */
  push(item) {
    this._buffer[this._head] = item;
    this._head = (this._head + 1) % this._capacity;
    if (this._size < this._capacity) this._size++;
  }

  /**
   * Get item at index (0 = oldest, size-1 = newest).
   */
  get(index) {
    if (index < 0 || index >= this._size) return undefined;
    const start = this._size === this._capacity
      ? this._head
      : 0;
    return this._buffer[(start + index) % this._capacity];
  }

  /**
   * Get the newest item.
   */
  latest() {
    if (this._size === 0) return undefined;
    return this._buffer[(this._head - 1 + this._capacity) % this._capacity];
  }

  /**
   * Get all items (oldest to newest).
   */
  toArray() {
    const arr = [];
    for (let i = 0; i < this._size; i++) {
      arr.push(this.get(i));
    }
    return arr;
  }

  get size() { return this._size; }
  get capacity() { return this._capacity; }
  get isFull() { return this._size === this._capacity; }
}
