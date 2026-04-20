// trie.js — Trie (prefix tree) for fast string prefix lookup
export class Trie {
  constructor() { this._root = {}; this._size = 0; }

  insert(key, value) {
    let node = this._root;
    for (const ch of key) {
      if (!node[ch]) node[ch] = {};
      node = node[ch];
    }
    if (!node._end) this._size++;
    node._end = true;
    node._value = value;
  }

  get(key) {
    const node = this._traverse(key);
    return node?._end ? node._value : undefined;
  }

  has(key) { const node = this._traverse(key); return node?._end === true; }

  delete(key) {
    const node = this._traverse(key);
    if (!node?._end) return false;
    delete node._end;
    delete node._value;
    this._size--;
    return true;
  }

  /** Find all keys with given prefix */
  prefixSearch(prefix, limit = 100) {
    const node = this._traverse(prefix);
    if (!node) return [];
    const results = [];
    this._collect(node, prefix, results, limit);
    return results;
  }

  /** Autocomplete: return keys starting with prefix */
  autocomplete(prefix, limit = 10) {
    return this.prefixSearch(prefix, limit).map(r => r.key);
  }

  _traverse(key) {
    let node = this._root;
    for (const ch of key) { if (!node[ch]) return null; node = node[ch]; }
    return node;
  }

  _collect(node, prefix, results, limit) {
    if (results.length >= limit) return;
    if (node._end) results.push({ key: prefix, value: node._value });
    for (const ch of Object.keys(node)) {
      if (ch[0] === '_') continue;
      this._collect(node[ch], prefix + ch, results, limit);
    }
  }

  get size() { return this._size; }
}

/**
 * RingBuffer — fixed-capacity circular buffer.
 * When full, new pushes overwrite the oldest entry.
 */
export class RingBuffer {
  constructor(capacity) {
    this._buf = new Array(capacity);
    this._capacity = capacity;
    this._head = 0;  // next write position
    this._count = 0;
  }

  push(item) {
    this._buf[this._head] = item;
    this._head = (this._head + 1) % this._capacity;
    if (this._count < this._capacity) this._count++;
  }

  latest() {
    if (this._count === 0) return undefined;
    return this._buf[(this._head - 1 + this._capacity) % this._capacity];
  }

  oldest() {
    if (this._count === 0) return undefined;
    return this._buf[(this._head - this._count + this._capacity) % this._capacity];
  }

  toArray() {
    const result = [];
    for (let i = 0; i < this._count; i++) {
      result.push(this._buf[(this._head - this._count + i + this._capacity) % this._capacity]);
    }
    return result;
  }

  get size() { return this._count; }
  get capacity() { return this._capacity; }
}
