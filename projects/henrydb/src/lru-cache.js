// lru-cache.js — Simple LRU cache using Map (ES6+ insertion order)
export class LRUCache {
  constructor(capacity) {
    this._capacity = capacity;
    this._cache = new Map();
  }

  get(key) {
    if (!this._cache.has(key)) return undefined;
    const value = this._cache.get(key);
    this._cache.delete(key);
    this._cache.set(key, value);
    return value;
  }

  put(key, value) {
    if (this._cache.has(key)) this._cache.delete(key);
    this._cache.set(key, value);
    if (this._cache.size > this._capacity) {
      this._cache.delete(this._cache.keys().next().value);
    }
  }

  get size() { return this._cache.size; }
}
