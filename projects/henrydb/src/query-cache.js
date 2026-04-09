// query-cache.js — Result cache for repeated queries
export class QueryCache {
  constructor(maxSize = 1000) {
    this._cache = new Map();
    this._maxSize = maxSize;
    this._hits = 0;
    this._misses = 0;
  }

  get(sql, params) {
    const key = sql + '|' + JSON.stringify(params || []);
    if (this._cache.has(key)) { this._hits++; return this._cache.get(key); }
    this._misses++;
    return undefined;
  }

  set(sql, params, result) {
    const key = sql + '|' + JSON.stringify(params || []);
    if (this._cache.size >= this._maxSize) {
      this._cache.delete(this._cache.keys().next().value);
    }
    this._cache.set(key, { result, timestamp: Date.now() });
  }

  invalidate(table) {
    for (const [key] of this._cache) {
      if (key.toLowerCase().includes(table.toLowerCase())) this._cache.delete(key);
    }
  }

  clear() { this._cache.clear(); }
  get hitRate() { return this._hits / (this._hits + this._misses) || 0; }
  get size() { return this._cache.size; }
}
