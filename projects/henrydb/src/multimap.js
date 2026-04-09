// multimap.js — Map with multiple values per key
export class MultiMap {
  constructor() { this._map = new Map(); this._size = 0; }
  get size() { return this._size; }
  
  add(key, value) {
    if (!this._map.has(key)) this._map.set(key, []);
    this._map.get(key).push(value);
    this._size++;
  }

  get(key) { return this._map.get(key) || []; }
  has(key) { return this._map.has(key); }
  
  delete(key, value) {
    const arr = this._map.get(key);
    if (!arr) return false;
    const idx = arr.indexOf(value);
    if (idx < 0) return false;
    arr.splice(idx, 1);
    this._size--;
    if (arr.length === 0) this._map.delete(key);
    return true;
  }

  keys() { return [...this._map.keys()]; }
  
  *entries() {
    for (const [key, values] of this._map) {
      for (const value of values) yield [key, value];
    }
  }
}
