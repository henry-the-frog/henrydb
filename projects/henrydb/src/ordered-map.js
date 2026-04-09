// ordered-map.js — Insertion-order preserving map with fast lookup
// Like Map but guarantees insertion order AND supports range queries on insert order.
export class OrderedMap {
  constructor() { this._map = new Map(); this._order = []; }
  get size() { return this._map.size; }

  set(key, value) {
    if (!this._map.has(key)) this._order.push(key);
    this._map.set(key, value);
  }

  get(key) { return this._map.get(key); }
  has(key) { return this._map.has(key); }

  delete(key) {
    if (this._map.delete(key)) {
      this._order = this._order.filter(k => k !== key);
      return true;
    }
    return false;
  }

  /** Get the n-th entry in insertion order. */
  at(index) {
    const key = this._order[index];
    return key !== undefined ? { key, value: this._map.get(key) } : undefined;
  }

  first() { return this.at(0); }
  last() { return this.at(this._order.length - 1); }

  *[Symbol.iterator]() {
    for (const key of this._order) yield { key, value: this._map.get(key) };
  }
}
