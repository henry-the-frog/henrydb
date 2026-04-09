// sequence.js — Auto-incrementing sequence generator (like PostgreSQL SERIAL)
export class Sequence {
  constructor(start = 1, increment = 1) {
    this._current = start - increment;
    this._increment = increment;
    this._cache = [];
    this._cacheSize = 20;
  }

  nextVal() {
    if (this._cache.length > 0) return this._cache.shift();
    this._current += this._increment;
    return this._current;
  }

  currVal() { return this._current; }

  /** Pre-fetch N values for batch inserts. */
  prefetch(n) {
    const values = [];
    for (let i = 0; i < n; i++) { this._current += this._increment; values.push(this._current); }
    return values;
  }

  reset(value) { this._current = value - this._increment; }
}
