// sorted-array.js — Simple sorted array with binary search
// The simplest possible index structure. Baseline for benchmarking.
// O(log n) lookup, O(n) insert (shift), O(n) delete.

export class SortedArray {
  constructor(comparator) {
    this._compare = comparator || ((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    this._keys = [];
    this._values = [];
  }

  get size() { return this._keys.length; }

  insert(key, value) {
    const idx = this._bsearch(key);
    if (idx < this._keys.length && this._compare(this._keys[idx], key) === 0) {
      this._values[idx] = value; // Upsert
      return;
    }
    this._keys.splice(idx, 0, key);
    this._values.splice(idx, 0, value);
  }

  get(key) {
    const idx = this._bsearch(key);
    if (idx < this._keys.length && this._compare(this._keys[idx], key) === 0) return this._values[idx];
    return undefined;
  }

  has(key) {
    const idx = this._bsearch(key);
    return idx < this._keys.length && this._compare(this._keys[idx], key) === 0;
  }

  delete(key) {
    const idx = this._bsearch(key);
    if (idx < this._keys.length && this._compare(this._keys[idx], key) === 0) {
      this._keys.splice(idx, 1);
      this._values.splice(idx, 1);
      return true;
    }
    return false;
  }

  range(lo, hi) {
    const start = this._bsearch(lo);
    const results = [];
    for (let i = start; i < this._keys.length; i++) {
      if (this._compare(this._keys[i], hi) > 0) break;
      results.push({ key: this._keys[i], value: this._values[i] });
    }
    return results;
  }

  min() { return this._keys.length ? { key: this._keys[0], value: this._values[0] } : undefined; }
  max() { return this._keys.length ? { key: this._keys[this._keys.length-1], value: this._values[this._keys.length-1] } : undefined; }

  _bsearch(key) {
    let lo = 0, hi = this._keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._compare(this._keys[mid], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}
