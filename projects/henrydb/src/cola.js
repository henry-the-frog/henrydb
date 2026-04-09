// cola.js — Cache-Oblivious Lookahead Array (COLA)
// A write-optimized data structure using a hierarchy of sorted arrays.
// Each level doubles in size. When a level fills, merge down to next.
// O(log²N / B) amortized inserts, O(log²N) lookups.
// Related to LSM-trees but mathematically cleaner.

export class COLA {
  constructor() {
    this._levels = []; // levels[i] has capacity 2^i
    this._size = 0;
  }

  get size() { return this._size; }

  /**
   * Insert a key-value pair. O(log²N / B) amortized.
   */
  insert(key, value) {
    let current = [{ key, value }];

    for (let i = 0; ; i++) {
      if (i >= this._levels.length) {
        this._levels.push(null);
      }

      if (this._levels[i] === null) {
        // Empty level: place here
        this._levels[i] = current;
        break;
      } else {
        // Merge and cascade to next level
        current = this._merge(current, this._levels[i]);
        this._levels[i] = null;
      }
    }

    this._size++;
  }

  /**
   * Get value for key. O(log²N).
   * Search each level (binary search within sorted array).
   */
  get(key) {
    for (let i = this._levels.length - 1; i >= 0; i--) {
      if (!this._levels[i]) continue;
      const arr = this._levels[i];
      const idx = this._bsearch(arr, key);
      if (idx < arr.length && arr[idx].key === key) return arr[idx].value;
    }
    return undefined;
  }

  /**
   * Check if key exists.
   */
  has(key) { return this.get(key) !== undefined; }

  /**
   * Range query [lo, hi].
   */
  range(lo, hi) {
    const results = new Map(); // Dedup across levels
    for (let i = this._levels.length - 1; i >= 0; i--) {
      if (!this._levels[i]) continue;
      const arr = this._levels[i];
      const start = this._bsearch(arr, lo);
      for (let j = start; j < arr.length; j++) {
        if (arr[j].key > hi) break;
        if (!results.has(arr[j].key)) {
          results.set(arr[j].key, arr[j].value);
        }
      }
    }
    return [...results.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1)).map(([k, v]) => ({ key: k, value: v }));
  }

  /**
   * Get level statistics.
   */
  getStats() {
    const levels = this._levels.map((l, i) => ({
      level: i,
      capacity: 1 << i,
      used: l ? l.length : 0,
      empty: !l,
    }));
    return { size: this._size, levels: levels.filter(l => !l.empty) };
  }

  _merge(a, b) {
    const result = [];
    let i = 0, j = 0;
    while (i < a.length && j < b.length) {
      if (a[i].key <= b[j].key) {
        // Later insert (smaller level) wins on tie
        if (a[i].key === b[j].key) j++;
        result.push(a[i++]);
      } else {
        result.push(b[j++]);
      }
    }
    while (i < a.length) result.push(a[i++]);
    while (j < b.length) result.push(b[j++]);
    return result;
  }

  _bsearch(arr, key) {
    let lo = 0, hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (arr[mid].key < key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}
