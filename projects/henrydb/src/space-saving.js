// space-saving.js — Heavy hitters detection (Metwally et al. 2005)
// Find approximate top-k most frequent items in O(k) space.
// Guaranteed: if item has frequency ≥ n/k, it will be in the output.

export class SpaceSaving {
  constructor(k) {
    this._k = k;
    this._counters = new Map(); // item → {count, error}
  }

  get size() { return this._counters.size; }

  add(item) {
    if (this._counters.has(item)) {
      this._counters.get(item).count++;
    } else if (this._counters.size < this._k) {
      this._counters.set(item, { count: 1, error: 0 });
    } else {
      // Replace minimum counter
      let minItem = null, minCount = Infinity;
      for (const [k, v] of this._counters) {
        if (v.count < minCount) { minItem = k; minCount = v.count; }
      }
      this._counters.delete(minItem);
      this._counters.set(item, { count: minCount + 1, error: minCount });
    }
  }

  /** Get top-k items sorted by frequency. */
  getTop(k) {
    return [...this._counters.entries()]
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, k || this._k)
      .map(([item, { count, error }]) => ({ item, count, error }));
  }

  /** Check if item is a potential heavy hitter. */
  isHeavyHitter(item) { return this._counters.has(item); }
}
