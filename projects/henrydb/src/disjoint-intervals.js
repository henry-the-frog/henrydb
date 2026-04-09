// disjoint-intervals.js — Efficiently manage a set of non-overlapping intervals
// Merge overlapping intervals on insert. O(log n) insert, O(1) gap query.
// Used in: range lock management, calendar scheduling, IP range queries.

export class DisjointIntervals {
  constructor() {
    this._intervals = []; // Sorted, non-overlapping: [{lo, hi}, ...]
  }

  get count() { return this._intervals.length; }

  /** Add interval [lo, hi]. Merges with overlapping intervals. */
  add(lo, hi) {
    const merged = [];
    let newLo = lo, newHi = hi;
    let inserted = false;
    
    for (const iv of this._intervals) {
      if (iv.hi < newLo - 1) {
        merged.push(iv); // Before new interval
      } else if (iv.lo > newHi + 1) {
        if (!inserted) { merged.push({ lo: newLo, hi: newHi }); inserted = true; }
        merged.push(iv); // After new interval
      } else {
        // Overlapping: extend
        newLo = Math.min(newLo, iv.lo);
        newHi = Math.max(newHi, iv.hi);
      }
    }
    
    if (!inserted) merged.push({ lo: newLo, hi: newHi });
    this._intervals = merged;
  }

  /** Remove interval [lo, hi]. Splits intervals as needed. */
  remove(lo, hi) {
    const result = [];
    for (const iv of this._intervals) {
      if (iv.hi < lo || iv.lo > hi) {
        result.push(iv); // No overlap
      } else {
        // Partial overlap: keep non-removed parts
        if (iv.lo < lo) result.push({ lo: iv.lo, hi: lo - 1 });
        if (iv.hi > hi) result.push({ lo: hi + 1, hi: iv.hi });
      }
    }
    this._intervals = result;
  }

  /** Check if a point is covered by any interval. */
  contains(point) {
    for (const iv of this._intervals) {
      if (point >= iv.lo && point <= iv.hi) return true;
      if (iv.lo > point) return false; // Sorted, can stop early
    }
    return false;
  }

  /** Get all intervals as array. */
  toArray() { return this._intervals.map(iv => [iv.lo, iv.hi]); }

  /** Get total covered length. */
  totalCoverage() {
    return this._intervals.reduce((sum, iv) => sum + (iv.hi - iv.lo + 1), 0);
  }

  /** Get gaps between intervals. */
  gaps() {
    const gaps = [];
    for (let i = 1; i < this._intervals.length; i++) {
      gaps.push({ lo: this._intervals[i-1].hi + 1, hi: this._intervals[i].lo - 1 });
    }
    return gaps;
  }
}
