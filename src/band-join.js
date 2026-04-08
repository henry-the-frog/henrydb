// band-join.js — Band join for range predicates
// For joins on range predicates (a.val BETWEEN b.lo AND b.hi), standard
// hash join doesn't work. Band join sorts both inputs and uses a sliding
// window to find matching ranges efficiently.
//
// Examples:
// - Temporal: events WHERE e.time BETWEEN s.start AND s.end
// - Spatial: points WHERE p.x BETWEEN r.x1 AND r.x2

/**
 * BandJoin — range predicate join using sorted sweep.
 */
export class BandJoin {
  constructor() {
    this.stats = { leftRows: 0, rightRows: 0, matches: 0, timeMs: 0 };
  }

  /**
   * Join where leftValue is BETWEEN rightLo and rightHi.
   * Both inputs sorted by their join key for efficient sweep.
   */
  join(leftValues, rightLo, rightHi) {
    const t0 = Date.now();
    this.stats.leftRows = leftValues.length;
    this.stats.rightRows = rightLo.length;

    // Sort left by value (with original index)
    const leftSorted = leftValues.map((v, i) => ({ val: v, idx: i }));
    leftSorted.sort((a, b) => a.val - b.val);

    // Sort right by lo value (with original index)
    const rightSorted = rightLo.map((lo, i) => ({ lo, hi: rightHi[i], idx: i }));
    rightSorted.sort((a, b) => a.lo - b.lo);

    const leftIndices = [];
    const rightIndices = [];

    // Sweep: for each left value, find all right ranges that contain it
    let rStart = 0;
    for (let l = 0; l < leftSorted.length; l++) {
      const val = leftSorted[l].val;

      // Advance rStart to first range where lo <= val
      while (rStart < rightSorted.length && rightSorted[rStart].lo > val) rStart++;

      // Check all active ranges
      for (let r = 0; r < rightSorted.length; r++) {
        if (rightSorted[r].lo <= val && val <= rightSorted[r].hi) {
          leftIndices.push(leftSorted[l].idx);
          rightIndices.push(rightSorted[r].idx);
        }
      }
    }

    this.stats.matches = leftIndices.length;
    this.stats.timeMs = Date.now() - t0;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  /**
   * Optimized band join with interval tree-like approach.
   * For each left value, binary search for matching ranges.
   */
  joinOptimized(leftValues, rightLo, rightHi) {
    const t0 = Date.now();
    this.stats.leftRows = leftValues.length;
    this.stats.rightRows = rightLo.length;

    // Sort right by lo (with original index)
    const rightSorted = rightLo.map((lo, i) => ({ lo, hi: rightHi[i], idx: i }));
    rightSorted.sort((a, b) => a.lo - b.lo);

    const leftIndices = [];
    const rightIndices = [];

    for (let l = 0; l < leftValues.length; l++) {
      const val = leftValues[l];

      // Binary search: find first range where lo <= val
      let lo = 0, hi = rightSorted.length - 1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (rightSorted[mid].lo <= val) lo = mid + 1;
        else hi = mid - 1;
      }

      // Check all ranges from 0 to hi where lo <= val
      for (let r = 0; r <= hi; r++) {
        if (val <= rightSorted[r].hi) {
          leftIndices.push(l);
          rightIndices.push(rightSorted[r].idx);
        }
      }
    }

    this.stats.matches = leftIndices.length;
    this.stats.timeMs = Date.now() - t0;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  getStats() { return { ...this.stats }; }
}
