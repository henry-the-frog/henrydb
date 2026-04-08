// sort-merge-join.js — Sort-merge join on TypedArray columns
// When both inputs are sorted (or can be sorted cheaply), merge join
// in O(n+m) is faster than hash join's O(n*k) where k is matches per key.
// Uses TypedArrays for maximum throughput.

import { TypedColumn } from './typed-columns.js';

/**
 * SortMergeJoin — merge join two sorted TypedArray key columns.
 */
export class SortMergeJoin {
  constructor() {
    this.stats = { leftRows: 0, rightRows: 0, matches: 0, sortTimeMs: 0, mergeTimeMs: 0 };
  }

  /**
   * Join two typed columns that are already sorted.
   * Returns index pairs.
   */
  joinSorted(leftKey, rightKey) {
    const leftArr = leftKey instanceof TypedColumn ? leftKey.toArray() : leftKey;
    const rightArr = rightKey instanceof TypedColumn ? rightKey.toArray() : rightKey;
    const leftLen = leftKey instanceof TypedColumn ? leftKey.length : leftKey.length;
    const rightLen = rightKey instanceof TypedColumn ? rightKey.length : rightKey.length;

    this.stats.leftRows = leftLen;
    this.stats.rightRows = rightLen;

    const t0 = Date.now();
    const leftIndices = [];
    const rightIndices = [];

    let li = 0, ri = 0;

    while (li < leftLen && ri < rightLen) {
      const lv = leftArr[li];
      const rv = rightArr[ri];

      if (lv < rv) {
        li++;
      } else if (lv > rv) {
        ri++;
      } else {
        // Equal: find all matches on both sides
        const matchVal = lv;
        const lStart = li;
        const rStart = ri;

        while (li < leftLen && leftArr[li] === matchVal) li++;
        while (ri < rightLen && rightArr[ri] === matchVal) ri++;

        // Cross product of matching ranges
        for (let l = lStart; l < li; l++) {
          for (let r = rStart; r < ri; r++) {
            leftIndices.push(l);
            rightIndices.push(r);
          }
        }
      }
    }

    this.stats.mergeTimeMs = Date.now() - t0;
    this.stats.matches = leftIndices.length;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  /**
   * Join two unsorted typed columns: sort first, then merge.
   */
  join(leftKey, rightKey) {
    const leftArr = leftKey instanceof TypedColumn ? leftKey.toArray() : new Int32Array(leftKey);
    const rightArr = rightKey instanceof TypedColumn ? rightKey.toArray() : new Int32Array(rightKey);
    const leftLen = leftArr.length;
    const rightLen = rightArr.length;

    this.stats.leftRows = leftLen;
    this.stats.rightRows = rightLen;

    // Create index arrays for sorting (to preserve original indices)
    const t0 = Date.now();
    const leftOrder = Array.from({ length: leftLen }, (_, i) => i);
    const rightOrder = Array.from({ length: rightLen }, (_, i) => i);

    leftOrder.sort((a, b) => leftArr[a] - leftArr[b]);
    rightOrder.sort((a, b) => rightArr[a] - rightArr[b]);
    this.stats.sortTimeMs = Date.now() - t0;

    // Merge
    const t1 = Date.now();
    const leftIndices = [];
    const rightIndices = [];

    let li = 0, ri = 0;

    while (li < leftLen && ri < rightLen) {
      const lv = leftArr[leftOrder[li]];
      const rv = rightArr[rightOrder[ri]];

      if (lv < rv) {
        li++;
      } else if (lv > rv) {
        ri++;
      } else {
        const matchVal = lv;
        const lStart = li;
        const rStart = ri;

        while (li < leftLen && leftArr[leftOrder[li]] === matchVal) li++;
        while (ri < rightLen && rightArr[rightOrder[ri]] === matchVal) ri++;

        for (let l = lStart; l < li; l++) {
          for (let r = rStart; r < ri; r++) {
            leftIndices.push(leftOrder[l]);
            rightIndices.push(rightOrder[r]);
          }
        }
      }
    }

    this.stats.mergeTimeMs = Date.now() - t1;
    this.stats.matches = leftIndices.length;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  getStats() { return { ...this.stats }; }
}
