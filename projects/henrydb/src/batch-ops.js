// batch-ops.js — SIMD-like batch operations on TypedArray columns
// Operate on entire columns at once, producing result columns.
// These are the building blocks for compiled vectorized expressions.

import { TypedColumn } from './typed-columns.js';

/**
 * Batch arithmetic operations on TypedColumns.
 * Each function takes column(s) and returns a new column with the result.
 */
export const BatchOps = {
  // --- Arithmetic ---

  /** Element-wise addition of two columns. */
  add(colA, colB) {
    const len = Math.min(colA.length, colB.length);
    const result = new TypedColumn('FLOAT', len);
    const a = colA.toArray(), b = colB.toArray();
    for (let i = 0; i < len; i++) result.push(a[i] + b[i]);
    return result;
  },

  /** Element-wise subtraction. */
  sub(colA, colB) {
    const len = Math.min(colA.length, colB.length);
    const result = new TypedColumn('FLOAT', len);
    const a = colA.toArray(), b = colB.toArray();
    for (let i = 0; i < len; i++) result.push(a[i] - b[i]);
    return result;
  },

  /** Element-wise multiplication. */
  mul(colA, colB) {
    const len = Math.min(colA.length, colB.length);
    const result = new TypedColumn('FLOAT', len);
    const a = colA.toArray(), b = colB.toArray();
    for (let i = 0; i < len; i++) result.push(a[i] * b[i]);
    return result;
  },

  /** Scalar multiplication. */
  mulScalar(col, scalar) {
    const len = col.length;
    const result = new TypedColumn('FLOAT', len);
    const a = col.toArray();
    for (let i = 0; i < len; i++) result.push(a[i] * scalar);
    return result;
  },

  // --- Comparison (produce selection vectors) ---

  /** Return indices where colA[i] == colB[i]. */
  eqColumns(colA, colB) {
    const len = Math.min(colA.length, colB.length);
    const result = new Uint32Array(len);
    const a = colA.toArray(), b = colB.toArray();
    let count = 0;
    for (let i = 0; i < len; i++) {
      if (a[i] === b[i]) result[count++] = i;
    }
    return result.subarray(0, count);
  },

  /** Return indices where colA[i] > colB[i]. */
  gtColumns(colA, colB) {
    const len = Math.min(colA.length, colB.length);
    const result = new Uint32Array(len);
    const a = colA.toArray(), b = colB.toArray();
    let count = 0;
    for (let i = 0; i < len; i++) {
      if (a[i] > b[i]) result[count++] = i;
    }
    return result.subarray(0, count);
  },

  // --- Selection vector operations ---

  /** Intersect two selection vectors (AND). */
  intersect(selA, selB) {
    // Both are sorted Uint32Arrays — merge intersection
    const result = new Uint32Array(Math.min(selA.length, selB.length));
    let ai = 0, bi = 0, count = 0;
    while (ai < selA.length && bi < selB.length) {
      if (selA[ai] === selB[bi]) {
        result[count++] = selA[ai];
        ai++; bi++;
      } else if (selA[ai] < selB[bi]) {
        ai++;
      } else {
        bi++;
      }
    }
    return result.subarray(0, count);
  },

  /** Union two selection vectors (OR). */
  union(selA, selB) {
    const result = new Uint32Array(selA.length + selB.length);
    let ai = 0, bi = 0, count = 0;
    while (ai < selA.length && bi < selB.length) {
      if (selA[ai] === selB[bi]) {
        result[count++] = selA[ai];
        ai++; bi++;
      } else if (selA[ai] < selB[bi]) {
        result[count++] = selA[ai++];
      } else {
        result[count++] = selB[bi++];
      }
    }
    while (ai < selA.length) result[count++] = selA[ai++];
    while (bi < selB.length) result[count++] = selB[bi++];
    return result.subarray(0, count);
  },

  /** Negate a selection vector (NOT) against total length. */
  negate(sel, totalLength) {
    const result = new Uint32Array(totalLength - sel.length);
    let si = 0, count = 0;
    for (let i = 0; i < totalLength; i++) {
      if (si < sel.length && sel[si] === i) {
        si++;
      } else {
        result[count++] = i;
      }
    }
    return result.subarray(0, count);
  },

  // --- Gather (select elements by indices) ---

  /** Gather: create new column from selected indices. */
  gather(col, selection) {
    const result = new TypedColumn(col.type, selection.length);
    const arr = col.toArray();
    for (let i = 0; i < selection.length; i++) {
      result.push(arr[selection[i]]);
    }
    return result;
  },

  // --- Aggregation on selections ---

  /** Sum of column at selected indices. */
  sumAt(col, selection) {
    const arr = col.toArray();
    let total = 0;
    for (let i = 0; i < selection.length; i++) total += arr[selection[i]];
    return total;
  },

  /** Count of selected indices. */
  countAt(selection) {
    return selection.length;
  },

  /** Average of column at selected indices. */
  avgAt(col, selection) {
    return selection.length > 0 ? this.sumAt(col, selection) / selection.length : null;
  },

  /** Min of column at selected indices. */
  minAt(col, selection) {
    if (selection.length === 0) return null;
    const arr = col.toArray();
    let min = arr[selection[0]];
    for (let i = 1; i < selection.length; i++) {
      if (arr[selection[i]] < min) min = arr[selection[i]];
    }
    return min;
  },

  /** Max of column at selected indices. */
  maxAt(col, selection) {
    if (selection.length === 0) return null;
    const arr = col.toArray();
    let max = arr[selection[0]];
    for (let i = 1; i < selection.length; i++) {
      if (arr[selection[i]] > max) max = arr[selection[i]];
    }
    return max;
  },

  // --- Hash join building blocks ---

  /** Build hash table from column: value → [indices]. */
  buildHash(col) {
    const arr = col.toArray();
    const ht = new Map();
    for (let i = 0; i < arr.length; i++) {
      const key = arr[i];
      if (!ht.has(key)) ht.set(key, []);
      ht.get(key).push(i);
    }
    return ht;
  },

  /** Probe hash table: for each element in col, find matching indices. */
  probeHash(col, hashTable) {
    const arr = col.toArray();
    const leftIndices = [];
    const rightIndices = [];
    for (let i = 0; i < arr.length; i++) {
      const matches = hashTable.get(arr[i]);
      if (matches) {
        for (const j of matches) {
          leftIndices.push(i);
          rightIndices.push(j);
        }
      }
    }
    return { left: new Uint32Array(leftIndices), right: new Uint32Array(rightIndices) };
  },
};
