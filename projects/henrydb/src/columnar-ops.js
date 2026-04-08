// columnar-ops.js — Vectorized columnar operations
// SIMD-like filter, bitmap ops, columnar join, late materialization.

/**
 * SIMD Filter — evaluate predicates directly on TypedArrays.
 * Returns selection vector (array of matching indices).
 */
export function simdFilterGT(column, threshold) {
  const sel = [];
  for (let i = 0; i < column.length; i++) if (column[i] > threshold) sel.push(i);
  return sel;
}

export function simdFilterLT(column, threshold) {
  const sel = [];
  for (let i = 0; i < column.length; i++) if (column[i] < threshold) sel.push(i);
  return sel;
}

export function simdFilterEQ(column, value) {
  const sel = [];
  for (let i = 0; i < column.length; i++) if (column[i] === value) sel.push(i);
  return sel;
}

export function simdFilterBetween(column, lo, hi) {
  const sel = [];
  for (let i = 0; i < column.length; i++) if (column[i] >= lo && column[i] <= hi) sel.push(i);
  return sel;
}

/** Intersect two selection vectors */
export function selIntersect(a, b) {
  const setB = new Set(b);
  return a.filter(x => setB.has(x));
}

/** Union two selection vectors */
export function selUnion(a, b) {
  return [...new Set([...a, ...b])].sort((x, y) => x - y);
}

/**
 * Bitmap SIMD — vectorized AND/OR on Uint32Arrays.
 */
export function bitmapAnd(a, b) {
  const result = new Uint32Array(Math.min(a.length, b.length));
  for (let i = 0; i < result.length; i++) result[i] = a[i] & b[i];
  return result;
}

export function bitmapOr(a, b) {
  const result = new Uint32Array(Math.max(a.length, b.length));
  for (let i = 0; i < a.length; i++) result[i] |= a[i];
  for (let i = 0; i < b.length; i++) result[i] |= b[i];
  return result;
}

export function bitmapNot(a) {
  const result = new Uint32Array(a.length);
  for (let i = 0; i < a.length; i++) result[i] = ~a[i];
  return result;
}

export function bitmapPopcount(a) {
  let count = 0;
  for (let i = 0; i < a.length; i++) {
    let w = a[i];
    w = w - ((w >>> 1) & 0x55555555);
    w = (w & 0x33333333) + ((w >>> 2) & 0x33333333);
    count += (((w + (w >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
  }
  return count;
}

/**
 * Columnar Hash Join — join directly on column arrays.
 */
export function columnarHashJoin(leftKeys, rightKeys, leftPayload, rightPayload) {
  // Build phase on left
  const ht = new Map();
  for (let i = 0; i < leftKeys.length; i++) {
    const key = leftKeys[i];
    if (!ht.has(key)) ht.set(key, []);
    ht.get(key).push(i);
  }
  
  // Probe phase on right
  const resultLeft = [];
  const resultRight = [];
  for (let i = 0; i < rightKeys.length; i++) {
    const matches = ht.get(rightKeys[i]);
    if (matches) {
      for (const leftIdx of matches) {
        resultLeft.push(leftIdx);
        resultRight.push(i);
      }
    }
  }
  
  return { leftIndices: resultLeft, rightIndices: resultRight, count: resultLeft.length };
}

/**
 * Late Materialization — operate on row IDs until final projection.
 */
export class LateMaterializer {
  constructor(columns) {
    this.columns = columns; // {name: TypedArray|Array}
    this.n = Object.values(columns)[0]?.length || 0;
  }

  /** Start with all row IDs */
  allRows() { return Array.from({ length: this.n }, (_, i) => i); }

  /** Filter using only the needed column */
  filter(rowIds, column, predicate) {
    const col = this.columns[column];
    return rowIds.filter(i => predicate(col[i]));
  }

  /** Materialize selected rows */
  materialize(rowIds, projectedColumns) {
    return rowIds.map(i => {
      const row = {};
      for (const col of projectedColumns) row[col] = this.columns[col][i];
      return row;
    });
  }
}
