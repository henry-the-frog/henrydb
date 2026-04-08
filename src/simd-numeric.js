// simd-numeric.js — Vectorized numeric operations on TypedArrays
// SIMD-like batch operations for columnar data processing.

/**
 * Vectorized arithmetic on Float64Arrays.
 */
export function vecAdd(a, b) {
  const r = new Float64Array(a.length);
  for (let i = 0; i < a.length; i++) r[i] = a[i] + b[i];
  return r;
}

export function vecSub(a, b) {
  const r = new Float64Array(a.length);
  for (let i = 0; i < a.length; i++) r[i] = a[i] - b[i];
  return r;
}

export function vecMul(a, b) {
  const r = new Float64Array(a.length);
  for (let i = 0; i < a.length; i++) r[i] = a[i] * b[i];
  return r;
}

export function vecDiv(a, b) {
  const r = new Float64Array(a.length);
  for (let i = 0; i < a.length; i++) r[i] = b[i] !== 0 ? a[i] / b[i] : 0;
  return r;
}

export function vecScalarMul(a, scalar) {
  const r = new Float64Array(a.length);
  for (let i = 0; i < a.length; i++) r[i] = a[i] * scalar;
  return r;
}

/**
 * Vectorized comparisons — returns selection vector.
 */
export function vecGT(a, threshold) {
  const sel = [];
  for (let i = 0; i < a.length; i++) if (a[i] > threshold) sel.push(i);
  return sel;
}

export function vecLT(a, threshold) {
  const sel = [];
  for (let i = 0; i < a.length; i++) if (a[i] < threshold) sel.push(i);
  return sel;
}

export function vecEQ(a, value) {
  const sel = [];
  for (let i = 0; i < a.length; i++) if (a[i] === value) sel.push(i);
  return sel;
}

export function vecBetween(a, lo, hi) {
  const sel = [];
  for (let i = 0; i < a.length; i++) if (a[i] >= lo && a[i] <= hi) sel.push(i);
  return sel;
}

/**
 * Vectorized aggregations.
 */
export function vecSum(a) { let s = 0; for (let i = 0; i < a.length; i++) s += a[i]; return s; }
export function vecMin(a) { let m = a[0]; for (let i = 1; i < a.length; i++) if (a[i] < m) m = a[i]; return m; }
export function vecMax(a) { let m = a[0]; for (let i = 1; i < a.length; i++) if (a[i] > m) m = a[i]; return m; }
export function vecAvg(a) { return a.length > 0 ? vecSum(a) / a.length : 0; }
export function vecCount(a) { return a.length; }

/**
 * Vectorized aggregation with selection vector.
 */
export function vecSumSel(a, sel) { let s = 0; for (const i of sel) s += a[i]; return s; }
export function vecMinSel(a, sel) { let m = a[sel[0]]; for (let i = 1; i < sel.length; i++) if (a[sel[i]] < m) m = a[sel[i]]; return m; }
export function vecMaxSel(a, sel) { let m = a[sel[0]]; for (let i = 1; i < sel.length; i++) if (a[sel[i]] > m) m = a[sel[i]]; return m; }

/**
 * Vectorized gather: collect elements at selected indices.
 */
export function vecGather(a, sel) {
  const r = new Float64Array(sel.length);
  for (let i = 0; i < sel.length; i++) r[i] = a[sel[i]];
  return r;
}

/**
 * Vectorized scatter: write values to selected indices.
 */
export function vecScatter(target, sel, values) {
  for (let i = 0; i < sel.length; i++) target[sel[i]] = values[i];
}

/**
 * Vectorized dot product.
 */
export function vecDot(a, b) {
  let sum = 0;
  for (let i = 0; i < a.length; i++) sum += a[i] * b[i];
  return sum;
}
