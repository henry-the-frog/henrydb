// percentile.js — Shared PERCENTILE_CONT / MEDIAN implementation
// PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY expr)
// Uses linear interpolation (SQL:2003 standard)

/**
 * Compute percentile using continuous interpolation.
 * @param {number[]} values - Numeric values (will be sorted)
 * @param {number} p - Percentile (0.0 to 1.0)
 * @returns {number|null} Interpolated percentile value
 */
export function percentileCont(values, p) {
  if (!values || values.length === 0) return null;
  if (p < 0 || p > 1) return null;
  
  const sorted = [...values].map(Number).filter(v => !isNaN(v)).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  if (sorted.length === 1) return sorted[0];
  
  const n = sorted.length;
  const pos = p * (n - 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  const frac = pos - lo;
  
  if (lo === hi) return sorted[lo];
  return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
}

/**
 * Compute MEDIAN (= PERCENTILE_CONT(0.5))
 * @param {number[]} values
 * @returns {number|null}
 */
export function median(values) {
  return percentileCont(values, 0.5);
}
