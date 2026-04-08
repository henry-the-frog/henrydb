// simd-string.js — Vectorized string operations
// Batch string operations on column arrays for SIMD-like throughput.
// LIKE patterns compiled to regex, substring search, UPPER/LOWER in batches.

/**
 * Batch LIKE operation on a string column.
 * Returns selection vector (indices of matching rows).
 */
export function batchLike(column, pattern) {
  const regex = likeToRegex(pattern);
  const selected = [];
  for (let i = 0; i < column.length; i++) {
    if (column[i] !== null && regex.test(column[i])) selected.push(i);
  }
  return selected;
}

/**
 * Convert SQL LIKE pattern to JavaScript RegExp.
 */
export function likeToRegex(pattern) {
  const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regexStr = escaped.replace(/%/g, '.*').replace(/_/g, '.');
  return new RegExp('^' + regexStr + '$', 'i');
}

/**
 * Batch substring search.
 */
export function batchContains(column, substring) {
  const sub = substring.toLowerCase();
  const selected = [];
  for (let i = 0; i < column.length; i++) {
    if (column[i] !== null && column[i].toLowerCase().includes(sub)) selected.push(i);
  }
  return selected;
}

/**
 * Batch UPPER/LOWER transformation.
 */
export function batchUpper(column) {
  return column.map(s => s !== null ? s.toUpperCase() : null);
}

export function batchLower(column) {
  return column.map(s => s !== null ? s.toLowerCase() : null);
}

/**
 * Batch string length computation.
 */
export function batchLength(column) {
  return column.map(s => s !== null ? s.length : null);
}

/**
 * Batch substring extraction.
 */
export function batchSubstring(column, start, length) {
  return column.map(s => s !== null ? s.substring(start, start + length) : null);
}

/**
 * Batch string concatenation of two columns.
 */
export function batchConcat(colA, colB, separator = '') {
  const result = new Array(colA.length);
  for (let i = 0; i < colA.length; i++) {
    result[i] = (colA[i] ?? '') + separator + (colB[i] ?? '');
  }
  return result;
}

/**
 * Batch TRIM.
 */
export function batchTrim(column) {
  return column.map(s => s !== null ? s.trim() : null);
}
