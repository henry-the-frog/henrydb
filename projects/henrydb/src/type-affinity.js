// type-affinity.js — SQLite-compatible type affinity and comparison utilities

/**
 * SQLite type class for comparison ordering.
 * NULL < INTEGER/REAL < TEXT < BLOB
 */
export function typeClass(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return 1;
  if (typeof v === 'string') return 2;
  if (Buffer.isBuffer(v) || v instanceof Uint8Array) return 3;
  return 1; // default to numeric
}

/**
 * SQLite-compatible comparison: different types compared by type class,
 * same types compared by value.
 */
export function sqliteCompare(left, right) {
  const lc = typeClass(left);
  const rc = typeClass(right);
  if (lc !== rc) return lc - rc;
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

/**
 * Apply type affinity to a value based on column type declaration.
 * Follows SQLite affinity rules:
 * - TEXT/VARCHAR/CHAR → always string
 * - INTEGER/INT → try integer
 * - REAL/FLOAT/DOUBLE → try number
 * - NUMERIC → try int, then float, then keep
 * - BLOB/NONE → no coercion
 */
export function applyAffinity(value, colType) {
  if (value == null) return value;
  const t = (colType || '').toUpperCase();
  
  if (t === 'TEXT' || t === 'VARCHAR' || t === 'CHAR' || t.includes('TEXT') || t.includes('CHAR') || t.includes('CLOB')) {
    return typeof value !== 'string' ? String(value) : value;
  }
  
  if (t === 'REAL' || t === 'FLOAT' || t === 'DOUBLE' || t.includes('REAL') || t.includes('FLOAT') || t.includes('DOUB')) {
    if (typeof value === 'string') {
      const n = Number(value);
      return (!isNaN(n) && value.trim() !== '') ? n : value;
    }
    return value;
  }
  
  if (t === 'INT' || t === 'INTEGER' || t === 'BIGINT' || t === 'SMALLINT' || t === 'TINYINT' || t.includes('INT')) {
    if (typeof value === 'string') {
      const n = Number(value);
      return (!isNaN(n) && value.trim() !== '' && Number.isInteger(n)) ? n : value;
    }
    if (typeof value === 'number' && !Number.isInteger(value)) return Math.trunc(value);
    return value;
  }
  
  return value;
}
