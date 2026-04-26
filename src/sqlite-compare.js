// sqlite-compare.js — SQLite-compatible type-aware comparison
// SQLite ordering: NULL < INTEGER/REAL < TEXT < BLOB
// Within same type: normal comparison

/**
 * Returns the SQLite type order for a value.
 * NULL=0, NUMBER=1, TEXT=2, BLOB=3
 */
function typeOrder(val) {
  if (val === null || val === undefined) return 0;
  if (typeof val === 'number') return 1;
  if (typeof val === 'bigint') return 1;
  if (typeof val === 'string') return 2;
  if (val instanceof Uint8Array || val instanceof ArrayBuffer || Buffer.isBuffer(val)) return 3;
  // Fallback: treat as text
  return 2;
}

/**
 * SQLite-compatible comparison: returns -1, 0, or 1.
 * Follows SQLite type affinity ordering: NULL < numeric < text < blob
 * Within same type class, uses natural ordering.
 */
export function sqliteCompare(a, b) {
  const typeA = typeOrder(a);
  const typeB = typeOrder(b);
  
  // Different type classes: compare by type order
  if (typeA !== typeB) return typeA < typeB ? -1 : 1;
  
  // Same type class
  if (typeA === 0) return 0; // NULL == NULL for ordering purposes
  
  // Numeric comparison
  if (typeA === 1) {
    const na = Number(a);
    const nb = Number(b);
    if (na < nb) return -1;
    if (na > nb) return 1;
    return 0;
  }
  
  // Text comparison (case-sensitive, like SQLite default)
  if (typeA === 2) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }
  
  // Blob comparison (byte-by-byte)
  return 0; // Simplified for now
}

/**
 * Type-aware equality check for EQ/NE.
 * In SQLite, '42' != 42 (different types are not equal).
 * But numeric strings may be coerced in some contexts.
 * This follows strict SQLite behavior.
 */
export function sqliteEquals(a, b) {
  if (a === null || a === undefined || b === null || b === undefined) return false;
  // Different types are not equal in SQLite
  if (typeof a !== typeof b) return false;
  return a === b;
}
