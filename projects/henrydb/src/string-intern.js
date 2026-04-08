// string-intern.js — String interning for efficient text column storage
// Maps strings to integer IDs for fast equality comparison and reduced memory.
// Like dictionary encoding in Parquet/ORC/DuckDB.

/**
 * StringInternPool — maps strings ↔ integer IDs.
 * Thread-safe for single-threaded JS (no mutex needed).
 */
export class StringInternPool {
  constructor() {
    this._stringToId = new Map();
    this._idToString = [];
    this._nextId = 0;
    this.stats = { interns: 0, hits: 0, misses: 0 };
  }

  /**
   * Intern a string: return its integer ID.
   * If already interned, returns the existing ID.
   */
  intern(str) {
    if (str === null || str === undefined) return -1;
    
    const existing = this._stringToId.get(str);
    if (existing !== undefined) {
      this.stats.hits++;
      return existing;
    }

    const id = this._nextId++;
    this._stringToId.set(str, id);
    this._idToString.push(str);
    this.stats.misses++;
    this.stats.interns++;
    return id;
  }

  /**
   * Look up the string for an ID.
   */
  lookup(id) {
    if (id < 0 || id >= this._idToString.length) return null;
    return this._idToString[id];
  }

  /**
   * Check if a string is interned.
   */
  has(str) {
    return this._stringToId.has(str);
  }

  /**
   * Get the ID for a string without interning it.
   * Returns undefined if not interned.
   */
  getId(str) {
    return this._stringToId.get(str);
  }

  /**
   * Number of unique strings interned.
   */
  get size() {
    return this._idToString.length;
  }

  /**
   * Memory savings estimate: original vs interned.
   */
  memorySavings(totalReferences) {
    const uniqueStrings = this._idToString.length;
    const avgStringLen = uniqueStrings > 0
      ? this._idToString.reduce((sum, s) => sum + (s?.length || 0), 0) / uniqueStrings
      : 0;

    // Original: totalReferences * avgStringLen * 2 bytes (UTF-16)
    const originalBytes = totalReferences * avgStringLen * 2;
    // Interned: uniqueStrings * avgStringLen * 2 + totalReferences * 4 (int32 IDs)
    const internedBytes = uniqueStrings * avgStringLen * 2 + totalReferences * 4;

    return {
      original: originalBytes,
      interned: internedBytes,
      savedBytes: originalBytes - internedBytes,
      savedPercent: originalBytes > 0 ? ((originalBytes - internedBytes) / originalBytes * 100).toFixed(1) : 0,
      uniqueStrings,
      totalReferences,
    };
  }
}

/**
 * DictionaryEncodedColumn — stores a column as dictionary-encoded integer IDs.
 * For low-cardinality text columns (region, status, category).
 */
export class DictionaryEncodedColumn {
  constructor() {
    this.pool = new StringInternPool();
    this._ids = []; // Encoded column values as integer IDs
  }

  /**
   * Append a value to the column.
   */
  push(value) {
    this._ids.push(this.pool.intern(value));
  }

  /**
   * Get the decoded value at index.
   */
  get(index) {
    return this.pool.lookup(this._ids[index]);
  }

  /**
   * Get the encoded ID at index (for fast comparison).
   */
  getId(index) {
    return this._ids[index];
  }

  /**
   * Equality filter: return indices where column equals value.
   * Uses integer comparison (fast) instead of string comparison.
   */
  filterEquals(value) {
    const targetId = this.pool.getId(value);
    if (targetId === undefined) return []; // Value not in dictionary

    const result = [];
    for (let i = 0; i < this._ids.length; i++) {
      if (this._ids[i] === targetId) result.push(i);
    }
    return result;
  }

  /**
   * Batch equality filter using Uint32Array for results.
   */
  filterEqualsBatch(value) {
    const targetId = this.pool.getId(value);
    if (targetId === undefined) return new Uint32Array(0);

    const selection = new Uint32Array(this._ids.length);
    let count = 0;
    for (let i = 0; i < this._ids.length; i++) {
      if (this._ids[i] === targetId) selection[count++] = i;
    }
    return selection.subarray(0, count);
  }

  /**
   * IN filter: return indices where column is in the set.
   */
  filterIn(values) {
    const targetIds = new Set();
    for (const v of values) {
      const id = this.pool.getId(v);
      if (id !== undefined) targetIds.add(id);
    }
    if (targetIds.size === 0) return [];

    const result = [];
    for (let i = 0; i < this._ids.length; i++) {
      if (targetIds.has(this._ids[i])) result.push(i);
    }
    return result;
  }

  /**
   * GROUP BY this column: return groups as Map<string, number[]>.
   */
  groupBy() {
    const groups = new Map();
    for (let i = 0; i < this._ids.length; i++) {
      const id = this._ids[i];
      if (!groups.has(id)) groups.set(id, []);
      groups.get(id).push(i);
    }

    // Convert IDs back to strings for keys
    const result = new Map();
    for (const [id, indices] of groups) {
      result.set(this.pool.lookup(id), indices);
    }
    return result;
  }

  get length() {
    return this._ids.length;
  }

  get cardinality() {
    return this.pool.size;
  }

  getStats() {
    return {
      length: this._ids.length,
      cardinality: this.pool.size,
      compressionRatio: this._ids.length > 0 ? (this._ids.length / this.pool.size).toFixed(1) : 0,
      ...this.pool.memorySavings(this._ids.length),
    };
  }
}
