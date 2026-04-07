// bitmap-index.js — Bitmap index for low-cardinality columns
// Each distinct value gets a bit vector where bit i = 1 if row i has that value.
// Extremely efficient for columns with few distinct values (gender, status, etc.).

export class BitmapIndex {
  constructor(name, column) {
    this.name = name;
    this.column = column;
    this._bitmaps = new Map(); // value → bit array
    this._size = 0;
  }

  /**
   * Add a row with a given value.
   */
  addRow(rowId, value) {
    if (!this._bitmaps.has(value)) {
      this._bitmaps.set(value, []);
    }
    const bitmap = this._bitmaps.get(value);
    // Ensure bitmap is large enough
    while (bitmap.length <= rowId) bitmap.push(0);
    bitmap[rowId] = 1;
    this._size = Math.max(this._size, rowId + 1);
  }

  /**
   * Find all row IDs that have a specific value. O(n/word_size).
   */
  findEqual(value) {
    const bitmap = this._bitmaps.get(value);
    if (!bitmap) return [];
    const results = [];
    for (let i = 0; i < bitmap.length; i++) {
      if (bitmap[i]) results.push(i);
    }
    return results;
  }

  /**
   * AND two bitmaps: rows matching BOTH value1 AND value2 (different columns).
   */
  static and(bitmap1, bitmap2) {
    const len = Math.min(bitmap1.length, bitmap2.length);
    const result = [];
    for (let i = 0; i < len; i++) {
      if (bitmap1[i] && bitmap2[i]) result.push(i);
    }
    return result;
  }

  /**
   * OR two bitmaps: rows matching value1 OR value2.
   */
  static or(bitmap1, bitmap2) {
    const len = Math.max(bitmap1.length, bitmap2.length);
    const result = [];
    for (let i = 0; i < len; i++) {
      if ((bitmap1[i] || 0) || (bitmap2[i] || 0)) result.push(i);
    }
    return result;
  }

  /**
   * NOT a bitmap: rows NOT having the value.
   */
  not(value) {
    const bitmap = this._bitmaps.get(value) || [];
    const results = [];
    for (let i = 0; i < this._size; i++) {
      if (!bitmap[i]) results.push(i);
    }
    return results;
  }

  /**
   * Count rows with a specific value (without materializing the list).
   */
  count(value) {
    const bitmap = this._bitmaps.get(value);
    if (!bitmap) return 0;
    return bitmap.reduce((sum, bit) => sum + bit, 0);
  }

  /**
   * Get all distinct values.
   */
  distinctValues() {
    return [...this._bitmaps.keys()];
  }

  /**
   * Get statistics.
   */
  stats() {
    return {
      column: this.column,
      distinctValues: this._bitmaps.size,
      totalRows: this._size,
      valueCounts: Object.fromEntries(
        [...this._bitmaps.entries()].map(([v, bm]) => [v, bm.reduce((s, b) => s + b, 0)])
      ),
    };
  }
}
