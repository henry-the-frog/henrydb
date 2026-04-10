// bitmap-index.js — Bitmap index for low-cardinality columns
// Each distinct value gets a bit vector. Very fast for AND/OR/NOT queries.
// Used in column stores and OLAP databases (Oracle, Vertica).

export class BitmapIndex {
  constructor() {
    this._bitmaps = new Map(); // value → Uint32Array
    this._size = 0;
    this._wordCount = 0;
  }

  get size() { return this._size; }
  get values() { return [...this._bitmaps.keys()]; }

  /**
   * Set bit for row at given value. O(1).
   */
  set(rowId, value) {
    if (!this._bitmaps.has(value)) {
      this._bitmaps.set(value, new Uint32Array(Math.max(1, this._wordCount)));
    }
    
    const wordIdx = rowId >>> 5;
    const bitIdx = rowId & 31;
    
    // Grow bitmaps if needed
    if (wordIdx >= this._wordCount) {
      const newWordCount = wordIdx + 1;
      for (const [v, bm] of this._bitmaps) {
        const newBm = new Uint32Array(newWordCount);
        newBm.set(bm);
        this._bitmaps.set(v, newBm);
      }
      this._wordCount = newWordCount;
    }
    
    this._bitmaps.get(value)[wordIdx] |= (1 << bitIdx);
    if (rowId >= this._size) this._size = rowId + 1;
  }

  /**
   * Get all row IDs matching a value. O(n/32).
   */
  lookup(value) {
    const bm = this._bitmaps.get(value);
    if (!bm) return [];
    return this._bitmapToRows(bm);
  }

  /**
   * AND: rows matching ALL values.
   */
  and(values) {
    const bitmaps = values.map(v => this._bitmaps.get(v)).filter(Boolean);
    if (bitmaps.length === 0) return [];
    
    const result = new Uint32Array(this._wordCount);
    result.set(bitmaps[0]);
    for (let i = 1; i < bitmaps.length; i++) {
      for (let j = 0; j < this._wordCount; j++) result[j] &= bitmaps[i][j];
    }
    return this._bitmapToRows(result);
  }

  /**
   * OR: rows matching ANY value.
   */
  or(values) {
    const result = new Uint32Array(this._wordCount);
    for (const v of values) {
      const bm = this._bitmaps.get(v);
      if (bm) for (let j = 0; j < this._wordCount; j++) result[j] |= bm[j];
    }
    return this._bitmapToRows(result);
  }

  /**
   * NOT: rows NOT matching a value.
   */
  not(value) {
    const bm = this._bitmaps.get(value);
    const result = new Uint32Array(this._wordCount);
    if (bm) {
      for (let j = 0; j < this._wordCount; j++) result[j] = ~bm[j];
    } else {
      result.fill(0xFFFFFFFF);
    }
    return this._bitmapToRows(result).filter(r => r < this._size);
  }

  /**
   * Count rows matching a value. O(n/32).
   */
  count(value) {
    const bm = this._bitmaps.get(value);
    if (!bm) return 0;
    let count = 0;
    for (const word of bm) {
      let v = word;
      v = v - ((v >> 1) & 0x55555555);
      v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
      count += ((v + (v >> 4) & 0xF0F0F0F) * 0x1010101) >> 24;
    }
    return count;
  }

  _bitmapToRows(bm) {
    const rows = [];
    for (let w = 0; w < bm.length; w++) {
      let word = bm[w];
      while (word) {
        const bit = word & (-word);
        const bitIdx = 31 - Math.clz32(bit);
        rows.push(w * 32 + bitIdx);
        word ^= bit;
      }
    }
    return rows;
  }

  getStats() {
    return {
      rows: this._size,
      distinctValues: this._bitmaps.size,
      bytesUsed: this._bitmaps.size * this._wordCount * 4,
    };
  }

  /** Bulk build from an array of values (one value per row). */
  build(values) {
    for (let i = 0; i < values.length; i++) {
      this.set(i, values[i]);
    }
  }

  /** Get a bitmap result for equality check. Returns a BitmapResult. */
  eq(value) {
    const bm = this._bitmaps.get(value);
    return new BitmapResult(bm ? bm.slice() : new Uint32Array(this._wordCount), this._wordCount, this._size);
  }
}

/** Chainable bitmap result for AND/OR/NOT operations. */
class BitmapResult {
  constructor(bitmap, wordCount, size) {
    this._bitmap = bitmap;
    this._wordCount = wordCount;
    this._size = size;
  }

  and(other) {
    const result = new Uint32Array(this._wordCount);
    for (let i = 0; i < this._wordCount; i++) {
      result[i] = this._bitmap[i] & other._bitmap[i];
    }
    return new BitmapResult(result, this._wordCount, this._size);
  }

  or(other) {
    const result = new Uint32Array(this._wordCount);
    for (let i = 0; i < this._wordCount; i++) {
      result[i] = this._bitmap[i] | other._bitmap[i];
    }
    return new BitmapResult(result, this._wordCount, this._size);
  }

  not() {
    const result = new Uint32Array(this._wordCount);
    for (let i = 0; i < this._wordCount; i++) {
      result[i] = ~this._bitmap[i];
    }
    return new BitmapResult(result, this._wordCount, this._size);
  }

  count() {
    let count = 0;
    for (let i = 0; i < this._wordCount; i++) {
      let v = this._bitmap[i];
      v = v - ((v >> 1) & 0x55555555);
      v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
      count += ((v + (v >> 4) & 0xF0F0F0F) * 0x1010101) >> 24;
    }
    return count;
  }

  toRows() {
    const rows = [];
    for (let w = 0; w < this._wordCount; w++) {
      let word = this._bitmap[w];
      while (word) {
        const bit = word & (-word);
        const bitIdx = 31 - Math.clz32(bit);
        rows.push(w * 32 + bitIdx);
        word ^= bit;
      }
    }
    return rows;
  }
}

export { BitVector } from './bit-vector.js';
