// bitmap-index.js — Bitmap Index for low-cardinality columns
// Each distinct value gets a bit vector. Bit i is set if row i has that value.
// Enables fast AND/OR/NOT operations for complex predicates.
// Ideal for columns with few distinct values (status, gender, country, etc.)

/**
 * BitVector — compressed bit array with set operations.
 */
export class BitVector {
  constructor(size = 0) {
    this._words = new Uint32Array(Math.ceil(size / 32));
    this._size = size;
  }

  set(pos) {
    const word = pos >>> 5;
    const bit = pos & 31;
    if (word >= this._words.length) this._grow(word + 1);
    this._words[word] |= (1 << bit);
  }

  clear(pos) {
    const word = pos >>> 5;
    const bit = pos & 31;
    if (word < this._words.length) this._words[word] &= ~(1 << bit);
  }

  get(pos) {
    const word = pos >>> 5;
    const bit = pos & 31;
    return word < this._words.length ? (this._words[word] & (1 << bit)) !== 0 : false;
  }

  and(other) {
    const len = Math.min(this._words.length, other._words.length);
    const result = new BitVector(Math.max(this._size, other._size));
    result._words = new Uint32Array(len);
    for (let i = 0; i < len; i++) result._words[i] = this._words[i] & other._words[i];
    return result;
  }

  or(other) {
    const len = Math.max(this._words.length, other._words.length);
    const result = new BitVector(Math.max(this._size, other._size));
    result._words = new Uint32Array(len);
    for (let i = 0; i < len; i++) {
      const a = i < this._words.length ? this._words[i] : 0;
      const b = i < other._words.length ? other._words[i] : 0;
      result._words[i] = a | b;
    }
    return result;
  }

  not(totalBits) {
    const result = new BitVector(totalBits);
    const len = Math.ceil(totalBits / 32);
    result._words = new Uint32Array(len);
    for (let i = 0; i < len; i++) {
      result._words[i] = i < this._words.length ? ~this._words[i] : 0xFFFFFFFF;
    }
    // Clear bits beyond totalBits
    const extraBits = totalBits & 31;
    if (extraBits > 0 && len > 0) {
      result._words[len - 1] &= (1 << extraBits) - 1;
    }
    return result;
  }

  popcount() {
    let count = 0;
    for (let i = 0; i < this._words.length; i++) {
      let w = this._words[i];
      // Hamming weight
      w = w - ((w >>> 1) & 0x55555555);
      w = (w & 0x33333333) + ((w >>> 2) & 0x33333333);
      count += (((w + (w >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
    }
    return count;
  }

  *positions() {
    for (let w = 0; w < this._words.length; w++) {
      let word = this._words[w];
      while (word !== 0) {
        const bit = word & (-word); // Lowest set bit
        const pos = (w << 5) + (31 - Math.clz32(bit));
        yield pos;
        word ^= bit;
      }
    }
  }

  _grow(newLen) {
    const old = this._words;
    this._words = new Uint32Array(newLen);
    this._words.set(old);
  }
}

/**
 * BitmapIndex — bitmap index for a single column.
 */
export class BitmapIndex {
  constructor(name, column) {
    this.name = name || '';
    this.column = column || '';
    this._bitmaps = new Map(); // value → BitVector
    this._rowCount = 0;
  }

  /**
   * Build index from column values.
   */
  build(values) {
    this._rowCount = values.length;
    this._bitmaps.clear();

    for (let i = 0; i < values.length; i++) {
      const val = values[i];
      if (!this._bitmaps.has(val)) {
        this._bitmaps.set(val, new BitVector(values.length));
      }
      this._bitmaps.get(val).set(i);
    }
  }

  /**
   * Get bitmap for a specific value.
   */
  eq(value) {
    return this._bitmaps.get(value) || new BitVector(this._rowCount);
  }

  /**
   * Get bitmap for IN (value1, value2, ...).
   */
  in(values) {
    let result = new BitVector(this._rowCount);
    for (const v of values) {
      const bm = this._bitmaps.get(v);
      if (bm) result = result.or(bm);
    }
    return result;
  }

  /**
   * Get bitmap for NOT value.
   */
  neq(value) {
    const bm = this.eq(value);
    return bm.not(this._rowCount);
  }

  /**
   * Get matching row indices for a value.
   */
  getRows(value) {
    return [...this.eq(value).positions()];
  }

  get distinctValues() { return [...this._bitmaps.keys()]; }
  get cardinality() { return this._bitmaps.size; }
  get rowCount() { return this._rowCount; }

  getStats() {
    return {
      rowCount: this._rowCount,
      distinctValues: this._bitmaps.size,
      memoryWords: [...this._bitmaps.values()].reduce((s, bv) => s + bv._words.length, 0),
    };
  }

  /**
   * Add a single row to the index.
   */
  addRow(rowId, value) {
    if (!this._bitmaps.has(value)) {
      this._bitmaps.set(value, new BitVector(Math.max(this._rowCount, rowId + 1)));
    }
    // Ensure bitmap is large enough
    const bm = this._bitmaps.get(value);
    bm.set(rowId);
    if (rowId >= this._rowCount) this._rowCount = rowId + 1;
  }

  /**
   * Count rows matching a value.
   */
  count(value) {
    const bm = this._bitmaps.get(value);
    return bm ? bm.popcount() : 0;
  }
}
