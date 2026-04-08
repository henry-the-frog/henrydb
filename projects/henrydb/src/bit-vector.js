// bit-vector.js — Fixed-size bit vector for bitmap index operations

export class BitVector {
  constructor(size) {
    this.size = size;
    this._words = new Uint32Array(Math.ceil(size / 32));
  }

  set(pos) { this._words[pos >>> 5] |= (1 << (pos & 31)); }
  clear(pos) { this._words[pos >>> 5] &= ~(1 << (pos & 31)); }
  get(pos) { return (this._words[pos >>> 5] >>> (pos & 31)) & 1; }
  
  /** AND two bit vectors */
  and(other) {
    const result = new BitVector(Math.min(this.size, other.size));
    for (let i = 0; i < result._words.length; i++) result._words[i] = this._words[i] & other._words[i];
    return result;
  }

  /** OR two bit vectors */
  or(other) {
    const result = new BitVector(Math.max(this.size, other.size));
    for (let i = 0; i < this._words.length; i++) result._words[i] |= this._words[i];
    for (let i = 0; i < other._words.length; i++) result._words[i] |= other._words[i];
    return result;
  }

  /** NOT (complement) */
  not() {
    const result = new BitVector(this.size);
    for (let i = 0; i < this._words.length; i++) result._words[i] = ~this._words[i];
    return result;
  }

  /** XOR */
  xor(other) {
    const result = new BitVector(Math.max(this.size, other.size));
    for (let i = 0; i < Math.min(this._words.length, other._words.length); i++) {
      result._words[i] = this._words[i] ^ other._words[i];
    }
    return result;
  }

  /** Population count (number of set bits) */
  popcount() {
    let count = 0;
    for (let i = 0; i < this._words.length; i++) {
      let w = this._words[i];
      w = w - ((w >>> 1) & 0x55555555);
      w = (w & 0x33333333) + ((w >>> 2) & 0x33333333);
      count += (((w + (w >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
    }
    return count;
  }

  /** Iterate over set bit positions */
  *ones() {
    for (let i = 0; i < this._words.length; i++) {
      let w = this._words[i];
      while (w) {
        const bit = w & (-w >>> 0); // Lowest set bit (unsigned)
        yield (i * 32) + (31 - Math.clz32(bit));
        w = (w ^ bit) >>> 0;
      }
    }
  }

  /** Set all bits */
  setAll() { this._words.fill(0xFFFFFFFF); }
  
  /** Clear all bits */
  clearAll() { this._words.fill(0); }
}

/**
 * BitmapScan — combine bitmap indexes to evaluate complex predicates.
 * WHERE age > 25 AND dept = 'eng' → bitmap_age_gt_25 AND bitmap_dept_eng
 */
export class BitmapScan {
  constructor(numRows) {
    this.numRows = numRows;
  }

  /** Create bitmap from predicate on data */
  createBitmap(data, predicate) {
    const bv = new BitVector(data.length);
    for (let i = 0; i < data.length; i++) {
      if (predicate(data[i])) bv.set(i);
    }
    return bv;
  }

  /** Combine bitmaps with AND/OR */
  combine(bitmaps, op = 'AND') {
    if (bitmaps.length === 0) return new BitVector(this.numRows);
    let result = bitmaps[0];
    for (let i = 1; i < bitmaps.length; i++) {
      result = op === 'AND' ? result.and(bitmaps[i]) : result.or(bitmaps[i]);
    }
    return result;
  }

  /** Fetch matching rows using bitmap */
  fetch(data, bitmap) {
    const results = [];
    for (const pos of bitmap.ones()) {
      if (pos < data.length) results.push(data[pos]);
    }
    return results;
  }
}
