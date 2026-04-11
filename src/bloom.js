// bloom.js — Bloom filter for probabilistic set membership
// Uses double hashing from SHA-256 for k hash functions.

import { sha256 } from './sha256.js';

/**
 * BloomFilter — space-efficient probabilistic set membership.
 * False positives possible, false negatives impossible.
 * 
 * Uses double hashing: h_i(x) = h1(x) + i * h2(x) mod m
 * where h1 and h2 are derived from SHA-256.
 */
export class BloomFilter {
  /**
   * Create a Bloom filter.
   * @param {number} expectedItems — expected number of items
   * @param {number} [falsePositiveRate=0.01] — desired false positive rate
   */
  constructor(expectedItems, falsePositiveRate = 0.01) {
    if (expectedItems <= 0) throw new Error('expectedItems must be positive');
    if (falsePositiveRate <= 0 || falsePositiveRate >= 1) {
      throw new Error('falsePositiveRate must be between 0 and 1');
    }
    
    // Optimal bit array size: m = -n * ln(p) / (ln(2))^2
    this._m = Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.LN2 * Math.LN2));
    
    // Optimal number of hash functions: k = (m/n) * ln(2)
    this._k = Math.max(1, Math.round((this._m / expectedItems) * Math.LN2));
    
    // Bit array (using Uint8Array for space efficiency)
    this._bits = new Uint8Array(Math.ceil(this._m / 8));
    this._count = 0;
    this._expectedItems = expectedItems;
    this._targetFPR = falsePositiveRate;
  }

  /** Number of bits in the filter. */
  get bitCount() { return this._m; }
  
  /** Number of hash functions. */
  get hashCount() { return this._k; }
  
  /** Number of items added. */
  get count() { return this._count; }
  
  /** Memory usage in bytes. */
  get byteSize() { return this._bits.length; }

  /**
   * Add an item to the filter.
   * @param {string} item
   */
  add(item) {
    const hashes = this._getHashes(item);
    for (const h of hashes) {
      this._setBit(h);
    }
    this._count++;
  }

  /**
   * Test if an item might be in the filter.
   * @param {string} item
   * @returns {boolean} — true = maybe in set, false = definitely not
   */
  test(item) {
    const hashes = this._getHashes(item);
    for (const h of hashes) {
      if (!this._getBit(h)) return false;
    }
    return true;
  }

  /**
   * Estimate the current false positive rate.
   * FPR ≈ (1 - e^(-kn/m))^k
   */
  estimateFPR() {
    const exponent = -this._k * this._count / this._m;
    return Math.pow(1 - Math.exp(exponent), this._k);
  }

  /**
   * Merge another Bloom filter into this one (OR of bit arrays).
   * Filters must have the same size.
   */
  merge(other) {
    if (this._m !== other._m || this._k !== other._k) {
      throw new Error('Cannot merge filters with different parameters');
    }
    for (let i = 0; i < this._bits.length; i++) {
      this._bits[i] |= other._bits[i];
    }
    this._count += other._count;
  }

  /**
   * Create a new filter from an existing set of items.
   * @param {string[]} items
   * @param {number} [fpr=0.01]
   * @returns {BloomFilter}
   */
  static from(items, fpr = 0.01) {
    const bf = new BloomFilter(Math.max(items.length, 1), fpr);
    for (const item of items) bf.add(item);
    return bf;
  }

  // ---- Internal ----

  /** Double hashing: derive k hash positions from SHA-256. */
  _getHashes(item) {
    const hash = sha256(item);
    // Split 64 hex chars into two 32-char halves
    const h1 = parseInt(hash.slice(0, 8), 16);
    const h2 = parseInt(hash.slice(8, 16), 16);
    
    const positions = [];
    for (let i = 0; i < this._k; i++) {
      // Double hashing: h(i) = (h1 + i * h2) mod m
      positions.push(((h1 + i * h2) % this._m + this._m) % this._m);
    }
    return positions;
  }

  _setBit(pos) {
    const byteIdx = pos >> 3;
    const bitIdx = pos & 7;
    this._bits[byteIdx] |= (1 << bitIdx);
  }

  _getBit(pos) {
    const byteIdx = pos >> 3;
    const bitIdx = pos & 7;
    return (this._bits[byteIdx] & (1 << bitIdx)) !== 0;
  }
}
