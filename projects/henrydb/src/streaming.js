// streaming.js — Streaming data structures for HenryDB
// Count-Min Sketch: frequency estimation
// HyperLogLog: cardinality (distinct count) estimation

/**
 * Count-Min Sketch: probabilistic frequency counter.
 * Space-efficient: O(width × depth) space, overestimates never underestimates.
 * 
 * @param {number} width - Number of counters per hash function (higher = more accurate)
 * @param {number} depth - Number of hash functions (higher = lower error probability)
 */
export class CountMinSketch {
  constructor(width = 1024, depth = 5) {
    this._width = width;
    this._depth = depth;
    this._table = Array.from({ length: depth }, () => new Int32Array(width));
    this._totalCount = 0;
    this._seeds = Array.from({ length: depth }, (_, i) => i * 0x9e3779b9 + 0x811c9dc5);
  }

  /**
   * Increment count for an item.
   */
  add(item, count = 1) {
    const str = String(item);
    for (let i = 0; i < this._depth; i++) {
      const hash = this._hash(str, this._seeds[i]) % this._width;
      this._table[i][hash] += count;
    }
    this._totalCount += count;
  }

  /**
   * Estimate frequency of an item (may overestimate, never underestimates).
   */
  estimate(item) {
    const str = String(item);
    let min = Infinity;
    for (let i = 0; i < this._depth; i++) {
      const hash = this._hash(str, this._seeds[i]) % this._width;
      min = Math.min(min, this._table[i][hash]);
    }
    return min;
  }

  get totalCount() { return this._totalCount; }

  _hash(str, seed) {
    let hash = seed;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }
}

/**
 * HyperLogLog: cardinality (distinct count) estimation.
 * Uses O(m) space to estimate cardinality of a set with ~1.04/√m relative error.
 * 
 * @param {number} precision - Number of bits for bucket index (4-16, default 14 = 16384 registers)
 */
export class HyperLogLog {
  constructor(precision = 14) {
    this._p = Math.min(16, Math.max(4, precision));
    this._m = 1 << this._p; // Number of registers
    this._registers = new Uint8Array(this._m);
    this._alphaMM = this._getAlphaMM();
  }

  /**
   * Add an item to the set.
   */
  add(item) {
    const hash = this._hash(String(item));
    const bucketIdx = hash >>> (32 - this._p); // First p bits
    const remaining = (hash << this._p) | (1 << (this._p - 1)); // Remaining bits
    
    // Count leading zeros + 1
    const rho = this._countLeadingZeros(remaining) + 1;
    this._registers[bucketIdx] = Math.max(this._registers[bucketIdx], rho);
  }

  /**
   * Estimate the number of distinct items.
   */
  estimate() {
    // Harmonic mean of 2^(-register[i])
    let sum = 0;
    let zeros = 0;
    
    for (let i = 0; i < this._m; i++) {
      sum += Math.pow(2, -this._registers[i]);
      if (this._registers[i] === 0) zeros++;
    }
    
    let estimate = this._alphaMM / sum;
    
    // Small range correction (linear counting)
    if (estimate <= 2.5 * this._m && zeros > 0) {
      estimate = this._m * Math.log(this._m / zeros);
    }
    
    return Math.round(estimate);
  }

  /**
   * Merge another HyperLogLog into this one (union).
   */
  merge(other) {
    if (other._m !== this._m) throw new Error('Cannot merge HLLs with different precision');
    for (let i = 0; i < this._m; i++) {
      this._registers[i] = Math.max(this._registers[i], other._registers[i]);
    }
  }

  _getAlphaMM() {
    const m = this._m;
    if (m === 16) return 0.673 * m * m;
    if (m === 32) return 0.697 * m * m;
    if (m === 64) return 0.709 * m * m;
    return (0.7213 / (1 + 1.079 / m)) * m * m;
  }

  _countLeadingZeros(value) {
    if (value === 0) return 32;
    let n = 0;
    if ((value & 0xFFFF0000) === 0) { n += 16; value <<= 16; }
    if ((value & 0xFF000000) === 0) { n += 8; value <<= 8; }
    if ((value & 0xF0000000) === 0) { n += 4; value <<= 4; }
    if ((value & 0xC0000000) === 0) { n += 2; value <<= 2; }
    if ((value & 0x80000000) === 0) { n += 1; }
    return n;
  }

  _hash(str) {
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }
}
