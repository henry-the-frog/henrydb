// hyperloglog.js — HyperLogLog for approximate cardinality estimation (COUNT DISTINCT)
// Uses register array to track the maximum number of leading zeros in hashed values.
// Space: m registers * 1 byte = ~1KB for m=1024
// Error: ~1.04/√m ≈ 3.25% for m=1024, 1.6% for m=4096
//
// Based on Flajolet et al., "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"

/**
 * HyperLogLog — approximate COUNT DISTINCT.
 */
export class HyperLogLog {
  constructor(precision = 10) {
    // precision p: use 2^p registers
    this.p = precision;
    this.m = 1 << precision; // number of registers
    this._registers = new Uint8Array(this.m); // max leading zeros per register
    this._alpha = this._getAlpha(this.m);
  }

  /**
   * Add an element to the set.
   */
  add(element) {
    const hash = this._hash(String(element));
    const registerIndex = hash >>> (32 - this.p); // Use top p bits for register
    const remaining = (hash << this.p) | (1 << (this.p - 1)); // Remaining bits
    const leadingZeros = this._clz32(remaining) + 1; // Count leading zeros + 1

    if (leadingZeros > this._registers[registerIndex]) {
      this._registers[registerIndex] = leadingZeros;
    }
  }

  /**
   * Estimate the cardinality (number of distinct elements).
   */
  estimate() {
    // Raw estimate: harmonic mean of 2^(-register[j])
    let sum = 0;
    for (let j = 0; j < this.m; j++) {
      sum += Math.pow(2, -this._registers[j]);
    }
    let estimate = this._alpha * this.m * this.m / sum;

    // Small range correction (linear counting)
    if (estimate <= 2.5 * this.m) {
      let zeros = 0;
      for (let j = 0; j < this.m; j++) {
        if (this._registers[j] === 0) zeros++;
      }
      if (zeros > 0) {
        estimate = this.m * Math.log(this.m / zeros);
      }
    }

    // Large range correction (not needed for 32-bit hashes < 2^32/30)
    return Math.round(estimate);
  }

  /**
   * Merge another HyperLogLog into this one (max of registers).
   * Useful for distributed cardinality estimation.
   */
  merge(other) {
    if (other.m !== this.m) throw new Error('HLLs must have same precision');
    for (let j = 0; j < this.m; j++) {
      if (other._registers[j] > this._registers[j]) {
        this._registers[j] = other._registers[j];
      }
    }
  }

  /**
   * Relative error of the estimate.
   */
  get expectedError() {
    return 1.04 / Math.sqrt(this.m);
  }

  _hash(str) {
    let h = 0x811c9dc5; // FNV offset basis
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 0x01000193); // FNV prime
    }
    // Extra mixing
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return (h >>> 16) ^ h;
  }

  _clz32(x) {
    if (x === 0) return 32;
    let n = 0;
    if ((x & 0xFFFF0000) === 0) { n += 16; x <<= 16; }
    if ((x & 0xFF000000) === 0) { n += 8; x <<= 8; }
    if ((x & 0xF0000000) === 0) { n += 4; x <<= 4; }
    if ((x & 0xC0000000) === 0) { n += 2; x <<= 2; }
    if ((x & 0x80000000) === 0) { n += 1; }
    return n;
  }

  _getAlpha(m) {
    if (m === 16) return 0.673;
    if (m === 32) return 0.697;
    if (m === 64) return 0.709;
    return 0.7213 / (1 + 1.079 / m);
  }

  getStats() {
    return {
      precision: this.p,
      registers: this.m,
      memoryBytes: this.m,
      expectedError: (this.expectedError * 100).toFixed(2) + '%',
      estimate: this.estimate(),
    };
  }
}
