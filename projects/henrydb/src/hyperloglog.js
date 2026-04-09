// hyperloglog.js — Cardinality estimation using HyperLogLog
//
// HyperLogLog (HLL) estimates the number of distinct elements in a multiset
// using only O(log log n) space. With 2^14 (16384) registers, it achieves
// ~0.81% standard error while using only 12KB of memory.
//
// Used in databases for:
//   - Approximate COUNT(DISTINCT col) without building full hash set
//   - Cardinality estimation for query planning
//   - Network monitoring (distinct IPs, users)
//
// Algorithm:
//   1. Hash each element
//   2. Use first p bits to select a register (bucket)
//   3. Count leading zeros in remaining bits
//   4. Store max(leading zeros + 1) in each register
//   5. Harmonic mean of 2^(-register) gives cardinality estimate

function hash32(key) {
  const str = typeof key === 'string' ? key : String(key);
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  // Additional mixing
  h ^= h >>> 16;
  h = (h * 0x85ebca6b) >>> 0;
  h ^= h >>> 13;
  h = (h * 0xc2b2ae35) >>> 0;
  h ^= h >>> 16;
  return h;
}

function countLeadingZeros32(x) {
  if (x === 0) return 32;
  let n = 0;
  if ((x & 0xFFFF0000) === 0) { n += 16; x <<= 16; }
  if ((x & 0xFF000000) === 0) { n += 8; x <<= 8; }
  if ((x & 0xF0000000) === 0) { n += 4; x <<= 4; }
  if ((x & 0xC0000000) === 0) { n += 2; x <<= 2; }
  if ((x & 0x80000000) === 0) { n += 1; }
  return n;
}

/**
 * HyperLogLog — Probabilistic cardinality estimator.
 */
export class HyperLogLog {
  /**
   * @param {number} precision - Number of bits for register addressing (4-18, default 14)
   *                             2^precision registers, using ~(2^precision * 6 bits) memory
   */
  constructor(precision = 14) {
    this.p = precision;
    this.m = 1 << precision;        // Number of registers
    this._registers = new Uint8Array(this.m); // Max value per register
    
    // Bias correction constant (depends on m)
    if (this.m === 16) this._alpha = 0.673;
    else if (this.m === 32) this._alpha = 0.697;
    else if (this.m === 64) this._alpha = 0.709;
    else this._alpha = 0.7213 / (1 + 1.079 / this.m);
  }

  /**
   * Add an element to the estimator.
   */
  add(key) {
    const h = hash32(key);
    
    // First p bits → register index
    const registerIdx = h >>> (32 - this.p);
    
    // Remaining bits → count leading zeros + 1
    const w = (h << this.p) >>> 0; // Shift out the register bits
    const rho = countLeadingZeros32(w) + 1;
    
    // Store maximum rho for this register
    if (rho > this._registers[registerIdx]) {
      this._registers[registerIdx] = rho;
    }
  }

  /**
   * Estimate the cardinality (number of distinct elements).
   */
  estimate() {
    // Raw HLL estimate: alpha * m^2 * (sum of 2^(-register))^(-1)
    let sum = 0;
    let zeros = 0;
    
    for (let i = 0; i < this.m; i++) {
      sum += Math.pow(2, -this._registers[i]);
      if (this._registers[i] === 0) zeros++;
    }
    
    let estimate = this._alpha * this.m * this.m / sum;
    
    // Small range correction (Linear Counting)
    if (estimate <= 2.5 * this.m && zeros > 0) {
      estimate = this.m * Math.log(this.m / zeros);
    }
    
    // Large range correction (for 32-bit hash)
    const TWO_32 = 4294967296; // 2^32
    if (estimate > TWO_32 / 30) {
      estimate = -TWO_32 * Math.log(1 - estimate / TWO_32);
    }
    
    return Math.round(estimate);
  }

  /**
   * Merge with another HyperLogLog (union of sets).
   * Both must have the same precision.
   */
  merge(other) {
    if (this.p !== other.p) throw new Error('Precision mismatch');
    const merged = new HyperLogLog(this.p);
    for (let i = 0; i < this.m; i++) {
      merged._registers[i] = Math.max(this._registers[i], other._registers[i]);
    }
    return merged;
  }

  /**
   * Reset all registers.
   */
  clear() {
    this._registers.fill(0);
  }

  /**
   * Get the relative standard error.
   */
  get standardError() {
    return 1.04 / Math.sqrt(this.m);
  }

  /**
   * Get statistics.
   */
  getStats() {
    return {
      precision: this.p,
      registers: this.m,
      bytesUsed: this._registers.byteLength,
      standardError: parseFloat((this.standardError * 100).toFixed(2)) + '%',
      emptyRegisters: this._registers.filter(r => r === 0).length,
      maxRegister: Math.max(...this._registers),
    };
  }
}
