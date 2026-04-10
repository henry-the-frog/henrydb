// hyperloglog.js — HyperLogLog Probabilistic Cardinality Estimation
//
// Estimates COUNT(DISTINCT) using only O(m) bytes where m = 2^p registers.
// Standard error: 1.04/sqrt(m)
//
// With p=14 (16384 registers): ~1.5KB memory, ~0.81% error.
// Used in: Redis PFCOUNT, PostgreSQL, Google BigQuery, Apache Flink.
//
// Reference: Flajolet et al., "HyperLogLog: the analysis of a near-optimal
// cardinality estimation algorithm" (2007)

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// Second hash for 64-bit simulation (better distribution)
function hash64(key) {
  const str = typeof key === 'string' ? key : String(key);
  const h1 = fnv1a(str);
  const h2 = fnv1a(str + '\x00');
  return [h1, h2]; // Simulated 64-bit hash as two 32-bit halves
}

/**
 * Count leading zeros of the lower bits after taking p bits for bucket.
 */
function countLeadingZeros(hash, p) {
  // Use the bits after the p prefix bits
  const w = hash >>> p; // Remaining bits
  if (w === 0) return 32 - p;
  let zeros = 0;
  let v = w;
  while ((v & 1) === 0 && zeros < (32 - p)) {
    zeros++;
    v >>>= 1;
  }
  return zeros + 1; // +1 because we count the position of the first 1
}

/**
 * HyperLogLog — probabilistic distinct count estimator.
 */
export class HyperLogLog {
  /**
   * @param {number} p - Precision (4-18). Number of registers = 2^p.
   *   p=14 → 16384 registers, ~0.81% error, 16KB memory
   *   p=10 → 1024 registers, ~3.25% error, 1KB memory
   */
  constructor(p = 14) {
    if (p < 4 || p > 18) throw new Error('Precision must be between 4 and 18');
    this._p = p;
    this._m = 1 << p;            // Number of registers
    this._registers = new Uint8Array(this._m); // Max value: 64 - p + 1
    this._alpha = this._getAlpha(this._m);
  }

  get precision() { return this._p; }
  get registerCount() { return this._m; }
  get memoryBytes() { return this._registers.byteLength; }

  /**
   * Add an element to the set.
   */
  add(element) {
    const str = typeof element === 'string' ? element : String(element);
    const hash = fnv1a(str);
    
    // Use first p bits to select register
    const idx = hash >>> (32 - this._p);
    // Count leading zeros of remaining bits + 1
    const rank = countLeadingZeros(hash << this._p >>> this._p, 0);
    
    // Update register with max
    if (rank > this._registers[idx]) {
      this._registers[idx] = rank;
    }
  }

  /**
   * Estimate the number of distinct elements.
   */
  estimate() { return this.count(); }

  count() {
    // Harmonic mean of 2^(-register[i])
    let sum = 0;
    let zeroRegisters = 0;
    
    for (let i = 0; i < this._m; i++) {
      sum += Math.pow(2, -this._registers[i]);
      if (this._registers[i] === 0) zeroRegisters++;
    }
    
    let estimate = this._alpha * this._m * this._m / sum;
    
    // Small range correction (linear counting)
    if (estimate <= 2.5 * this._m && zeroRegisters > 0) {
      estimate = this._m * Math.log(this._m / zeroRegisters);
    }
    
    // Large range correction (for 32-bit hashes)
    const twoTo32 = 4294967296;
    if (estimate > twoTo32 / 30) {
      estimate = -twoTo32 * Math.log(1 - estimate / twoTo32);
    }
    
    return Math.round(estimate);
  }

  /**
   * Merge another HLL into this one (for distributed counting).
   * Takes the max of each register.
   */
  merge(other) {
    if (this._p !== other._p) throw new Error('Cannot merge HLLs with different precision');
    for (let i = 0; i < this._m; i++) {
      if (other._registers[i] > this._registers[i]) {
        this._registers[i] = other._registers[i];
      }
    }
    return this;
  }

  /**
   * Create a new HLL that is the union of this and other (non-mutating).
   */
  mergeNew(other) {
    if (this._p !== other._p) throw new Error('Cannot merge HLLs with different precision');
    const result = new HyperLogLog(this._p);
    for (let i = 0; i < this._m; i++) {
      result._registers[i] = Math.max(this._registers[i], other._registers[i]);
    }
    return result;
  }

  /**
   * Standard error of the estimate.
   */
  standardError() {
    return 1.04 / Math.sqrt(this._m);
  }

  /**
   * Get the alpha correction constant.
   */
  _getAlpha(m) {
    if (m === 16) return 0.673;
    if (m === 32) return 0.697;
    if (m === 64) return 0.709;
    return 0.7213 / (1 + 1.079 / m);
  }

  getStats() {
    return {
      precision: this._p,
      registers: this._m,
      bytesUsed: this._registers.byteLength,
      estimate: this.count()
    };
  }
}
