// bloom-filter.js — Bloom filter for HenryDB
//
// A Bloom filter is a space-efficient probabilistic data structure that tests
// whether an element is a member of a set. False positives are possible,
// but false negatives are not — if the filter says "definitely not present",
// the element is definitely not present.
//
// Use cases in a database:
//   - Skip pages that definitely don't contain matching rows (zone-maps on steroids)
//   - JOIN optimization: build bloom filter from small table, probe against large table
//   - Semi-join reduction in distributed queries
//
// Parameters:
//   - m: number of bits in the filter
//   - k: number of hash functions
//   - n: expected number of elements
//   Optimal: m = -n*ln(p) / (ln(2))^2, k = (m/n) * ln(2)

/**
 * Simple hash functions using FNV-1a variants.
 * We generate k hash functions using double hashing: h(i) = h1 + i*h2
 */
function hash1(key) {
  const str = typeof key === 'string' ? key : String(key);
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

function hash2(key) {
  const str = typeof key === 'string' ? key : String(key);
  let hash = 0x01000193;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash ^ str.charCodeAt(i)) * 0x811c9dc5) >>> 0;
  }
  return hash;
}

/**
 * BloomFilter — Probabilistic set membership testing.
 */
export class BloomFilter {
  /**
   * @param {number} expectedElements - Expected number of elements
   * @param {number} falsePositiveRate - Desired false positive rate (default: 0.01 = 1%)
   */
  constructor(expectedElements = 1000, falsePositiveRate = 0.01) {
    this.n = expectedElements;
    this.p = falsePositiveRate;
    
    // Calculate optimal parameters
    // m = -n * ln(p) / (ln(2))^2
    this.m = Math.ceil(-expectedElements * Math.log(falsePositiveRate) / (Math.LN2 * Math.LN2));
    // k = (m/n) * ln(2)
    this.k = Math.max(1, Math.round((this.m / expectedElements) * Math.LN2));
    
    // Bit array (using Uint32Array for efficiency)
    this._bits = new Uint32Array(Math.ceil(this.m / 32));
    this._count = 0;
  }

  /**
   * Create a Bloom filter with explicit bit count and hash count.
   */
  static withParams(numBits, numHashes) {
    const bf = new BloomFilter(1, 0.5); // Dummy, will override
    bf.m = numBits;
    bf.k = numHashes;
    bf._bits = new Uint32Array(Math.ceil(numBits / 32));
    bf._count = 0;
    return bf;
  }

  /**
   * Add an element to the filter.
   */
  add(key) {
    const h1 = hash1(key);
    const h2 = hash2(key);
    
    for (let i = 0; i < this.k; i++) {
      const pos = ((h1 + i * h2) >>> 0) % this.m;
      const wordIdx = pos >>> 5;     // pos / 32
      const bitIdx = pos & 31;       // pos % 32
      this._bits[wordIdx] |= (1 << bitIdx);
    }
    
    this._count++;
  }

  /**
   * Test if an element MIGHT be in the set.
   * Returns true if the element might be present (with false positive rate p).
   * Returns false if the element is DEFINITELY NOT present.
   */
  mightContain(key) {
    const h1 = hash1(key);
    const h2 = hash2(key);
    
    for (let i = 0; i < this.k; i++) {
      const pos = ((h1 + i * h2) >>> 0) % this.m;
      const wordIdx = pos >>> 5;
      const bitIdx = pos & 31;
      if ((this._bits[wordIdx] & (1 << bitIdx)) === 0) {
        return false; // Definitely not present
      }
    }
    
    return true; // Might be present
  }

  /**
   * Get the current estimated false positive rate.
   */
  get estimatedFPR() {
    // FPR ≈ (1 - e^(-kn/m))^k
    const exponent = -this.k * this._count / this.m;
    return Math.pow(1 - Math.exp(exponent), this.k);
  }

  /**
   * Get the number of bits set to 1.
   */
  get bitsSet() {
    let count = 0;
    for (const word of this._bits) {
      // popcount
      let v = word;
      v = v - ((v >> 1) & 0x55555555);
      v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
      count += ((v + (v >> 4) & 0xF0F0F0F) * 0x1010101) >> 24;
    }
    return count;
  }

  /**
   * Get the fill ratio (bits set / total bits).
   */
  get fillRatio() {
    return this.bitsSet / this.m;
  }

  /**
   * Get statistics about the filter.
   */
  getStats() {
    return {
      elements: this._count,
      bits: this.m,
      hashes: this.k,
      bytesUsed: this._bits.byteLength,
      fillRatio: parseFloat(this.fillRatio.toFixed(4)),
      estimatedFPR: parseFloat(this.estimatedFPR.toFixed(6)),
      targetFPR: this.p,
    };
  }

  /**
   * Merge two Bloom filters (OR their bit arrays).
   * Both must have the same m and k parameters.
   */
  merge(other) {
    if (this.m !== other.m || this.k !== other.k) {
      throw new Error('Cannot merge Bloom filters with different parameters');
    }
    const merged = BloomFilter.withParams(this.m, this.k);
    for (let i = 0; i < this._bits.length; i++) {
      merged._bits[i] = this._bits[i] | other._bits[i];
    }
    merged._count = this._count + other._count;
    return merged;
  }

  /**
   * Reset the filter (clear all bits).
   */
  clear() {
    this._bits.fill(0);
    this._count = 0;
  }
}
