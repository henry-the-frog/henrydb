// counting-bloom.js — Counting Bloom Filter
//
// Standard Bloom filters can't delete because decrementing a shared bit
// might affect other elements. Counting Bloom Filters (CBFs) solve this
// by replacing each bit with a counter.
//
// Operations: O(k) where k = number of hash functions
// Space: 4x more than standard Bloom (4-bit counters vs 1-bit)
//
// Used in: network routers (packet deduplication), distributed caches,
// LSM-tree level filters (delete without rebuild).

/**
 * Hash function using MurmurHash3-style mixing with different seeds.
 */
function hash(key, seed) {
  let h = seed;
  const str = typeof key === 'string' ? key : String(key);
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 13;
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 15;
  }
  return h >>> 0;
}

/**
 * CountingBloomFilter — Bloom filter with counter-based deletion support.
 */
export class CountingBloomFilter {
  /**
   * @param {number} expectedItems - Expected number of items
   * @param {number} fpr - Target false positive rate (default 0.01 = 1%)
   * @param {number} counterBits - Bits per counter (default 4, max count = 15)
   */
  constructor(expectedItems, fpr = 0.01, counterBits = 4) {
    // Optimal filter parameters
    const m = Math.ceil(-expectedItems * Math.log(fpr) / (Math.LN2 * Math.LN2));
    const k = Math.max(1, Math.round(m / expectedItems * Math.LN2));
    
    this._m = m;              // Number of counter slots
    this._k = k;              // Number of hash functions
    this._counterBits = counterBits;
    this._maxCount = (1 << counterBits) - 1;
    
    // Store counters in a typed array
    // 4-bit counters → pack 2 per byte (or use Uint8Array for simplicity)
    this._counters = new Uint8Array(m); // Each byte is one counter (up to 255)
    this._count = 0;           // Number of items added (net)
    this._seeds = Array.from({ length: k }, (_, i) => 0x9e3779b9 + i * 0x517cc1b7);
  }

  get size() { return this._count; }
  get numSlots() { return this._m; }
  get numHashes() { return this._k; }

  /**
   * Get the k hash positions for a key.
   */
  _positions(key) {
    const positions = new Array(this._k);
    for (let i = 0; i < this._k; i++) {
      positions[i] = hash(key, this._seeds[i]) % this._m;
    }
    return positions;
  }

  /**
   * Insert a key into the filter.
   */
  insert(key) {
    const positions = this._positions(key);
    for (const pos of positions) {
      if (this._counters[pos] < this._maxCount) {
        this._counters[pos]++;
      }
      // If counter overflows, leave at max (conservative — won't false-delete)
    }
    this._count++;
  }

  /**
   * Delete a key from the filter.
   * WARNING: Only delete keys that were actually inserted! Deleting a
   * non-inserted key can cause false negatives (the one thing Bloom
   * filters are supposed to never have).
   */
  delete(key) {
    // First check if the key might be present
    if (!this.contains(key)) return false;
    
    const positions = this._positions(key);
    for (const pos of positions) {
      if (this._counters[pos] > 0) {
        this._counters[pos]--;
      }
    }
    this._count--;
    return true;
  }

  /**
   * Test if a key might be in the set.
   * Returns true if key MIGHT be present (possible false positive).
   * Returns false if key is DEFINITELY not present.
   */
  contains(key) {
    const positions = this._positions(key);
    for (const pos of positions) {
      if (this._counters[pos] === 0) return false;
    }
    return true;
  }

  /**
   * Merge another counting bloom filter into this one.
   * Both must have the same parameters (m, k).
   */
  merge(other) {
    if (this._m !== other._m || this._k !== other._k) {
      throw new Error('Cannot merge filters with different parameters');
    }
    for (let i = 0; i < this._m; i++) {
      const sum = this._counters[i] + other._counters[i];
      this._counters[i] = Math.min(sum, this._maxCount);
    }
    this._count += other._count;
  }

  /**
   * Get estimated false positive rate based on current load.
   */
  estimatedFPR() {
    // Count non-zero slots
    let nonZero = 0;
    for (let i = 0; i < this._m; i++) {
      if (this._counters[i] > 0) nonZero++;
    }
    const fillRatio = nonZero / this._m;
    return Math.pow(fillRatio, this._k);
  }

  /**
   * Convert to a standard (non-counting) bloom filter for space savings.
   * Loses deletion capability.
   */
  toBitArray() {
    const bits = new Uint8Array(Math.ceil(this._m / 8));
    for (let i = 0; i < this._m; i++) {
      if (this._counters[i] > 0) {
        bits[i >>> 3] |= (1 << (i & 7));
      }
    }
    return bits;
  }

  getStats() {
    let maxCounter = 0;
    let nonZero = 0;
    let totalCount = 0;
    for (let i = 0; i < this._m; i++) {
      if (this._counters[i] > 0) nonZero++;
      if (this._counters[i] > maxCounter) maxCounter = this._counters[i];
      totalCount += this._counters[i];
    }
    return {
      items: this._count,
      slots: this._m,
      hashes: this._k,
      fillRatio: nonZero / this._m,
      maxCounter,
      avgCounter: nonZero > 0 ? totalCount / nonZero : 0,
      estimatedFPR: this.estimatedFPR(),
      bytesUsed: this._counters.byteLength,
    };
  }
}
