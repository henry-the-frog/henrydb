// bloom.js — Bloom filter for HenryDB
// Probabilistic data structure for fast membership testing.
// false negatives: never happens (if we say "not present", it's 100% not present)
// false positives: possible (if we say "maybe present", it might not be)

/**
 * Bloom Filter implementation.
 * 
 * @param {number} expectedItems - Expected number of items
 * @param {number} falsePositiveRate - Desired false positive rate (e.g., 0.01 for 1%)
 */
export class BloomFilter {
  constructor(expectedItems = 1000, falsePositiveRate = 0.01) {
    // Calculate optimal bit array size: m = -n*ln(p) / (ln2)^2
    this._numItems = expectedItems;
    this._fpRate = falsePositiveRate;
    this._size = Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.log(2) ** 2));
    this._size = Math.max(this._size, 64); // Minimum 64 bits
    
    // Calculate optimal number of hash functions: k = (m/n) * ln(2)
    this._numHashes = Math.max(1, Math.round((this._size / expectedItems) * Math.log(2)));
    
    // Bit array using Uint8Array
    this._bits = new Uint8Array(Math.ceil(this._size / 8));
    this._count = 0;
  }

  /**
   * Add an element to the filter.
   */
  add(value) {
    const hashes = this._getHashes(value);
    for (const h of hashes) {
      const idx = h % this._size;
      this._bits[Math.floor(idx / 8)] |= (1 << (idx % 8));
    }
    this._count++;
  }

  /**
   * Test if an element might be in the set.
   * Returns false = definitely not present, true = maybe present.
   */
  mightContain(value) {
    const hashes = this._getHashes(value);
    for (const h of hashes) {
      const idx = h % this._size;
      if (!(this._bits[Math.floor(idx / 8)] & (1 << (idx % 8)))) {
        return false; // Definitely not present
      }
    }
    return true; // Maybe present
  }

  /**
   * Get filter statistics.
   */
  stats() {
    let bitsSet = 0;
    for (let i = 0; i < this._size; i++) {
      if (this._bits[Math.floor(i / 8)] & (1 << (i % 8))) bitsSet++;
    }
    const fillRatio = bitsSet / this._size;
    // Estimated false positive rate: (bitsSet/size)^numHashes
    const estimatedFPR = Math.pow(fillRatio, this._numHashes);
    
    return {
      size: this._size,
      numHashes: this._numHashes,
      itemCount: this._count,
      bitsSet,
      fillRatio: Math.round(fillRatio * 1000) / 1000,
      estimatedFPR: Math.round(estimatedFPR * 10000) / 10000,
    };
  }

  /**
   * Generate k hash values for a given input.
   * Uses double hashing: h(i) = h1 + i*h2
   */
  _getHashes(value) {
    const str = String(value);
    const h1 = this._hash1(str);
    const h2 = this._hash2(str);
    
    const hashes = [];
    for (let i = 0; i < this._numHashes; i++) {
      hashes.push(Math.abs((h1 + i * h2) % this._size));
    }
    return hashes;
  }

  /**
   * FNV-1a hash (fast, good distribution).
   */
  _hash1(str) {
    let hash = 0x811c9dc5; // FNV offset basis
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193); // FNV prime
    }
    return hash >>> 0;
  }

  /**
   * DJB2 hash (simple, effective).
   */
  _hash2(str) {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) + hash) + str.charCodeAt(i);
    }
    return hash >>> 0;
  }
}
