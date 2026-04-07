// bloom-filter.js — Probabilistic membership testing for HenryDB LSM tree
//
// A Bloom filter is a space-efficient probabilistic data structure that tests
// whether an element is a member of a set. False positives are possible,
// but false negatives are not.
//
// Used in LSM trees to skip SSTables that definitely don't contain a key,
// avoiding expensive disk reads. This is critical for read performance
// since LSM trees may need to check many SSTables.

/**
 * BloomFilter — classic implementation with multiple hash functions.
 * 
 * Space: m bits
 * Hash functions: k
 * False positive rate: (1 - e^(-kn/m))^k
 * Optimal k = (m/n) * ln(2) ≈ 0.693 * m/n
 * 
 * @param {number} expectedItems - Expected number of items
 * @param {number} falsePositiveRate - Desired false positive rate (0-1)
 */
export class BloomFilter {
  constructor(expectedItems = 1000, falsePositiveRate = 0.01) {
    // Calculate optimal parameters
    // m = -(n * ln(p)) / (ln(2))^2
    const n = Math.max(1, expectedItems);
    const p = Math.max(0.0001, Math.min(0.5, falsePositiveRate));
    
    this._numBits = Math.ceil(-(n * Math.log(p)) / (Math.LN2 * Math.LN2));
    this._numBits = Math.max(64, this._numBits); // Minimum 64 bits
    
    // k = (m/n) * ln(2)
    this._numHashes = Math.max(1, Math.round((this._numBits / n) * Math.LN2));
    this._numHashes = Math.min(32, this._numHashes); // Cap at 32
    
    // Bit array (using Uint32Array for efficient bit manipulation)
    this._bits = new Uint32Array(Math.ceil(this._numBits / 32));
    this._count = 0;
    
    // Random seeds for hash functions (deterministic for reproducibility)
    this._seeds = Array.from({ length: this._numHashes }, (_, i) => 
      0x9e3779b9 + i * 0x517cc1b7
    );
  }

  /**
   * Add a key to the filter.
   */
  add(key) {
    const keyStr = String(key);
    for (let i = 0; i < this._numHashes; i++) {
      const bit = this._hash(keyStr, this._seeds[i]) % this._numBits;
      this._setBit(bit);
    }
    this._count++;
  }

  /**
   * Test if a key might be in the set.
   * Returns true if the key MIGHT be present (possible false positive).
   * Returns false if the key is DEFINITELY NOT present.
   */
  mightContain(key) {
    const keyStr = String(key);
    for (let i = 0; i < this._numHashes; i++) {
      const bit = this._hash(keyStr, this._seeds[i]) % this._numBits;
      if (!this._getBit(bit)) return false;
    }
    return true;
  }

  /**
   * Get the current estimated false positive rate.
   */
  get falsePositiveRate() {
    const setBits = this._countSetBits();
    const p = Math.pow(setBits / this._numBits, this._numHashes);
    return p;
  }

  /**
   * Get filter statistics.
   */
  get stats() {
    return {
      bits: this._numBits,
      hashes: this._numHashes,
      items: this._count,
      setBits: this._countSetBits(),
      fillRatio: this._countSetBits() / this._numBits,
      estimatedFPR: this.falsePositiveRate,
      bytesUsed: this._bits.byteLength
    };
  }

  // --- Internal ---

  _hash(key, seed) {
    // MurmurHash3-like mixing
    let h = seed;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      h = Math.imul(h, 0x5bd1e995);
      h ^= h >>> 15;
    }
    h ^= key.length;
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 13;
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 16;
    return h >>> 0; // Ensure unsigned
  }

  _setBit(pos) {
    const idx = pos >>> 5;     // Divide by 32
    const bit = pos & 0x1f;    // Mod 32
    this._bits[idx] |= (1 << bit);
  }

  _getBit(pos) {
    const idx = pos >>> 5;
    const bit = pos & 0x1f;
    return (this._bits[idx] & (1 << bit)) !== 0;
  }

  _countSetBits() {
    let count = 0;
    for (const word of this._bits) {
      // Hamming weight (popcount)
      let v = word;
      v = v - ((v >>> 1) & 0x55555555);
      v = (v & 0x33333333) + ((v >>> 2) & 0x33333333);
      count += (((v + (v >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
    }
    return count;
  }

  /**
   * Serialize the Bloom filter to a Buffer for disk storage.
   */
  serialize() {
    const header = Buffer.alloc(16);
    header.writeUInt32LE(this._numBits, 0);
    header.writeUInt32LE(this._numHashes, 4);
    header.writeUInt32LE(this._count, 8);
    header.writeUInt32LE(this._bits.length, 12);
    
    const data = Buffer.from(this._bits.buffer);
    return Buffer.concat([header, data]);
  }

  /**
   * Deserialize a Bloom filter from a Buffer.
   */
  static deserialize(buf) {
    const numBits = buf.readUInt32LE(0);
    const numHashes = buf.readUInt32LE(4);
    const count = buf.readUInt32LE(8);
    const arrLen = buf.readUInt32LE(12);
    
    const filter = new BloomFilter(count || 1);
    filter._numBits = numBits;
    filter._numHashes = numHashes;
    filter._count = count;
    filter._seeds = Array.from({ length: numHashes }, (_, i) => 
      0x9e3779b9 + i * 0x517cc1b7
    );
    
    // Copy bit array
    filter._bits = new Uint32Array(arrLen);
    for (let i = 0; i < arrLen; i++) {
      filter._bits[i] = buf.readUInt32LE(16 + i * 4);
    }
    
    return filter;
  }
}

/**
 * Counting Bloom Filter — supports deletion.
 * Uses counters instead of single bits.
 * Trade-off: 4x more space but supports remove().
 */
export class CountingBloomFilter {
  constructor(expectedItems = 1000, falsePositiveRate = 0.01) {
    const n = Math.max(1, expectedItems);
    const p = Math.max(0.0001, Math.min(0.5, falsePositiveRate));
    
    this._numSlots = Math.ceil(-(n * Math.log(p)) / (Math.LN2 * Math.LN2));
    this._numSlots = Math.max(64, this._numSlots);
    this._numHashes = Math.max(1, Math.round((this._numSlots / n) * Math.LN2));
    this._numHashes = Math.min(32, this._numHashes);
    
    // 4-bit counters (max value 15) — use Uint8Array
    this._counters = new Uint8Array(this._numSlots);
    this._count = 0;
    
    this._seeds = Array.from({ length: this._numHashes }, (_, i) => 
      0x9e3779b9 + i * 0x517cc1b7
    );
  }

  add(key) {
    const keyStr = String(key);
    for (let i = 0; i < this._numHashes; i++) {
      const slot = this._hash(keyStr, this._seeds[i]) % this._numSlots;
      if (this._counters[slot] < 255) this._counters[slot]++;
    }
    this._count++;
  }

  remove(key) {
    const keyStr = String(key);
    // First check if it might be present
    if (!this.mightContain(key)) return false;
    
    for (let i = 0; i < this._numHashes; i++) {
      const slot = this._hash(keyStr, this._seeds[i]) % this._numSlots;
      if (this._counters[slot] > 0) this._counters[slot]--;
    }
    this._count--;
    return true;
  }

  mightContain(key) {
    const keyStr = String(key);
    for (let i = 0; i < this._numHashes; i++) {
      const slot = this._hash(keyStr, this._seeds[i]) % this._numSlots;
      if (this._counters[slot] === 0) return false;
    }
    return true;
  }

  _hash(key, seed) {
    let h = seed;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      h = Math.imul(h, 0x5bd1e995);
      h ^= h >>> 15;
    }
    h ^= key.length;
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 13;
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 16;
    return h >>> 0;
  }
}
