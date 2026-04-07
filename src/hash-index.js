// hash-index.js — Hash index for O(1) equality lookups
// Uses chained hashing with dynamic resizing (extendible hashing).

/**
 * Hash Index: O(1) average-case equality lookups.
 * Uses chaining for collision resolution and doubles on load factor > 0.75.
 */
export class HashIndex {
  constructor(initialBuckets = 16) {
    this._numBuckets = initialBuckets;
    this._buckets = new Array(initialBuckets).fill(null).map(() => []);
    this._size = 0;
    this._loadFactorThreshold = 0.75;
  }

  /**
   * Insert a key-value pair.
   */
  insert(key, value) {
    const idx = this._hash(key) % this._numBuckets;
    this._buckets[idx].push({ key, value });
    this._size++;
    
    // Resize if load factor exceeded
    if (this._size / this._numBuckets > this._loadFactorThreshold) {
      this._resize();
    }
  }

  /**
   * Find all values matching a key. O(1) average case.
   */
  find(key) {
    const idx = this._hash(key) % this._numBuckets;
    return this._buckets[idx]
      .filter(entry => entry.key === key)
      .map(entry => entry.value);
  }

  /**
   * Delete all entries with a given key.
   */
  delete(key) {
    const idx = this._hash(key) % this._numBuckets;
    const before = this._buckets[idx].length;
    this._buckets[idx] = this._buckets[idx].filter(entry => entry.key !== key);
    this._size -= (before - this._buckets[idx].length);
  }

  /**
   * Check if key exists. O(1) average.
   */
  has(key) {
    const idx = this._hash(key) % this._numBuckets;
    return this._buckets[idx].some(entry => entry.key === key);
  }

  get size() { return this._size; }

  stats() {
    let maxChain = 0;
    let emptyBuckets = 0;
    for (const bucket of this._buckets) {
      if (bucket.length === 0) emptyBuckets++;
      if (bucket.length > maxChain) maxChain = bucket.length;
    }
    return {
      size: this._size,
      buckets: this._numBuckets,
      loadFactor: Math.round(this._size / this._numBuckets * 1000) / 1000,
      maxChainLength: maxChain,
      emptyBuckets,
    };
  }

  /**
   * Double the number of buckets and rehash everything.
   */
  _resize() {
    const oldBuckets = this._buckets;
    this._numBuckets *= 2;
    this._buckets = new Array(this._numBuckets).fill(null).map(() => []);
    this._size = 0;
    
    for (const bucket of oldBuckets) {
      for (const { key, value } of bucket) {
        this.insert(key, value);
      }
    }
  }

  /**
   * FNV-1a hash function.
   */
  _hash(key) {
    const str = String(key);
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }
}
