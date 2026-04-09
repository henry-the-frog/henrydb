// cuckoo-filter.js — Cuckoo filter: Bloom filter alternative with deletion support
//
// A Cuckoo filter stores fingerprints of elements using cuckoo hashing.
// Unlike Bloom filters, it supports deletion without false negatives.
//
// Advantages over Bloom filters:
//   - Supports deletion
//   - Better space efficiency for FPR < 3%
//   - Faster lookups (fewer memory accesses)
//
// Disadvantages:
//   - Insertion can fail if table is too full (load factor ~95%)
//   - Deletion of non-inserted elements can cause false negatives
//
// Based on: "Cuckoo Filter: Practically Better Than Bloom" (Fan et al., 2014)

const MAX_KICKS = 500;

function fingerprint(key, fpBits) {
  let h = 0x811c9dc5;
  const str = typeof key === 'string' ? key : String(key);
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  // Ensure fingerprint is non-zero
  const fp = (h % ((1 << fpBits) - 1)) + 1;
  return fp;
}

function hashIndex(key, numBuckets) {
  let h = 0x01000193;
  const str = typeof key === 'string' ? key : String(key);
  for (let i = 0; i < str.length; i++) {
    h = ((h ^ str.charCodeAt(i)) * 0x5bd1e995) >>> 0;
  }
  return h % numBuckets;
}

function altIndex(index, fp, numBuckets) {
  // i2 = i1 ⊕ hash(fingerprint)
  let h = fp * 0x5bd1e995;
  h = (h >>> 0) % numBuckets;
  return (index ^ h) % numBuckets;
}

/**
 * CuckooFilter — Set membership with deletion support.
 */
export class CuckooFilter {
  /**
   * @param {number} capacity - Number of items the filter should hold
   * @param {number} bucketSize - Entries per bucket (default: 4)
   * @param {number} fpBits - Fingerprint bits (default: 8)
   */
  constructor(capacity = 1024, bucketSize = 4, fpBits = 8) {
    this.bucketSize = bucketSize;
    this.fpBits = fpBits;
    this.numBuckets = Math.max(1, Math.ceil(capacity / bucketSize));
    
    // Ensure numBuckets is power of 2 (for efficient XOR alternate indexing)
    this.numBuckets = 1 << Math.ceil(Math.log2(this.numBuckets));
    
    // Bucket array: each bucket holds up to bucketSize fingerprints
    this._buckets = Array.from({ length: this.numBuckets }, () => new Uint8Array(bucketSize));
    this._count = 0;
  }

  get size() { return this._count; }
  get loadFactor() { return this._count / (this.numBuckets * this.bucketSize); }

  /**
   * Insert an element.
   * Returns true on success, false if the filter is too full.
   */
  insert(key) {
    const fp = fingerprint(key, this.fpBits);
    const i1 = hashIndex(key, this.numBuckets);
    const i2 = altIndex(i1, fp, this.numBuckets);
    
    // Try bucket i1
    if (this._addToBucket(i1, fp)) {
      this._count++;
      return true;
    }
    // Try bucket i2
    if (this._addToBucket(i2, fp)) {
      this._count++;
      return true;
    }
    
    // Both full — relocate existing entries (cuckoo hashing)
    let idx = Math.random() < 0.5 ? i1 : i2;
    let currentFp = fp;
    
    for (let kick = 0; kick < MAX_KICKS; kick++) {
      // Swap with random entry in bucket
      const slot = Math.floor(Math.random() * this.bucketSize);
      const evictedFp = this._buckets[idx][slot];
      this._buckets[idx][slot] = currentFp;
      currentFp = evictedFp;
      
      // Find alternate bucket for evicted entry
      idx = altIndex(idx, currentFp, this.numBuckets);
      if (this._addToBucket(idx, currentFp)) {
        this._count++;
        return true;
      }
    }
    
    return false; // Filter is too full
  }

  /**
   * Check if an element MIGHT be in the filter.
   * Returns false for definite absence, true for possible presence.
   */
  contains(key) {
    const fp = fingerprint(key, this.fpBits);
    const i1 = hashIndex(key, this.numBuckets);
    const i2 = altIndex(i1, fp, this.numBuckets);
    
    return this._bucketContains(i1, fp) || this._bucketContains(i2, fp);
  }

  /**
   * Delete an element.
   * WARNING: Only delete elements you're sure were inserted.
   * Deleting non-inserted elements can cause false negatives.
   */
  delete(key) {
    const fp = fingerprint(key, this.fpBits);
    const i1 = hashIndex(key, this.numBuckets);
    const i2 = altIndex(i1, fp, this.numBuckets);
    
    if (this._removeFromBucket(i1, fp)) {
      this._count--;
      return true;
    }
    if (this._removeFromBucket(i2, fp)) {
      this._count--;
      return true;
    }
    
    return false;
  }

  /**
   * Get statistics.
   */
  getStats() {
    return {
      count: this._count,
      capacity: this.numBuckets * this.bucketSize,
      buckets: this.numBuckets,
      bucketSize: this.bucketSize,
      fpBits: this.fpBits,
      loadFactor: parseFloat(this.loadFactor.toFixed(4)),
      bytesUsed: this.numBuckets * this.bucketSize,
      theoreticalFPR: Math.pow(2, -this.fpBits + 2) * this.bucketSize, // Approximate
    };
  }

  // --- Internal ---

  _addToBucket(idx, fp) {
    const bucket = this._buckets[idx];
    for (let i = 0; i < this.bucketSize; i++) {
      if (bucket[i] === 0) {
        bucket[i] = fp;
        return true;
      }
    }
    return false; // Bucket full
  }

  _bucketContains(idx, fp) {
    const bucket = this._buckets[idx];
    for (let i = 0; i < this.bucketSize; i++) {
      if (bucket[i] === fp) return true;
    }
    return false;
  }

  _removeFromBucket(idx, fp) {
    const bucket = this._buckets[idx];
    for (let i = 0; i < this.bucketSize; i++) {
      if (bucket[i] === fp) {
        bucket[i] = 0;
        return true;
      }
    }
    return false;
  }
}
