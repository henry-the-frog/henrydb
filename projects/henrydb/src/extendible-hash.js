// extendible-hash.js — Extendible Hash Table for HenryDB
//
// An extendible hash table uses a directory that doubles in size when needed,
// but only splits the overflowing bucket — not the entire table.
//
// Key concepts:
//   - Global depth: number of bits from hash used to index the directory
//   - Local depth: number of bits used by a specific bucket
//   - When a bucket overflows and local_depth == global_depth: double directory
//   - When a bucket overflows and local_depth < global_depth: split bucket only
//
// This provides O(1) amortized lookups and inserts, with gradual growth.
// Unlike a B+tree, it cannot do range scans — equality lookups only.

/**
 * Hash function: FNV-1a 32-bit for strings and numbers.
 */
function fnv1a(key) {
  const str = typeof key === 'string' ? key : String(key);
  let hash = 0x811c9dc5; // FNV offset basis
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0; // FNV prime, keep 32-bit unsigned
  }
  return hash;
}

/**
 * Bucket — holds key-value pairs up to a maximum capacity.
 */
class Bucket {
  constructor(localDepth, maxSize = 8) {
    this.localDepth = localDepth;
    this.maxSize = maxSize;
    this.entries = []; // Array of {key, value}
  }

  isFull() {
    return this.entries.length >= this.maxSize;
  }

  find(key) {
    for (const entry of this.entries) {
      if (entry.key === key) return entry.value;
    }
    return undefined;
  }

  insert(key, value) {
    // Update if exists
    for (const entry of this.entries) {
      if (entry.key === key) {
        entry.value = value;
        return false; // Not a new entry
      }
    }
    this.entries.push({ key, value });
    return true; // New entry
  }

  remove(key) {
    const idx = this.entries.findIndex(e => e.key === key);
    if (idx === -1) return false;
    this.entries.splice(idx, 1);
    return true;
  }
}

/**
 * ExtendibleHashTable — Dynamic hash table with bucket splitting.
 */
export class ExtendibleHashTable {
  /**
   * @param {number} bucketSize - Maximum entries per bucket (default: 8)
   * @param {number} initialDepth - Initial global depth (default: 1)
   */
  constructor(bucketSize = 8, initialDepth = 1) {
    this.bucketSize = bucketSize;
    this.globalDepth = initialDepth;
    this._size = 0;
    this._splitCount = 0;
    this._directoryGrowths = 0;
    
    // Initialize directory with 2^globalDepth entries
    const numSlots = 1 << this.globalDepth;
    this.directory = new Array(numSlots);
    
    // Each slot gets its own bucket initially
    for (let i = 0; i < numSlots; i++) {
      this.directory[i] = new Bucket(this.globalDepth, this.bucketSize);
    }
  }

  /**
   * Number of key-value pairs in the table.
   */
  get size() { return this._size; }

  /**
   * Number of directory slots (2^globalDepth).
   */
  get numSlots() { return this.directory.length; }

  /**
   * Number of unique buckets (may be less than directory slots due to sharing).
   */
  get numBuckets() {
    const seen = new Set();
    for (const bucket of this.directory) seen.add(bucket);
    return seen.size;
  }

  /**
   * Look up a value by key. O(1) amortized.
   */
  get(key) {
    const bucket = this._getBucket(key);
    return bucket.find(key);
  }

  /**
   * Insert a key-value pair. O(1) amortized.
   * If the key already exists, updates the value.
   */
  insert(key, value) {
    const hash = fnv1a(key);
    let bucketIdx = this._directoryIndex(hash);
    let bucket = this.directory[bucketIdx];
    
    // Try to insert
    if (!bucket.isFull()) {
      if (bucket.insert(key, value)) this._size++;
      return;
    }
    
    // Check if key already exists (update, no overflow)
    if (bucket.find(key) !== undefined) {
      bucket.insert(key, value); // Update existing
      return;
    }
    
    // Bucket is full — need to split
    this._split(bucketIdx, hash);
    
    // Retry insert after split
    bucketIdx = this._directoryIndex(hash);
    bucket = this.directory[bucketIdx];
    if (bucket.insert(key, value)) this._size++;
  }

  /**
   * Remove a key. O(1) amortized.
   * Returns true if the key was found and removed.
   */
  remove(key) {
    const bucket = this._getBucket(key);
    if (bucket.remove(key)) {
      this._size--;
      return true;
    }
    return false;
  }

  /**
   * Iterate all entries (unordered).
   */
  *entries() {
    const visited = new Set();
    for (const bucket of this.directory) {
      if (visited.has(bucket)) continue;
      visited.add(bucket);
      for (const entry of bucket.entries) {
        yield entry;
      }
    }
  }

  /**
   * Get all values for a given key (for non-unique indexes, returns array).
   */
  getAll(key) {
    const bucket = this._getBucket(key);
    return bucket.entries
      .filter(e => e.key === key)
      .map(e => e.value);
  }

  /**
   * Get statistics.
   */
  getStats() {
    const bucketSizes = [];
    const visited = new Set();
    for (const bucket of this.directory) {
      if (visited.has(bucket)) continue;
      visited.add(bucket);
      bucketSizes.push(bucket.entries.length);
    }
    
    return {
      size: this._size,
      globalDepth: this.globalDepth,
      directorySlots: this.numSlots,
      uniqueBuckets: this.numBuckets,
      bucketSize: this.bucketSize,
      loadFactor: this._size / (this.numBuckets * this.bucketSize),
      avgBucketFill: bucketSizes.reduce((a, b) => a + b, 0) / bucketSizes.length,
      maxBucketFill: Math.max(...bucketSizes),
      splits: this._splitCount,
      directoryGrowths: this._directoryGrowths,
    };
  }

  // --- Internal ---

  _getBucket(key) {
    const hash = fnv1a(key);
    const idx = this._directoryIndex(hash);
    return this.directory[idx];
  }

  _directoryIndex(hash) {
    // Use the last `globalDepth` bits of the hash
    return hash & ((1 << this.globalDepth) - 1);
  }

  _split(bucketIdx, hash) {
    const bucket = this.directory[bucketIdx];
    this._splitCount++;
    
    if (bucket.localDepth === this.globalDepth) {
      // Need to double the directory
      this._growDirectory();
    }
    
    // Split the bucket
    const oldDepth = bucket.localDepth;
    const newDepth = oldDepth + 1;
    
    const bucket0 = new Bucket(newDepth, this.bucketSize);
    const bucket1 = new Bucket(newDepth, this.bucketSize);
    
    // Redistribute entries based on the new bit
    const splitBit = 1 << oldDepth;
    for (const entry of bucket.entries) {
      const h = fnv1a(entry.key);
      if (h & splitBit) {
        bucket1.entries.push(entry);
      } else {
        bucket0.entries.push(entry);
      }
    }
    
    // Update directory pointers
    // All slots that pointed to the old bucket need to be updated
    for (let i = 0; i < this.directory.length; i++) {
      if (this.directory[i] === bucket) {
        if (i & splitBit) {
          this.directory[i] = bucket1;
        } else {
          this.directory[i] = bucket0;
        }
      }
    }
    
    // If one of the new buckets is still full (hash collisions),
    // we may need to split again — but that's handled by the caller retrying
  }

  _growDirectory() {
    this._directoryGrowths++;
    const oldSize = this.directory.length;
    const newDir = new Array(oldSize * 2);
    
    // Copy: slot i maps to same bucket, slot i + oldSize also maps to same bucket
    for (let i = 0; i < oldSize; i++) {
      newDir[i] = this.directory[i];
      newDir[i + oldSize] = this.directory[i];
    }
    
    this.directory = newDir;
    this.globalDepth++;
  }
}
