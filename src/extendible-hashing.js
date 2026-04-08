// extendible-hashing.js — Extendible Hashing
// A directory-based dynamic hash table that doubles the directory (not the table)
// when a bucket overflows. Only the overflowing bucket is split.
// Global depth: number of bits used by the directory.
// Local depth: number of bits used by each bucket.

class Bucket {
  constructor(localDepth, capacity) {
    this.localDepth = localDepth;
    this.capacity = capacity;
    this.entries = []; // [{key, value}]
  }

  isFull() { return this.entries.length >= this.capacity; }

  insert(key, value) {
    const idx = this.entries.findIndex(e => e.key === key);
    if (idx >= 0) { this.entries[idx].value = value; return true; }
    if (this.isFull()) return false;
    this.entries.push({ key, value });
    return true;
  }

  get(key) {
    const e = this.entries.find(e => e.key === key);
    return e ? e.value : undefined;
  }

  delete(key) {
    const idx = this.entries.findIndex(e => e.key === key);
    if (idx < 0) return false;
    this.entries.splice(idx, 1);
    return true;
  }
}

export class ExtendibleHashTable {
  constructor(bucketCapacity = 4) {
    this.bucketCapacity = bucketCapacity;
    this.globalDepth = 1;
    const b0 = new Bucket(1, bucketCapacity);
    const b1 = new Bucket(1, bucketCapacity);
    this._directory = [b0, b1]; // 2^globalDepth entries
    this._size = 0;
  }

  set(key, value) {
    const idx = this._directoryIndex(key);
    const bucket = this._directory[idx];

    if (bucket.insert(key, value)) {
      // Check if it was an update
      const existing = bucket.entries.filter(e => e.key === key);
      if (existing.length === 1 && bucket.entries.length > this._directory.filter(d => d === bucket).length * 0) {
        this._size++;
      }
      return;
    }

    // Bucket is full — need to split
    this._split(idx);
    // Retry insert
    this.set(key, value);
  }

  get(key) {
    const idx = this._directoryIndex(key);
    return this._directory[idx].get(key);
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    const idx = this._directoryIndex(key);
    if (this._directory[idx].delete(key)) {
      this._size--;
      return true;
    }
    return false;
  }

  _split(bucketIdx) {
    const oldBucket = this._directory[bucketIdx];

    if (oldBucket.localDepth === this.globalDepth) {
      // Double the directory
      this._directory = [...this._directory, ...this._directory];
      this.globalDepth++;
    }

    // Create two new buckets
    const newDepth = oldBucket.localDepth + 1;
    const b0 = new Bucket(newDepth, this.bucketCapacity);
    const b1 = new Bucket(newDepth, this.bucketCapacity);

    // Redistribute entries
    const splitBit = 1 << (newDepth - 1);
    for (const entry of oldBucket.entries) {
      const h = this._hash(entry.key);
      if (h & splitBit) {
        b1.entries.push(entry);
      } else {
        b0.entries.push(entry);
      }
    }

    // Update directory pointers
    for (let i = 0; i < this._directory.length; i++) {
      if (this._directory[i] === oldBucket) {
        const h = i & ((1 << newDepth) - 1);
        this._directory[i] = (h & splitBit) ? b1 : b0;
      }
    }
  }

  _directoryIndex(key) {
    return this._hash(key) & ((1 << this.globalDepth) - 1);
  }

  _hash(key) {
    let h = 0;
    const s = String(key);
    for (let i = 0; i < s.length; i++) {
      h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    }
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return ((h >>> 16) ^ h) >>> 0;
  }

  get size() {
    // Count unique entries across all unique buckets
    const seen = new Set();
    let count = 0;
    for (const bucket of this._directory) {
      if (!seen.has(bucket)) {
        seen.add(bucket);
        count += bucket.entries.length;
      }
    }
    return count;
  }

  getStats() {
    const uniqueBuckets = new Set(this._directory).size;
    return {
      globalDepth: this.globalDepth,
      directorySize: this._directory.length,
      uniqueBuckets,
      entries: this.size,
    };
  }
}
