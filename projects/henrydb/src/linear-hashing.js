// linear-hashing.js — Linear Hashing
// No directory — buckets are split in round-robin order.
// Uses a split pointer that advances linearly through buckets.
// When load factor exceeds threshold, split the bucket at the pointer.
// Two hash functions: h0 (current) and h1 (next level).

export class LinearHashTable {
  constructor(initialBuckets = 4, bucketCapacity = 8, loadThreshold = 0.75) {
    this.bucketCapacity = bucketCapacity;
    this.loadThreshold = loadThreshold;
    this._buckets = Array.from({ length: initialBuckets }, () => []);
    this._splitPointer = 0;
    this._level = 0;
    this._initialBuckets = initialBuckets;
    this._size = 0;
  }

  set(key, value) {
    const idx = this._bucketIndex(key);
    const bucket = this._buckets[idx];
    
    const existing = bucket.findIndex(e => e.key === key);
    if (existing >= 0) {
      bucket[existing].value = value;
      return;
    }

    bucket.push({ key, value });
    this._size++;

    if (this._loadFactor() > this.loadThreshold) this._split();
  }

  get(key) {
    const idx = this._bucketIndex(key);
    const entry = this._buckets[idx].find(e => e.key === key);
    return entry ? entry.value : undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    const idx = this._bucketIndex(key);
    const bucket = this._buckets[idx];
    const pos = bucket.findIndex(e => e.key === key);
    if (pos < 0) return false;
    bucket.splice(pos, 1);
    this._size--;
    return true;
  }

  _bucketIndex(key) {
    const h = this._hash(key);
    const n = this._initialBuckets * (1 << this._level);
    let idx = h % n;
    if (idx < this._splitPointer) {
      idx = h % (n * 2);
    }
    return idx;
  }

  _split() {
    const n = this._initialBuckets * (1 << this._level);
    const oldBucket = this._buckets[this._splitPointer];
    const newBucket = [];
    this._buckets.push(newBucket);

    // Redistribute entries
    const remaining = [];
    const newN = n * 2;
    for (const entry of oldBucket) {
      const h = this._hash(entry.key);
      if (h % newN === this._splitPointer) {
        remaining.push(entry);
      } else {
        newBucket.push(entry);
      }
    }
    this._buckets[this._splitPointer] = remaining;

    this._splitPointer++;
    if (this._splitPointer >= n) {
      this._splitPointer = 0;
      this._level++;
    }
  }

  _loadFactor() {
    return this._size / (this._buckets.length * this.bucketCapacity);
  }

  _hash(key) {
    let h = 0;
    const s = String(key);
    for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return ((h >>> 16) ^ h) >>> 0;
  }

  get size() { return this._size; }

  getStats() {
    const maxChain = Math.max(...this._buckets.map(b => b.length));
    const avgChain = this._size / this._buckets.length;
    return {
      size: this._size,
      buckets: this._buckets.length,
      level: this._level,
      splitPointer: this._splitPointer,
      loadFactor: this._loadFactor().toFixed(3),
      maxChain,
      avgChain: avgChain.toFixed(2),
    };
  }
}
