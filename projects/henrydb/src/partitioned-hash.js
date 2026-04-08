// partitioned-hash.js — Concurrent-friendly partitioned hash table
// Segments the hash space into independent partitions to reduce contention.

function fnv1a(key) {
  let h = 0x811c9dc5;
  const s = String(key);
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
}

export class PartitionedHashTable {
  constructor(numPartitions = 16, initialBuckets = 64) {
    this.numPartitions = numPartitions;
    this.partitions = Array.from({ length: numPartitions }, () => ({
      buckets: new Array(initialBuckets).fill(null).map(() => []),
      size: 0,
    }));
    this._totalSize = 0;
  }

  _partition(key) { return fnv1a(key) % this.numPartitions; }
  _bucket(key, part) { return fnv1a(key + '_b') % part.buckets.length; }

  set(key, value) {
    const partIdx = this._partition(key);
    const part = this.partitions[partIdx];
    const bucketIdx = this._bucket(key, part);
    const bucket = part.buckets[bucketIdx];
    
    for (const entry of bucket) {
      if (entry.key === key) { entry.value = value; return; }
    }
    
    bucket.push({ key, value });
    part.size++;
    this._totalSize++;
    
    // Resize partition if load factor > 0.75
    if (part.size / part.buckets.length > 0.75) this._resizePartition(partIdx);
  }

  get(key) {
    const partIdx = this._partition(key);
    const part = this.partitions[partIdx];
    const bucketIdx = this._bucket(key, part);
    
    for (const entry of part.buckets[bucketIdx]) {
      if (entry.key === key) return entry.value;
    }
    return undefined;
  }

  delete(key) {
    const partIdx = this._partition(key);
    const part = this.partitions[partIdx];
    const bucketIdx = this._bucket(key, part);
    const bucket = part.buckets[bucketIdx];
    
    const idx = bucket.findIndex(e => e.key === key);
    if (idx === -1) return false;
    bucket.splice(idx, 1);
    part.size--;
    this._totalSize--;
    return true;
  }

  _resizePartition(partIdx) {
    const part = this.partitions[partIdx];
    const newBuckets = new Array(part.buckets.length * 2).fill(null).map(() => []);
    const oldPart = { buckets: newBuckets, size: part.size };
    
    for (const bucket of part.buckets) {
      for (const entry of bucket) {
        const newIdx = this._bucket(entry.key, oldPart);
        newBuckets[newIdx].push(entry);
      }
    }
    part.buckets = newBuckets;
  }

  /** Get partition-level stats */
  stats() {
    return this.partitions.map((p, i) => ({
      partition: i,
      size: p.size,
      buckets: p.buckets.length,
      loadFactor: (p.size / p.buckets.length).toFixed(2),
    }));
  }

  get size() { return this._totalSize; }
}
