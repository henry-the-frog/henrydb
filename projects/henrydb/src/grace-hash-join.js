// grace-hash-join.js — Grace hash join (partitioned hash join)
// When the hash table doesn't fit in memory, partition both inputs by hash(key),
// write partitions to "disk", then join within each partition where the hash table fits.
//
// Named after the Grace database machine (Kitsuregawa et al., 1983).

/**
 * GraceHashJoin — partitioned hash join with bounded memory.
 */
export class GraceHashJoin {
  constructor(options = {}) {
    this.numPartitions = options.numPartitions || 16;
    this.stats = { leftRows: 0, rightRows: 0, matches: 0, partitionTimeMs: 0, joinTimeMs: 0 };
  }

  /**
   * Join two key arrays using Grace hash join.
   */
  join(leftKeys, rightKeys) {
    const t0 = Date.now();
    this.stats.leftRows = leftKeys.length;
    this.stats.rightRows = rightKeys.length;

    // Phase 1: Partition both inputs
    const leftPartitions = this._partition(leftKeys);
    const rightPartitions = this._partition(rightKeys);
    this.stats.partitionTimeMs = Date.now() - t0;

    // Phase 2: Join within each partition
    const t1 = Date.now();
    const leftIndices = [];
    const rightIndices = [];

    for (let p = 0; p < this.numPartitions; p++) {
      const lp = leftPartitions[p];
      const rp = rightPartitions[p];
      if (!lp || !rp || lp.length === 0 || rp.length === 0) continue;

      // Build hash table on right partition
      const ht = new Map();
      for (const { key, idx } of rp) {
        if (!ht.has(key)) ht.set(key, []);
        ht.get(key).push(idx);
      }

      // Probe with left partition
      for (const { key, idx } of lp) {
        const matches = ht.get(key);
        if (matches) {
          for (const rIdx of matches) {
            leftIndices.push(idx);
            rightIndices.push(rIdx);
          }
        }
      }
    }

    this.stats.joinTimeMs = Date.now() - t1;
    this.stats.matches = leftIndices.length;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  _partition(keys) {
    const partitions = Array.from({ length: this.numPartitions }, () => []);
    for (let i = 0; i < keys.length; i++) {
      const h = this._hash(keys[i]) % this.numPartitions;
      partitions[h].push({ key: keys[i], idx: i });
    }
    return partitions;
  }

  _hash(key) {
    let h = key | 0;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return ((h >>> 16) ^ h) >>> 0;
  }

  getStats() { return { ...this.stats }; }
}
