// radix-join.js — Radix-partitioned hash join for cache-conscious execution
// Standard hash joins have poor cache behavior on large tables because the
// hash table exceeds L2 cache. Radix partitioning splits both inputs into
// partitions that fit in cache, then joins within partitions.
//
// Based on: Manegold et al., "Optimizing Main-Memory Join on Modern Hardware" (2000)

import { TypedColumn } from './typed-columns.js';

const DEFAULT_RADIX_BITS = 8; // 256 partitions
const L2_CACHE_SIZE = 256 * 1024; // 256KB typical L2

/**
 * Radix-partitioned hash join on TypedArray columns.
 */
export class RadixJoin {
  constructor(options = {}) {
    this.radixBits = options.radixBits || DEFAULT_RADIX_BITS;
    this.numPartitions = 1 << this.radixBits;
    this.mask = this.numPartitions - 1;
    this.stats = { partitionTimeMs: 0, joinTimeMs: 0, totalMatches: 0, partitionsUsed: 0 };
  }

  /**
   * Join two relations on integer key columns.
   * Returns { left: Uint32Array, right: Uint32Array } — matching index pairs.
   */
  join(leftKey, rightKey) {
    const startMs = Date.now();

    // Phase 1: Partition both sides by radix of key
    const t0 = Date.now();
    const leftPartitions = this._partition(leftKey);
    const rightPartitions = this._partition(rightKey);
    this.stats.partitionTimeMs = Date.now() - t0;

    // Phase 2: Join within each partition
    const t1 = Date.now();
    const leftMatches = [];
    const rightMatches = [];

    for (let p = 0; p < this.numPartitions; p++) {
      const lp = leftPartitions[p];
      const rp = rightPartitions[p];
      if (!lp || !rp || lp.length === 0 || rp.length === 0) continue;
      
      this.stats.partitionsUsed++;

      // Build hash table on smaller side (within partition — fits in cache)
      const buildSide = lp.length <= rp.length ? lp : rp;
      const probeSide = lp.length <= rp.length ? rp : lp;
      const isLeftBuild = lp.length <= rp.length;

      const ht = new Map();
      for (const { key, idx } of buildSide) {
        if (!ht.has(key)) ht.set(key, []);
        ht.get(key).push(idx);
      }

      // Probe
      for (const { key, idx } of probeSide) {
        const matches = ht.get(key);
        if (matches) {
          for (const matchIdx of matches) {
            if (isLeftBuild) {
              leftMatches.push(matchIdx);
              rightMatches.push(idx);
            } else {
              leftMatches.push(idx);
              rightMatches.push(matchIdx);
            }
          }
        }
      }
    }

    this.stats.joinTimeMs = Date.now() - t1;
    this.stats.totalMatches = leftMatches.length;

    return {
      left: new Uint32Array(leftMatches),
      right: new Uint32Array(rightMatches),
    };
  }

  /**
   * Partition a TypedColumn by radix hash of values.
   * Returns array of partitions, each containing {key, idx} pairs.
   */
  _partition(col) {
    const arr = col.toArray();
    const len = col.length;
    const partitions = new Array(this.numPartitions);

    // Count pass: determine partition sizes
    const counts = new Uint32Array(this.numPartitions);
    for (let i = 0; i < len; i++) {
      counts[this._hash(arr[i])]++;
    }

    // Allocate partitions
    for (let p = 0; p < this.numPartitions; p++) {
      if (counts[p] > 0) {
        partitions[p] = new Array(counts[p]);
        counts[p] = 0; // Reset for fill pass
      }
    }

    // Fill pass: distribute elements to partitions
    for (let i = 0; i < len; i++) {
      const key = arr[i];
      const p = this._hash(key);
      partitions[p][counts[p]++] = { key, idx: i };
    }

    return partitions;
  }

  /**
   * Radix hash: extract low bits as partition number.
   * For integer keys, this is fast and gives good distribution.
   */
  _hash(key) {
    // Mix the bits slightly for better distribution
    let h = key | 0;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = (h >>> 16) ^ h;
    return h & this.mask;
  }

  getStats() {
    return { ...this.stats };
  }
}
