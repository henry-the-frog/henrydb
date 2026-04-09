// sstable.js — Sorted String Table (SSTable)
// Immutable sorted key-value file format used by LSM-trees (LevelDB, RocksDB).
// Features: block-based storage, binary search on index, optional bloom filter.

import { BloomFilter } from './bloom-filter.js';

/**
 * SSTable — In-memory simulation of an immutable sorted string table.
 * In production this would be file-backed; here we demonstrate the format.
 */
export class SSTable {
  /**
   * Build an SSTable from sorted entries.
   * @param {Array<{key: *, value: *}>} entries - MUST be sorted by key
   * @param {Object} opts
   * @param {number} opts.blockSize - Entries per block (default 64)
   * @param {boolean} opts.useBloomFilter - Enable bloom filter (default true)
   */
  constructor(entries, opts = {}) {
    const blockSize = opts.blockSize || 64;
    const useBloom = opts.useBloomFilter !== false;

    // Build blocks
    this._blocks = [];
    this._index = []; // Sparse index: first key of each block
    
    for (let i = 0; i < entries.length; i += blockSize) {
      const block = entries.slice(i, i + blockSize);
      this._blocks.push(block);
      this._index.push({ key: block[0].key, blockIdx: this._blocks.length - 1 });
    }

    // Build bloom filter
    this._bloom = null;
    if (useBloom && entries.length > 0) {
      this._bloom = new BloomFilter(entries.length, 0.01);
      for (const e of entries) this._bloom.add(String(e.key));
    }

    this._size = entries.length;
    this._minKey = entries.length > 0 ? entries[0].key : null;
    this._maxKey = entries.length > 0 ? entries[entries.length - 1].key : null;
  }

  get size() { return this._size; }
  get minKey() { return this._minKey; }
  get maxKey() { return this._maxKey; }
  get blockCount() { return this._blocks.length; }

  /**
   * Point lookup. O(log B + B_size) where B = number of blocks.
   */
  get(key) {
    if (this._bloom && !this._bloom.mightContain(String(key))) return undefined;
    
    const blockIdx = this._findBlock(key);
    if (blockIdx < 0) return undefined;
    
    const block = this._blocks[blockIdx];
    // Binary search within block
    let lo = 0, hi = block.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (block[mid].key === key) return block[mid].value;
      if (block[mid].key < key) lo = mid + 1;
      else hi = mid - 1;
    }
    return undefined;
  }

  /**
   * Range scan [startKey, endKey]. Returns sorted entries.
   */
  range(startKey, endKey) {
    const results = [];
    const startBlock = Math.max(0, this._findBlock(startKey));
    
    for (let b = startBlock; b < this._blocks.length; b++) {
      const block = this._blocks[b];
      if (block[0].key > endKey) break;
      
      for (const entry of block) {
        if (entry.key >= startKey && entry.key <= endKey) {
          results.push(entry);
        }
        if (entry.key > endKey) break;
      }
    }
    return results;
  }

  /**
   * Iterator over all entries.
   */
  *[Symbol.iterator]() {
    for (const block of this._blocks) {
      for (const entry of block) yield entry;
    }
  }

  /**
   * Merge two SSTables into a new one (used in compaction).
   */
  static merge(a, b, opts) {
    const merged = [];
    const itA = a[Symbol.iterator]();
    const itB = b[Symbol.iterator]();
    let eA = itA.next();
    let eB = itB.next();

    while (!eA.done && !eB.done) {
      if (eA.value.key <= eB.value.key) {
        if (eA.value.key === eB.value.key) eB = itB.next(); // b overwrites a
        merged.push(eA.value);
        eA = itA.next();
      } else {
        merged.push(eB.value);
        eB = itB.next();
      }
    }
    while (!eA.done) { merged.push(eA.value); eA = itA.next(); }
    while (!eB.done) { merged.push(eB.value); eB = itB.next(); }

    return new SSTable(merged, opts);
  }

  _findBlock(key) {
    // Binary search on sparse index
    let lo = 0, hi = this._index.length - 1;
    let result = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this._index[mid].key <= key) {
        result = this._index[mid].blockIdx;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return result;
  }

  getStats() {
    return {
      entries: this._size,
      blocks: this._blocks.length,
      minKey: this._minKey,
      maxKey: this._maxKey,
      hasBloomFilter: !!this._bloom,
    };
  }
}
