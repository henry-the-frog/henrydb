// lsm-tree.js — Log-Structured Merge Tree for HenryDB
//
// An LSM-tree optimizes writes by batching them in an in-memory buffer
// (memtable) and periodically flushing to sorted, immutable on-disk
// runs (SSTables). Reads merge results from all levels.
//
// Architecture:
//   Level 0: Active memtable (SkipList) — all writes go here
//   Level 0': Immutable memtable (being flushed to disk)
//   Level 1..N: Sorted runs (SSTables) — immutable, merged during compaction
//
// Write path: memtable.put(key, value) → when full, flush to Level 1
// Read path: check memtable → check immutable → check L1 → check L2 → ...
// Delete: write a tombstone marker
//
// Used by: LevelDB, RocksDB, Cassandra, HBase, CockroachDB

import { SkipList } from './skip-list.js';

const TOMBSTONE = Symbol('TOMBSTONE');

/**
 * SSTable — Sorted String Table (immutable sorted run).
 * In a real implementation, this would be disk-backed.
 * Here we use an in-memory sorted array.
 */
class SSTable {
  constructor(entries, level = 1) {
    // entries: Array of {key, value} sorted by key
    this.entries = entries;
    this.level = level;
    this.minKey = entries.length > 0 ? entries[0].key : null;
    this.maxKey = entries.length > 0 ? entries[entries.length - 1].key : null;
    this.size = entries.length;
    this.createdAt = Date.now();
  }

  /**
   * Binary search for a key.
   */
  get(key) {
    let lo = 0, hi = this.entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const cmp = this._compare(this.entries[mid].key, key);
      if (cmp === 0) return this.entries[mid].value;
      if (cmp < 0) lo = mid + 1;
      else hi = mid - 1;
    }
    return undefined;
  }

  /**
   * Range scan.
   */
  *range(low, high) {
    // Binary search for start position
    let lo = 0, hi = this.entries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._compare(this.entries[mid].key, low) < 0) lo = mid + 1;
      else hi = mid;
    }
    
    for (let i = lo; i < this.entries.length; i++) {
      if (this._compare(this.entries[i].key, high) > 0) break;
      yield this.entries[i];
    }
  }

  _compare(a, b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }
}

/**
 * LSMTree — Log-Structured Merge Tree.
 */
export class LSMTree {
  /**
   * @param {Object} options
   * @param {number} options.memtableSize - Max entries in memtable before flush (default: 1000)
   * @param {number} options.compactionThreshold - SSTables per level before compaction (default: 4)
   */
  constructor(options = {}) {
    this.memtableSize = options.memtableSize || 1000;
    this.compactionThreshold = options.compactionThreshold || 4;
    
    this._memtable = new SkipList();
    this._immutable = null; // Being flushed
    this._levels = [[], []]; // Level 0 = recent, Level 1 = older
    this._size = 0;
    this._flushCount = 0;
    this._compactionCount = 0;
  }

  /**
   * Write a key-value pair. O(log n) in memtable.
   */
  put(key, value) {
    this._memtable.insert(key, value);
    this._size++;
    
    // Check if memtable needs flushing
    if (this._memtable.size >= this.memtableSize) {
      this._flush();
    }
  }

  /**
   * Delete a key by writing a tombstone.
   */
  delete(key) {
    this._memtable.insert(key, TOMBSTONE);
    
    if (this._memtable.size >= this.memtableSize) {
      this._flush();
    }
  }

  /**
   * Read a value by key.
   * Checks: memtable → immutable → L0 SSTables → L1 SSTables
   */
  get(key) {
    // 1. Check active memtable
    const memVal = this._memtable.get(key);
    if (memVal !== undefined) {
      return memVal === TOMBSTONE ? undefined : memVal;
    }
    
    // 2. Check immutable memtable
    if (this._immutable) {
      const immVal = this._immutable.get(key);
      if (immVal !== undefined) {
        return immVal === TOMBSTONE ? undefined : immVal;
      }
    }
    
    // 3. Check SSTables (newest first)
    for (let level = 0; level < this._levels.length; level++) {
      const tables = this._levels[level];
      for (let i = tables.length - 1; i >= 0; i--) {
        const table = tables[i];
        // Quick check: is key in range?
        if (key < table.minKey || key > table.maxKey) continue;
        
        const val = table.get(key);
        if (val !== undefined) {
          return val === TOMBSTONE ? undefined : val;
        }
      }
    }
    
    return undefined;
  }

  /**
   * Flush memtable to a new SSTable in Level 0.
   */
  _flush() {
    if (this._memtable.size === 0) return;
    
    // Convert memtable to sorted array
    const entries = [...this._memtable].map(({ key, value }) => ({ key, value }));
    const sstable = new SSTable(entries, 0);
    
    this._levels[0].push(sstable);
    this._flushCount++;
    
    // Create new empty memtable
    this._memtable = new SkipList();
    
    // Check if compaction is needed
    if (this._levels[0].length >= this.compactionThreshold) {
      this._compact(0);
    }
  }

  /**
   * Compact Level N by merging all its SSTables into Level N+1.
   */
  _compact(level) {
    if (level >= this._levels.length - 1) {
      this._levels.push([]); // Add new level
    }
    
    const tables = this._levels[level];
    if (tables.length === 0) return;
    
    // Merge all tables at this level
    const merged = this._mergeSSTables(tables);
    const newSSTable = new SSTable(merged, level + 1);
    
    // Replace: clear current level, add to next
    this._levels[level] = [];
    this._levels[level + 1].push(newSSTable);
    this._compactionCount++;
    
    // Recursive compaction if next level is also full
    if (this._levels[level + 1].length >= this.compactionThreshold) {
      this._compact(level + 1);
    }
  }

  /**
   * Merge multiple sorted SSTables into one sorted array.
   * Newer entries (later tables) win for duplicate keys.
   * Tombstones are preserved (for correctness with older levels).
   */
  _mergeSSTables(tables) {
    // Simple k-way merge using a priority queue approach
    const result = new Map(); // key → value (last writer wins)
    
    // Process oldest first, newest last (so newest wins)
    for (const table of tables) {
      for (const entry of table.entries) {
        result.set(entry.key, entry.value);
      }
    }
    
    // Convert to sorted array
    return [...result.entries()]
      .sort((a, b) => {
        if (a[0] < b[0]) return -1;
        if (a[0] > b[0]) return 1;
        return 0;
      })
      .map(([key, value]) => ({ key, value }));
  }

  /**
   * Scan all entries in key order.
   */
  *scan() {
    // Merge all sources
    const allEntries = new Map();
    
    // Process from oldest to newest (so newest wins)
    for (let level = this._levels.length - 1; level >= 0; level--) {
      for (const table of this._levels[level]) {
        for (const entry of table.entries) {
          allEntries.set(entry.key, entry.value);
        }
      }
    }
    
    if (this._immutable) {
      for (const { key, value } of this._immutable) {
        allEntries.set(key, value);
      }
    }
    
    for (const { key, value } of this._memtable) {
      allEntries.set(key, value);
    }
    
    // Sort and yield (skip tombstones)
    const sorted = [...allEntries.entries()].sort((a, b) => {
      if (a[0] < b[0]) return -1;
      if (a[0] > b[0]) return 1;
      return 0;
    });
    
    for (const [key, value] of sorted) {
      if (value !== TOMBSTONE) {
        yield { key, value };
      }
    }
  }

  /**
   * Get statistics.
   */
  getStats() {
    const sstableCounts = this._levels.map(l => l.length);
    const sstableEntries = this._levels.map(l => l.reduce((s, t) => s + t.size, 0));
    
    return {
      memtableSize: this._memtable.size,
      memtableCapacity: this.memtableSize,
      levels: this._levels.length,
      sstablesPerLevel: sstableCounts,
      entriesPerLevel: sstableEntries,
      totalSSTables: sstableCounts.reduce((s, c) => s + c, 0),
      flushes: this._flushCount,
      compactions: this._compactionCount,
      totalEntries: this._memtable.size + sstableEntries.reduce((s, c) => s + c, 0),
    };
  }
}

export { TOMBSTONE };
