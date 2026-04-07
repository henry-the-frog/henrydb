// lsm.js — Log-Structured Merge Tree for HenryDB
// Write-optimized storage engine used by LevelDB/RocksDB/Cassandra.
//
// Architecture:
// 1. Memtable: in-memory sorted structure (using skip list) for recent writes
// 2. SSTables: sorted, immutable on-disk tables (in-memory simulation)
// 3. Compaction: merge overlapping SSTables to reduce read amplification
//
// Write path: write to memtable → flush to SSTable when full
// Read path: check memtable → check SSTables (newest first)

import { SkipList } from './skip-list.js';

// Tombstone marker for deletes (so they propagate through compaction)
const TOMBSTONE = Symbol('TOMBSTONE');

/**
 * SSTable: Sorted String Table.
 * An immutable, sorted array of key-value pairs.
 */
class SSTable {
  constructor(entries = []) {
    // entries: [{ key, value }] sorted by key
    this._entries = entries;
    this._bloomFilter = new Set(entries.map(e => e.key)); // Simple bloom approximation
    this.level = 0;
    this.created = Date.now();
  }

  /**
   * Binary search for a key.
   */
  get(key) {
    // Quick bloom check
    if (!this._bloomFilter.has(key)) return undefined;
    
    let lo = 0, hi = this._entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this._entries[mid].key === key) {
        return this._entries[mid].value;
      }
      if (this._entries[mid].key < key) lo = mid + 1;
      else hi = mid - 1;
    }
    return undefined;
  }

  /**
   * Range scan: return entries where low <= key <= high.
   */
  range(low, high) {
    const results = [];
    // Find start position with binary search
    let lo = 0, hi = this._entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this._entries[mid].key < low) lo = mid + 1;
      else hi = mid - 1;
    }
    // Scan forward
    for (let i = lo; i < this._entries.length && this._entries[i].key <= high; i++) {
      results.push(this._entries[i]);
    }
    return results;
  }

  get size() { return this._entries.length; }
  get entries() { return this._entries; }
}

/**
 * Merge two sorted arrays of entries.
 * Later entries (from newer SSTable) take precedence.
 */
function mergeSorted(entries1, entries2) {
  const merged = [];
  let i = 0, j = 0;
  
  while (i < entries1.length && j < entries2.length) {
    if (entries1[i].key < entries2[j].key) {
      merged.push(entries1[i++]);
    } else if (entries1[i].key > entries2[j].key) {
      merged.push(entries2[j++]);
    } else {
      // Same key: newer entry wins (entries2 is newer)
      merged.push(entries2[j++]);
      i++; // Skip older entry
    }
  }
  
  while (i < entries1.length) merged.push(entries1[i++]);
  while (j < entries2.length) merged.push(entries2[j++]);
  
  // Remove tombstones during compaction
  return merged.filter(e => e.value !== TOMBSTONE);
}

/**
 * LSM Tree: Log-Structured Merge Tree.
 */
export class LSMTree {
  constructor(memtableMaxSize = 1000) {
    this._memtableMaxSize = memtableMaxSize;
    this._memtable = new SkipList();
    this._sstables = []; // Sorted by creation time (newest first)
    this._writeCount = 0;
    this._readCount = 0;
    this._compactionCount = 0;
  }

  /**
   * Put a key-value pair. O(log n) for memtable.
   */
  put(key, value) {
    this._memtable.insert(key, value);
    this._writeCount++;
    
    // Flush memtable to SSTable when full
    if (this._memtable.size >= this._memtableMaxSize) {
      this._flushMemtable();
    }
  }

  /**
   * Delete a key by writing a tombstone.
   */
  delete(key) {
    this.put(key, TOMBSTONE);
  }

  /**
   * Get a value by key.
   * Checks memtable first, then SSTables (newest first).
   */
  get(key) {
    this._readCount++;
    
    // 1. Check memtable
    const memValue = this._memtable.find(key);
    if (memValue !== null) {
      return memValue === TOMBSTONE ? undefined : memValue;
    }
    
    // 2. Check SSTables (newest first)
    for (const sst of this._sstables) {
      const value = sst.get(key);
      if (value !== undefined) {
        return value === TOMBSTONE ? undefined : value;
      }
    }
    
    return undefined;
  }

  /**
   * Range scan across all levels.
   */
  range(low, high) {
    // Merge results from all sources
    const allEntries = new Map();
    
    // SSTables (oldest first, so newer overwrites older)
    for (let i = this._sstables.length - 1; i >= 0; i--) {
      for (const entry of this._sstables[i].range(low, high)) {
        allEntries.set(entry.key, entry.value);
      }
    }
    
    // Memtable (newest, overwrites all)
    for (const entry of this._memtable) {
      if (entry.key >= low && entry.key <= high) {
        allEntries.set(entry.key, entry.value);
      }
    }
    
    // Remove tombstones and sort
    return [...allEntries.entries()]
      .filter(([, value]) => value !== TOMBSTONE)
      .sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)
      .map(([key, value]) => ({ key, value }));
  }

  /**
   * Flush memtable to a new SSTable.
   */
  _flushMemtable() {
    if (this._memtable.size === 0) return;
    
    const entries = [...this._memtable].map(e => ({ key: e.key, value: e.value }));
    const sst = new SSTable(entries);
    this._sstables.unshift(sst); // Add to front (newest)
    
    // Create fresh memtable
    this._memtable = new SkipList();
    
    // Maybe trigger compaction
    if (this._sstables.length > 4) {
      this._compact();
    }
  }

  /**
   * Compact: merge oldest SSTables to reduce read amplification.
   */
  _compact() {
    if (this._sstables.length < 2) return;
    
    // Merge the two oldest SSTables
    const older = this._sstables.pop();
    const newer = this._sstables.pop();
    
    const merged = mergeSorted(older.entries, newer.entries);
    const compacted = new SSTable(merged);
    compacted.level = Math.max(older.level, newer.level) + 1;
    
    this._sstables.push(compacted);
    this._compactionCount++;
  }

  /**
   * Force flush memtable.
   */
  flush() {
    this._flushMemtable();
  }

  /**
   * Get statistics.
   */
  stats() {
    return {
      memtableSize: this._memtable.size,
      sstableCount: this._sstables.length,
      sstableSizes: this._sstables.map(s => s.size),
      writes: this._writeCount,
      reads: this._readCount,
      compactions: this._compactionCount,
      memtableMaxSize: this._memtableMaxSize,
    };
  }
}

export { TOMBSTONE, SSTable, mergeSorted };
