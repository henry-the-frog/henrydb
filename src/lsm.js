// lsm.js — Log-Structured Merge Tree
// Architecture: MemTable (sorted in-memory) → SSTable (sorted on-disk) → Compaction
// Uses Bloom filters for fast negative lookups on SSTables.
//
// Write path: writes go to MemTable. When MemTable reaches threshold, flush to SSTable.
// Read path: check MemTable → check SSTables (newest first, with Bloom filter).
// Compaction: merge overlapping SSTables to reduce read amplification.

import { BloomFilter } from './bloom.js';

/**
 * MemTable — sorted in-memory buffer (uses a sorted array for simplicity).
 * In production, this would be a skip list or red-black tree.
 */
class MemTable {
  constructor(maxSize = 1000) {
    this._entries = new Map(); // key → { value, deleted, seq }
    this._maxSize = maxSize;
    this._seq = 0;
  }

  get size() { return this._entries.size; }
  get isFull() { return this._entries.size >= this._maxSize; }

  put(key, value) {
    this._entries.set(key, { value, deleted: false, seq: this._seq++ });
  }

  delete(key) {
    this._entries.set(key, { value: null, deleted: true, seq: this._seq++ });
  }

  get(key) {
    const entry = this._entries.get(key);
    if (!entry) return undefined;
    if (entry.deleted) return null; // tombstone
    return entry.value;
  }

  /** Get all entries sorted by key. */
  getSorted() {
    return [...this._entries.entries()]
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, entry]) => ({ key, ...entry }));
  }

  clear() {
    this._entries.clear();
    this._seq = 0;
  }
}

/**
 * SSTable — Sorted String Table (immutable, sorted key-value pairs).
 * In production, this would be file-backed. Here it's in-memory.
 */
class SSTable {
  /**
   * @param {Array<{key, value, deleted}>} entries — sorted by key
   * @param {number} level — compaction level (0 = freshest)
   */
  constructor(entries, level = 0) {
    this._entries = entries;
    this.level = level;
    this.id = SSTable._nextId++;
    this.createdAt = Date.now();
    
    // Build Bloom filter for fast negative lookups
    this._bloom = new BloomFilter(Math.max(entries.length, 1), 0.01);
    for (const entry of entries) {
      this._bloom.add(entry.key);
    }
    
    // Min/max key for range checks
    this.minKey = entries.length > 0 ? entries[0].key : null;
    this.maxKey = entries.length > 0 ? entries[entries.length - 1].key : null;
  }

  get size() { return this._entries.length; }

  /** Check if key might be in this SSTable (Bloom filter). */
  mightContain(key) {
    return this._bloom.test(key);
  }

  /** Get a value by key (binary search). */
  get(key) {
    if (!this.mightContain(key)) return undefined; // Bloom filter says no
    
    let lo = 0, hi = this._entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const cmp = this._entries[mid].key < key ? -1 : this._entries[mid].key > key ? 1 : 0;
      if (cmp === 0) {
        const entry = this._entries[mid];
        return entry.deleted ? null : entry.value;
      }
      if (cmp < 0) lo = mid + 1;
      else hi = mid - 1;
    }
    return undefined; // Not found (Bloom filter false positive)
  }

  /** Get all entries (for compaction/merge). */
  getEntries() {
    return this._entries;
  }

  /** Check if key range overlaps with this SSTable. */
  overlaps(minKey, maxKey) {
    if (this.maxKey < minKey || this.minKey > maxKey) return false;
    return true;
  }
}

SSTable._nextId = 0;

/**
 * LSMTree — Log-Structured Merge Tree.
 * Write-optimized storage engine with configurable compaction.
 */
export class LSMTree {
  /**
   * @param {Object} options
   * @param {number} [options.memtableSize=1000] — max memtable entries before flush
   * @param {number} [options.maxLevel=4] — max compaction levels
   * @param {number} [options.levelMultiplier=10] — size ratio between levels
   */
  constructor(options = {}) {
    this._memtableSize = options.memtableSize ?? 1000;
    this._maxLevel = options.maxLevel ?? 4;
    this._levelMultiplier = options.levelMultiplier ?? 10;
    
    this._memtable = new MemTable(this._memtableSize);
    this._immutable = null; // MemTable being flushed
    this._sstables = []; // Array of SSTables, newest first
    this._writeCount = 0;
    this._readCount = 0;
    this._bloomSaves = 0; // Reads saved by Bloom filter
    this._compactionCount = 0;
  }

  /**
   * Put a key-value pair.
   * @param {string} key
   * @param {*} value
   */
  put(key, value) {
    this._memtable.put(key, value);
    this._writeCount++;
    
    if (this._memtable.isFull) {
      this._flush();
    }
  }

  /**
   * Delete a key (writes a tombstone).
   * @param {string} key
   */
  delete(key) {
    this._memtable.delete(key);
    this._writeCount++;
    
    if (this._memtable.isFull) {
      this._flush();
    }
  }

  /**
   * Get a value by key.
   * Search order: memtable → immutable memtable → SSTables (newest first).
   * @param {string} key
   * @returns {*} — value, null (deleted), or undefined (not found)
   */
  get(key) {
    this._readCount++;
    
    // 1. Check memtable
    const memVal = this._memtable.get(key);
    if (memVal !== undefined) return memVal;
    
    // 2. Check immutable memtable
    if (this._immutable) {
      const immVal = this._immutable.get(key);
      if (immVal !== undefined) return immVal;
    }
    
    // 3. Check SSTables (newest first)
    for (const sst of this._sstables) {
      if (!sst.mightContain(key)) {
        this._bloomSaves++;
        continue;
      }
      const val = sst.get(key);
      if (val !== undefined) return val;
    }
    
    return undefined;
  }

  /**
   * Scan a range of keys.
   * @param {string} [startKey] — inclusive start
   * @param {string} [endKey] — exclusive end
   * @returns {Array<{key, value}>}
   */
  scan(startKey, endKey) {
    // Collect all entries from all sources
    const allEntries = new Map(); // key → {value, deleted, source_order}
    let order = 0;
    
    // SSTables (oldest first, so newer overwrites)
    for (let i = this._sstables.length - 1; i >= 0; i--) {
      for (const entry of this._sstables[i].getEntries()) {
        if (startKey && entry.key < startKey) continue;
        if (endKey && entry.key >= endKey) continue;
        allEntries.set(entry.key, { ...entry, order: order++ });
      }
    }
    
    // Immutable memtable
    if (this._immutable) {
      for (const entry of this._immutable.getSorted()) {
        if (startKey && entry.key < startKey) continue;
        if (endKey && entry.key >= endKey) continue;
        allEntries.set(entry.key, { ...entry, order: order++ });
      }
    }
    
    // Memtable (newest)
    for (const entry of this._memtable.getSorted()) {
      if (startKey && entry.key < startKey) continue;
      if (endKey && entry.key >= endKey) continue;
      allEntries.set(entry.key, { ...entry, order: order++ });
    }
    
    // Filter deleted, sort by key
    return [...allEntries.entries()]
      .filter(([, e]) => !e.deleted)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, e]) => ({ key, value: e.value }));
  }

  /** Flush memtable to a new SSTable. */
  _flush() {
    if (this._memtable.size === 0) return;
    
    const entries = this._memtable.getSorted();
    const sst = new SSTable(entries, 0);
    this._sstables.unshift(sst); // newest first
    
    this._memtable.clear();
    
    // Check if compaction is needed
    this._maybeCompact();
  }

  /** Run compaction if too many SSTables at any level. */
  _maybeCompact() {
    // Count SSTables per level
    const levels = new Map();
    for (const sst of this._sstables) {
      levels.set(sst.level, (levels.get(sst.level) || 0) + 1);
    }
    
    // Compact if level 0 has too many tables
    const l0Count = levels.get(0) || 0;
    if (l0Count >= 4) {
      this._compactLevel(0);
    }
  }

  /** Compact SSTables at a given level into the next level. */
  _compactLevel(level) {
    const sourceTables = this._sstables.filter(s => s.level === level);
    if (sourceTables.length < 2) return;
    
    // Merge all entries from source tables
    const merged = new Map();
    // Process oldest first so newest wins
    for (const sst of [...sourceTables].reverse()) {
      for (const entry of sst.getEntries()) {
        merged.set(entry.key, entry);
      }
    }
    
    // Create merged SSTable at next level
    const entries = [...merged.values()]
      .sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0);
    
    const newSST = new SSTable(entries, level + 1);
    
    // Remove source tables, add merged, keep sorted newest-first
    const sourceIds = new Set(sourceTables.map(s => s.id));
    this._sstables = this._sstables.filter(s => !sourceIds.has(s.id));
    this._sstables.unshift(newSST); // merged is "newest" among its level
    
    // Re-sort: level 0 first (uncompacted), then by id descending within level
    this._sstables.sort((a, b) => {
      if (a.level !== b.level) return a.level - b.level;
      return b.id - a.id; // newer first within level
    });
    
    this._compactionCount++;
  }

  /** Force flush + compaction. */
  compact() {
    this._flush();
    for (let level = 0; level < this._maxLevel; level++) {
      this._compactLevel(level);
    }
  }

  /** Get LSM tree statistics. */
  getStats() {
    const levels = new Map();
    for (const sst of this._sstables) {
      const count = levels.get(sst.level) || { tables: 0, entries: 0 };
      count.tables++;
      count.entries += sst.size;
      levels.set(sst.level, count);
    }
    
    return {
      memtableSize: this._memtable.size,
      memtableCapacity: this._memtableSize,
      sstableCount: this._sstables.length,
      levels: Object.fromEntries(levels),
      writes: this._writeCount,
      reads: this._readCount,
      bloomSaves: this._bloomSaves,
      compactions: this._compactionCount,
    };
  }
}
