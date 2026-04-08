// lsm-compaction.js — LSM-tree with compaction strategies
// Implements a Log-Structured Merge tree with:
// 1. Memtable (in-memory sorted buffer)
// 2. SSTable flush (memtable → sorted run on disk)
// 3. Size-tiered compaction (STCS): merge same-size SSTables
// 4. Leveled compaction (LCS): maintain sorted levels with size ratio

/**
 * SSTable — immutable sorted run.
 */
class SSTable {
  constructor(id, entries, level = 0) {
    this.id = id;
    this.level = level;
    // entries: sorted array of { key, value, deleted }
    this.entries = entries;
    this.size = entries.length;
    this.minKey = entries.length > 0 ? entries[0].key : null;
    this.maxKey = entries.length > 0 ? entries[entries.length - 1].key : null;
  }

  get(key) {
    // Binary search
    let lo = 0, hi = this.entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const cmp = this.entries[mid].key < key ? -1 : this.entries[mid].key > key ? 1 : 0;
      if (cmp === 0) {
        const entry = this.entries[mid];
        return entry.deleted ? { found: true, deleted: true } : { found: true, value: entry.value };
      }
      if (cmp < 0) lo = mid + 1;
      else hi = mid - 1;
    }
    return { found: false };
  }

  overlaps(lo, hi) {
    return !(this.maxKey < lo || this.minKey > hi);
  }
}

/**
 * LSMTree — Log-Structured Merge tree with compaction.
 */
export class LSMTree {
  constructor(options = {}) {
    this.memtableLimit = options.memtableLimit || 64;
    this.levelSizeRatio = options.levelSizeRatio || 10; // For leveled compaction
    this.sizeTierThreshold = options.sizeTierThreshold || 4; // For size-tiered: merge when N same-size tables
    this.strategy = options.strategy || 'leveled'; // 'leveled' | 'size-tiered'
    
    this._memtable = new Map(); // Current write buffer
    this._immutableMemtables = []; // Being flushed
    this._levels = [[]]; // levels[0] = L0, levels[1] = L1, etc.
    this._nextSSTableId = 0;
    
    this.stats = { writes: 0, reads: 0, flushes: 0, compactions: 0, bytesCompacted: 0 };
  }

  /**
   * Write a key-value pair.
   */
  put(key, value) {
    this._memtable.set(key, { key, value, deleted: false });
    this.stats.writes++;

    if (this._memtable.size >= this.memtableLimit) {
      this._flush();
    }
  }

  /**
   * Delete a key (tombstone).
   */
  delete(key) {
    this._memtable.set(key, { key, value: null, deleted: true });
    this.stats.writes++;

    if (this._memtable.size >= this.memtableLimit) {
      this._flush();
    }
  }

  /**
   * Read a key. Checks memtable → immutable memtables → L0 → L1 → ...
   */
  get(key) {
    this.stats.reads++;

    // Check memtable
    if (this._memtable.has(key)) {
      const entry = this._memtable.get(key);
      return entry.deleted ? undefined : entry.value;
    }

    // Check immutable memtables (newest first)
    for (let i = this._immutableMemtables.length - 1; i >= 0; i--) {
      if (this._immutableMemtables[i].has(key)) {
        const entry = this._immutableMemtables[i].get(key);
        return entry.deleted ? undefined : entry.value;
      }
    }

    // Check SSTables level by level (newest first within level)
    for (const level of this._levels) {
      for (let i = level.length - 1; i >= 0; i--) {
        const result = level[i].get(key);
        if (result.found) {
          return result.deleted ? undefined : result.value;
        }
      }
    }

    return undefined;
  }

  /**
   * Flush memtable to L0 SSTable.
   */
  _flush() {
    if (this._memtable.size === 0) return;

    const entries = [...this._memtable.values()].sort((a, b) =>
      a.key < b.key ? -1 : a.key > b.key ? 1 : 0
    );

    const sst = new SSTable(this._nextSSTableId++, entries, 0);
    
    if (this._levels.length === 0) this._levels.push([]);
    this._levels[0].push(sst);
    
    this._memtable = new Map();
    this.stats.flushes++;

    // Trigger compaction if needed
    this._maybeCompact();
  }

  /**
   * Check if compaction is needed and run it.
   */
  _maybeCompact() {
    if (this.strategy === 'leveled') {
      this._leveledCompaction();
    } else {
      this._sizeTieredCompaction();
    }
  }

  /**
   * Leveled compaction: when a level exceeds its size limit,
   * pick an SSTable and merge it into the next level.
   */
  _leveledCompaction() {
    for (let level = 0; level < this._levels.length; level++) {
      const maxSize = level === 0 ? this.sizeTierThreshold : Math.pow(this.levelSizeRatio, level);
      
      if (this._levels[level].length > maxSize) {
        // Pick oldest SSTable from this level
        const sst = this._levels[level].shift();
        
        // Ensure next level exists
        while (this._levels.length <= level + 1) this._levels.push([]);
        
        // Find overlapping SSTables in next level
        const nextLevel = this._levels[level + 1];
        const overlapping = nextLevel.filter(s => s.overlaps(sst.minKey, sst.maxKey));
        const nonOverlapping = nextLevel.filter(s => !s.overlaps(sst.minKey, sst.maxKey));
        
        // Merge all overlapping + the source SSTable
        const merged = this._mergeSSTables([sst, ...overlapping]);
        const newSST = new SSTable(this._nextSSTableId++, merged, level + 1);
        
        this._levels[level + 1] = [...nonOverlapping, newSST].sort((a, b) =>
          a.minKey < b.minKey ? -1 : a.minKey > b.minKey ? 1 : 0
        );
        
        this.stats.compactions++;
        this.stats.bytesCompacted += sst.size + overlapping.reduce((s, t) => s + t.size, 0);
      }
    }
  }

  /**
   * Size-tiered compaction: merge SSTables of similar size.
   */
  _sizeTieredCompaction() {
    for (let level = 0; level < this._levels.length; level++) {
      if (this._levels[level].length >= this.sizeTierThreshold) {
        // Take the oldest N tables and merge them
        const toMerge = this._levels[level].splice(0, this.sizeTierThreshold);
        const merged = this._mergeSSTables(toMerge);
        
        // Push to next level
        while (this._levels.length <= level + 1) this._levels.push([]);
        const newSST = new SSTable(this._nextSSTableId++, merged, level + 1);
        this._levels[level + 1].push(newSST);
        
        this.stats.compactions++;
        this.stats.bytesCompacted += toMerge.reduce((s, t) => s + t.size, 0);
      }
    }
  }

  /**
   * K-way merge of multiple SSTables.
   * Newer entries override older ones. Tombstones cancel entries.
   */
  _mergeSSTables(tables) {
    const merged = new Map();
    
    // Process oldest to newest (later entries override)
    for (const table of tables) {
      for (const entry of table.entries) {
        merged.set(entry.key, entry);
      }
    }

    // Sort and return (skip tombstones at the deepest level)
    return [...merged.values()].sort((a, b) =>
      a.key < b.key ? -1 : a.key > b.key ? 1 : 0
    );
  }

  /**
   * Force flush and compact everything.
   */
  compact() {
    this._flush();
    for (let i = 0; i < 10; i++) this._maybeCompact();
  }

  /**
   * Scan all entries in key order.
   */
  *scan(lo, hi) {
    // Collect all entries, deduplicate, filter range
    const all = new Map();
    
    // SSTables (oldest first → newest overrides)
    for (let level = this._levels.length - 1; level >= 0; level--) {
      for (const sst of this._levels[level]) {
        for (const entry of sst.entries) {
          if ((!lo || entry.key >= lo) && (!hi || entry.key <= hi)) {
            all.set(entry.key, entry);
          }
        }
      }
    }

    // Memtable (newest)
    for (const [key, entry] of this._memtable) {
      if ((!lo || key >= lo) && (!hi || key <= hi)) {
        all.set(key, entry);
      }
    }

    // Sort and yield non-deleted
    const sorted = [...all.values()].sort((a, b) =>
      a.key < b.key ? -1 : a.key > b.key ? 1 : 0
    );

    for (const entry of sorted) {
      if (!entry.deleted) yield { key: entry.key, value: entry.value };
    }
  }

  get size() {
    let count = 0;
    for (const { deleted } of this.scan()) if (!deleted) count++;
    return count;
  }

  getStats() {
    const sstCounts = this._levels.map((l, i) => `L${i}:${l.length}`).join(' ');
    return {
      ...this.stats,
      memtableSize: this._memtable.size,
      levels: this._levels.length,
      sstables: sstCounts,
      strategy: this.strategy,
    };
  }
}
