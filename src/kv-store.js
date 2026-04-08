// kv-store.js — Key-value store with memtable, SSTables, range tombstones

/**
 * Range Tombstone — marks a range [start, end) as deleted.
 */
export class RangeTombstoneStore {
  constructor() {
    this._tombstones = []; // [{start, end, timestamp}]
  }

  /** Add a range deletion */
  addTombstone(start, end, timestamp = Date.now()) {
    this._tombstones.push({ start, end, timestamp });
  }

  /** Check if key is covered by a tombstone */
  isDeleted(key, readTs = Infinity) {
    return this._tombstones.some(t => key >= t.start && key < t.end && t.timestamp <= readTs);
  }

  /** Filter out tombstoned entries from a sorted list */
  filter(entries, keyFn = e => e.key) {
    return entries.filter(e => !this.isDeleted(keyFn(e)));
  }

  /** Compact overlapping tombstones */
  compact() {
    if (this._tombstones.length <= 1) return;
    this._tombstones.sort((a, b) => a.start < b.start ? -1 : a.start > b.start ? 1 : 0);
    const merged = [this._tombstones[0]];
    for (let i = 1; i < this._tombstones.length; i++) {
      const last = merged[merged.length - 1];
      if (this._tombstones[i].start <= last.end) {
        last.end = this._tombstones[i].end > last.end ? this._tombstones[i].end : last.end;
        last.timestamp = Math.max(last.timestamp, this._tombstones[i].timestamp);
      } else {
        merged.push(this._tombstones[i]);
      }
    }
    this._tombstones = merged;
  }

  get count() { return this._tombstones.length; }
}

/**
 * Simple KV Store — memtable + SSTable simulation.
 */
export class KVStore {
  constructor(memtableLimit = 1000) {
    this.memtableLimit = memtableLimit;
    this._memtable = new Map(); // Active writes
    this._sstables = []; // Flushed sorted tables
    this._tombstones = new RangeTombstoneStore();
    this.stats = { gets: 0, puts: 0, flushes: 0 };
  }

  put(key, value) {
    this._memtable.set(key, { value, ts: Date.now(), deleted: false });
    this.stats.puts++;
    if (this._memtable.size >= this.memtableLimit) this.flush();
  }

  get(key) {
    this.stats.gets++;
    // Check tombstones first
    if (this._tombstones.isDeleted(key)) return undefined;
    // Check memtable
    const mem = this._memtable.get(key);
    if (mem) return mem.deleted ? undefined : mem.value;
    // Check SSTables (newest first)
    for (let i = this._sstables.length - 1; i >= 0; i--) {
      const entry = this._sstables[i].get(key);
      if (entry) return entry.deleted ? undefined : entry.value;
    }
    return undefined;
  }

  delete(key) {
    this._memtable.set(key, { value: null, ts: Date.now(), deleted: true });
  }

  deleteRange(start, end) {
    this._tombstones.addTombstone(start, end);
  }

  flush() {
    if (this._memtable.size === 0) return;
    // Sort memtable entries and freeze as SSTable
    const sorted = new Map([...this._memtable.entries()].sort());
    this._sstables.push(sorted);
    this._memtable = new Map();
    this.stats.flushes++;
  }

  /** Compact all SSTables into one */
  compact() {
    this.flush();
    if (this._sstables.length <= 1) return;
    
    const merged = new Map();
    for (const sst of this._sstables) {
      for (const [key, entry] of sst) {
        if (!this._tombstones.isDeleted(key)) merged.set(key, entry);
      }
    }
    this._sstables = [merged];
    this._tombstones.compact();
  }

  get size() {
    let count = 0;
    for (const [, entry] of this._memtable) if (!entry.deleted) count++;
    for (const sst of this._sstables) for (const [, entry] of sst) if (!entry.deleted) count++;
    return count;
  }

  get sstableCount() { return this._sstables.length; }
}

/**
 * Column Family — group columns for different access patterns.
 */
export class ColumnFamily {
  constructor(name, columns) {
    this.name = name;
    this.columns = columns;
    this._store = new KVStore();
  }

  putRow(rowKey, columnValues) {
    for (const col of this.columns) {
      if (columnValues[col] !== undefined) {
        this._store.put(`${rowKey}:${col}`, columnValues[col]);
      }
    }
  }

  getRow(rowKey) {
    const row = {};
    for (const col of this.columns) {
      const val = this._store.get(`${rowKey}:${col}`);
      if (val !== undefined) row[col] = val;
    }
    return Object.keys(row).length > 0 ? row : null;
  }

  getColumn(rowKey, col) {
    return this._store.get(`${rowKey}:${col}`);
  }

  deleteRow(rowKey) {
    for (const col of this.columns) this._store.delete(`${rowKey}:${col}`);
  }
}
