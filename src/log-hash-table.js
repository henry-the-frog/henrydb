// log-hash-table.js — Log-structured hash table (Bitcask-style)
// All writes are append-only to a log file. An in-memory hash index
// maps keys to their position in the log. Reads are O(1): hash lookup → seek.
// Compaction merges old segments to reclaim space.

export class LogHashTable {
  constructor() {
    this._log = []; // Append-only log: [{key, value, offset, deleted}]
    this._index = new Map(); // key → log offset (latest)
    this._size = 0;
  }

  set(key, value) {
    const offset = this._log.length;
    this._log.push({ key, value, offset, deleted: false });
    
    if (!this._index.has(key)) this._size++;
    this._index.set(key, offset);
  }

  get(key) {
    const offset = this._index.get(key);
    if (offset === undefined) return undefined;
    const entry = this._log[offset];
    return entry.deleted ? undefined : entry.value;
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    if (!this._index.has(key)) return false;
    // Append tombstone
    const offset = this._log.length;
    this._log.push({ key, value: null, offset, deleted: true });
    this._index.set(key, offset);
    this._size--;
    return true;
  }

  /**
   * Compact: remove old/deleted entries, rebuild index.
   */
  compact() {
    const newLog = [];
    const newIndex = new Map();

    // Only keep latest non-deleted entry per key
    for (const [key, offset] of this._index) {
      const entry = this._log[offset];
      if (!entry.deleted) {
        const newOffset = newLog.length;
        newLog.push({ ...entry, offset: newOffset });
        newIndex.set(key, newOffset);
      }
    }

    const oldSize = this._log.length;
    this._log = newLog;
    this._index = newIndex;
    return { entriesBefore: oldSize, entriesAfter: newLog.length, reclaimed: oldSize - newLog.length };
  }

  get size() { return this._size; }
  get logSize() { return this._log.length; }

  getStats() {
    return { size: this._size, logEntries: this._log.length, wasteRatio: this._log.length > 0 ? ((this._log.length - this._size) / this._log.length * 100).toFixed(1) + '%' : '0%' };
  }
}
