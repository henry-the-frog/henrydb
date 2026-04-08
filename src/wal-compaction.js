// wal-compaction.js — WAL checkpoint and truncation
// After a checkpoint (all dirty pages flushed), the WAL can be truncated
// to reclaim disk space. This simulates the WAL lifecycle:
// 1. Append log entries (INSERT, UPDATE, DELETE)
// 2. Checkpoint: flush all dirty state, record LSN
// 3. Truncate: remove entries before checkpoint LSN

/**
 * WALCompactor — manages WAL lifecycle with checkpoint and truncation.
 */
export class WALCompactor {
  constructor(options = {}) {
    this.maxWalSize = options.maxWalSize || 1000; // Auto-checkpoint threshold
    this.autoCheckpoint = options.autoCheckpoint !== false;
    
    this._entries = []; // { lsn, type, table, key, data, txnId }
    this._nextLSN = 1;
    this._checkpoints = []; // { lsn, timestamp }
    this._lastCheckpointLSN = 0;
    this._truncatedBefore = 0; // All entries before this LSN have been removed
    
    this.stats = {
      entriesWritten: 0,
      checkpoints: 0,
      truncations: 0,
      bytesReclaimed: 0,
      peakEntries: 0,
    };
  }

  /**
   * Append a log entry.
   */
  append(type, table, key, data, txnId = null) {
    const lsn = this._nextLSN++;
    const entry = { lsn, type, table, key, data, txnId, timestamp: Date.now() };
    this._entries.push(entry);
    this.stats.entriesWritten++;
    
    if (this._entries.length > this.stats.peakEntries) {
      this.stats.peakEntries = this._entries.length;
    }

    // Auto-checkpoint if WAL is too large
    if (this.autoCheckpoint && this._entries.length >= this.maxWalSize) {
      this.checkpoint();
    }

    return lsn;
  }

  /**
   * Create a checkpoint: mark all entries up to current LSN as durable.
   * In a real DB, this would flush all dirty pages to disk first.
   */
  checkpoint() {
    const checkpointLSN = this._nextLSN - 1;
    if (checkpointLSN <= this._lastCheckpointLSN) return null; // Nothing new

    this._checkpoints.push({ lsn: checkpointLSN, timestamp: Date.now() });
    this._lastCheckpointLSN = checkpointLSN;
    this.stats.checkpoints++;

    // Auto-truncate after checkpoint
    this.truncate(checkpointLSN);

    return checkpointLSN;
  }

  /**
   * Truncate WAL entries before the given LSN.
   * Only entries before a checkpoint can be safely truncated.
   */
  truncate(beforeLSN) {
    // Can only truncate up to last checkpoint
    const safeLSN = Math.min(beforeLSN, this._lastCheckpointLSN);
    if (safeLSN <= this._truncatedBefore) return 0;

    const beforeCount = this._entries.length;
    this._entries = this._entries.filter(e => e.lsn > safeLSN);
    const removed = beforeCount - this._entries.length;

    if (removed > 0) {
      this._truncatedBefore = safeLSN;
      this.stats.truncations++;
      this.stats.bytesReclaimed += removed; // Simplified: 1 entry ≈ 1 unit
    }

    return removed;
  }

  /**
   * Replay WAL entries from a given LSN (for crash recovery).
   */
  replay(fromLSN = 0) {
    return this._entries.filter(e => e.lsn > fromLSN);
  }

  /**
   * Get entries for a specific transaction.
   */
  getTransactionEntries(txnId) {
    return this._entries.filter(e => e.txnId === txnId);
  }

  /**
   * Get entries for a specific table.
   */
  getTableEntries(table) {
    return this._entries.filter(e => e.table === table);
  }

  get currentLSN() { return this._nextLSN - 1; }
  get entryCount() { return this._entries.length; }
  get lastCheckpointLSN() { return this._lastCheckpointLSN; }

  getStats() {
    return {
      ...this.stats,
      currentEntries: this._entries.length,
      currentLSN: this._nextLSN - 1,
      lastCheckpointLSN: this._lastCheckpointLSN,
      truncatedBefore: this._truncatedBefore,
      checkpointCount: this._checkpoints.length,
    };
  }
}
