// group-commit-wal.js — Write-Ahead Log with Group Commit Optimization
//
// Instead of fsyncing after every transaction (slow: ~5000 TPS with 200µs fsync),
// batch multiple transactions and fsync once (fast: 100K+ TPS).
//
// How it works:
// 1. Transactions append to WAL buffer (fast, in-memory)
// 2. When buffer is full OR timeout expires → batch fsync
// 3. All waiting transactions are notified after the single fsync
//
// Used in: PostgreSQL, MySQL InnoDB, SQLite WAL mode

/**
 * GroupCommitWAL — WAL with batched fsync for high throughput.
 */
export class GroupCommitWAL {
  constructor(options = {}) {
    this._buffer = [];           // Pending log entries
    this._flushed = [];          // Already flushed entries
    this._nextLsn = 1;
    this._flushCount = 0;
    this._totalEntries = 0;
    this._batchSize = options.batchSize || 32;   // Max entries per batch
    this._maxWaitMs = options.maxWaitMs || 10;    // Max wait before forced flush
    this._lastFlushTime = Date.now();
    
    // Simulate fsync latency
    this._fsyncLatencyMs = options.fsyncLatencyMs || 0.2; // 200µs
    this._totalFsyncMs = 0;
    
    // Pending commit callbacks
    this._pendingCallbacks = [];
  }

  get stats() {
    return {
      totalEntries: this._totalEntries,
      flushCount: this._flushCount,
      avgBatchSize: this._flushCount > 0 ? this._totalEntries / this._flushCount : 0,
      totalFsyncMs: this._totalFsyncMs,
      bufferSize: this._buffer.length,
    };
  }

  /**
   * Append a log entry. Returns the LSN.
   * The entry is NOT durable until flush() is called.
   */
  append(entry) {
    const lsn = this._nextLsn++;
    this._buffer.push({ lsn, entry, time: Date.now() });
    this._totalEntries++;
    
    // Auto-flush if batch is full
    if (this._buffer.length >= this._batchSize) {
      this.flush();
    }
    
    return lsn;
  }

  /**
   * Commit a transaction: append commit record and flush (or batch).
   * In group commit mode, this may wait for the next batch flush.
   */
  commit(txId, entries) {
    const lsns = [];
    for (const entry of entries) {
      lsns.push(this.append({ ...entry, txId }));
    }
    const commitLsn = this.append({ type: 'COMMIT', txId });
    lsns.push(commitLsn);
    
    // Check if we should flush now
    if (this._buffer.length >= this._batchSize) {
      this.flush();
    }
    
    return { lsns, commitLsn, flushed: !this._buffer.some(b => b.lsn === commitLsn) };
  }

  /**
   * Flush all pending entries to disk (simulated fsync).
   * This is the GROUP COMMIT: one fsync for multiple transactions.
   */
  flush() {
    if (this._buffer.length === 0) return 0;
    
    const batch = this._buffer.splice(0);
    this._flushed.push(...batch);
    this._flushCount++;
    this._totalFsyncMs += this._fsyncLatencyMs;
    this._lastFlushTime = Date.now();
    
    return batch.length;
  }

  /**
   * Check if a flush is needed based on timeout.
   */
  maybeFlush() {
    if (this._buffer.length > 0 && 
        Date.now() - this._lastFlushTime >= this._maxWaitMs) {
      return this.flush();
    }
    return 0;
  }

  /**
   * Get all flushed entries from a starting LSN.
   */
  readFrom(startLsn) {
    return this._flushed
      .filter(e => e.lsn >= startLsn)
      .map(e => ({ lsn: e.lsn, ...e.entry }));
  }

  /**
   * Simulate processing N transactions with group commit.
   * Returns throughput metrics.
   */
  static benchmark(numTxns, opsPerTxn = 3, batchSize = 32) {
    const wal = new GroupCommitWAL({ batchSize, fsyncLatencyMs: 0.2 });
    
    const t0 = performance.now();
    for (let i = 0; i < numTxns; i++) {
      const entries = [];
      for (let j = 0; j < opsPerTxn; j++) {
        entries.push({ type: 'UPDATE', key: `key-${j}`, value: i });
      }
      wal.commit(i, entries);
    }
    wal.flush(); // Flush remaining
    const elapsed = performance.now() - t0;
    
    const stats = wal.stats;
    return {
      txns: numTxns,
      elapsed,
      tps: numTxns / (elapsed / 1000),
      avgBatchSize: stats.avgBatchSize,
      flushes: stats.flushCount,
      totalFsyncMs: stats.totalFsyncMs,
      // Compare: without group commit, each txn would need its own fsync
      withoutGroupCommit: numTxns * 0.2, // ms spent on fsync
      savings: `${((1 - stats.totalFsyncMs / (numTxns * 0.2)) * 100).toFixed(1)}% fewer fsyncs`,
    };
  }
}
