// aries-recovery.js — ARIES Write-Ahead Log Recovery Protocol
//
// ARIES: Algorithm for Recovery and Isolation Exploiting Semantics
// The standard crash recovery protocol used by IBM DB2, SQL Server, PostgreSQL, etc.
//
// Three-phase recovery:
//   1. ANALYSIS — Determine dirty pages and active transactions at crash
//   2. REDO — Replay all changes (idempotent, from earliest dirty page LSN)
//   3. UNDO — Rollback incomplete transactions (backward, with CLRs)
//
// Key invariants:
//   - WAL: Changes to data pages are logged BEFORE the page is written to disk
//   - Force at commit: All log records for a txn are flushed before commit ack
//   - Steal: Dirty pages can be flushed before txn commits (hence need undo)
//
// References:
//   - Mohan et al., "ARIES: A Transaction Recovery Method" (1992)
//   - CMU 15-445 Lecture 20: Database Recovery

/**
 * Log record types.
 */
export const LOG_TYPE = {
  UPDATE:     'UPDATE',      // Data modification
  COMMIT:     'COMMIT',      // Transaction committed
  ABORT:      'ABORT',       // Transaction aborted
  BEGIN:      'BEGIN',        // Transaction started
  END:        'END',          // Transaction ended (cleanup done)
  CHECKPOINT: 'CHECKPOINT',  // Fuzzy checkpoint
  CLR:        'CLR',          // Compensation Log Record (undo of an update)
};

/**
 * A single WAL log record.
 */
export class LogRecord {
  constructor(options) {
    this.lsn = options.lsn;             // Log Sequence Number (monotonically increasing)
    this.type = options.type;            // LOG_TYPE
    this.txId = options.txId || null;    // Transaction ID
    this.pageId = options.pageId || null; // Page that was modified
    this.prevLsn = options.prevLsn || null; // Previous LSN for this txn (for undo chain)
    this.undoNextLsn = options.undoNextLsn || null; // For CLR: next record to undo
    this.before = options.before || null; // Before-image of data (for undo)
    this.after = options.after || null;   // After-image of data (for redo)
    
    // Checkpoint-specific
    this.activeTxns = options.activeTxns || null;  // ATT at checkpoint time
    this.dirtyPages = options.dirtyPages || null;   // DPT at checkpoint time
  }
}

/**
 * Write-Ahead Log — append-only log with sequential LSNs.
 */
export class WriteAheadLog {
  constructor() {
    this._records = [];
    this._nextLsn = 1;
    this._flushedLsn = 0;   // Highest LSN flushed to disk
    this._txLastLsn = new Map(); // txId → last LSN for this txn
  }

  get nextLsn() { return this._nextLsn; }
  get records() { return this._records; }

  /**
   * Append a log record. Returns the assigned LSN.
   */
  append(options) {
    const lsn = this._nextLsn++;
    const prevLsn = this._txLastLsn.get(options.txId) || null;
    const record = new LogRecord({ ...options, lsn, prevLsn });
    this._records.push(record);
    if (options.txId) this._txLastLsn.set(options.txId, lsn);
    return lsn;
  }

  /**
   * Flush log records up to the given LSN.
   */
  flush(lsn) {
    this._flushedLsn = Math.max(this._flushedLsn, lsn);
  }

  /**
   * Get records from a starting LSN (inclusive).
   */
  recordsFrom(startLsn) {
    return this._records.filter(r => r.lsn >= startLsn);
  }

  /**
   * Get records in reverse order from a starting LSN (inclusive).
   */
  recordsReverse(fromLsn) {
    return this._records.filter(r => r.lsn <= fromLsn).reverse();
  }

  /**
   * Find the last checkpoint record.
   */
  lastCheckpoint() {
    for (let i = this._records.length - 1; i >= 0; i--) {
      if (this._records[i].type === LOG_TYPE.CHECKPOINT) return this._records[i];
    }
    return null;
  }
}

/**
 * ARIES Recovery Manager — implements the three-phase recovery protocol.
 */
export class ARIESRecovery {
  constructor(wal, pageStore) {
    if (!wal && !pageStore) {
      // Self-contained mode: create internal WAL and page store
      this.wal = new WriteAheadLog();
      this.pageStore = new InMemoryPageStore();
      this._data = new Map(); // High-level key→value store for convenience API
      this._txnWrites = new Map(); // txId → [{key, before, after}]
      this._selfContained = true;
    } else {
      this.wal = wal;
      this.pageStore = pageStore;
      this._selfContained = false;
    }
    
    // Recovery state
    this.activeTxnTable = new Map();  // txId → {status, lastLsn}
    this.dirtyPageTable = new Map();  // pageId → recLsn (first LSN that dirtied this page)
    
    // Recovery stats
    this.stats = { analysisRecords: 0, redoRecords: 0, undoRecords: 0, clrsWritten: 0, redone: 0, undone: 0 };
  }

  // ============================================================
  // Convenience API (self-contained mode)
  // ============================================================

  /** Begin a transaction */
  begin(txId) {
    this.wal.append({ type: LOG_TYPE.BEGIN, txId });
    this._txnWrites.set(txId, []);
  }

  /** Write a key-value pair within a transaction */
  write(txId, key, value) {
    const before = this._data.has(key) ? this._data.get(key) : null;
    this._data.set(key, value);
    const writes = this._txnWrites.get(txId) || [];
    writes.push({ key, before, after: value });
    this._txnWrites.set(txId, writes);
    this.wal.append({
      type: LOG_TYPE.UPDATE,
      txId,
      pageId: key,
      before,
      after: value,
    });
  }

  /** Commit a transaction */
  commit(txId) {
    this.wal.append({ type: LOG_TYPE.COMMIT, txId });
    this.wal.append({ type: LOG_TYPE.END, txId });
    this._txnWrites.delete(txId);
  }

  /** Write a checkpoint */
  checkpoint() {
    // In self-contained mode, "flush" all current data to the page store
    // This simulates dirty pages being written to disk before/during checkpoint
    for (const [key, value] of this._data) {
      // Find the latest LSN for this key
      let latestLsn = 0;
      for (const rec of this.wal.records) {
        if (rec.type === LOG_TYPE.UPDATE && rec.pageId === key) {
          latestLsn = Math.max(latestLsn, rec.lsn);
        }
      }
      this.pageStore.applyRedo(key, value, latestLsn);
    }
    
    const activeTxns = new Map();
    // Track which transactions haven't been committed
    const begun = new Set();
    const ended = new Set();
    for (const rec of this.wal.records) {
      if (rec.type === LOG_TYPE.BEGIN) begun.add(rec.txId);
      if (rec.type === LOG_TYPE.COMMIT || rec.type === LOG_TYPE.ABORT || rec.type === LOG_TYPE.END) ended.add(rec.txId);
    }
    for (const txId of begun) {
      if (!ended.has(txId)) {
        // Find last LSN for this txn
        let lastLsn = 0;
        for (const rec of this.wal.records) {
          if (rec.txId === txId) lastLsn = rec.lsn;
        }
        activeTxns.set(txId, { status: 'active', lastLsn });
      }
    }
    
    this.wal.append({
      type: LOG_TYPE.CHECKPOINT,
      activeTxns,
      dirtyPages: new Map(), // No dirty pages after flush
    });
  }

  /** Simulate crash and run ARIES recovery */
  crashAndRecover() {
    // Reset recovery state
    this.activeTxnTable = new Map();
    this.dirtyPageTable = new Map();
    this.stats = { analysisRecords: 0, redoRecords: 0, undoRecords: 0, clrsWritten: 0, redone: 0, undone: 0 };

    // Simulate crash: pages written to disk (via checkpoint) survive
    // Pages only in buffer pool (written after last checkpoint) are lost
    const lastCkpt = this.wal.lastCheckpoint();
    if (!lastCkpt) {
      // No checkpoint — all pages lost (nothing was flushed to disk)
      this.pageStore = new InMemoryPageStore();
    }
    // If checkpoint exists, pageStore already has the flushed state from checkpoint()
    
    // Clear in-memory data — will be rebuilt from pageStore after recovery
    this._data = new Map();

    // Run ARIES recovery
    this._analysis();
    this._redo();
    this._undo();

    // Sync _data from pageStore
    for (const [pageId, page] of this.pageStore._pages) {
      if (page.data !== null) {
        this._data.set(pageId, page.data);
      }
    }

    this.stats.redone = this.stats.redoRecords;
    this.stats.undone = this.stats.undoRecords;
  }

  /**
   * Run full ARIES recovery. Returns recovery statistics.
   */
  recover() {
    this._analysis();
    this._redo();
    this._undo();
    return this.stats;
  }

  // ============================================================
  // Phase 1: ANALYSIS
  // ============================================================
  
  /**
   * Scan log forward from last checkpoint.
   * Reconstruct the Active Transaction Table (ATT) and Dirty Page Table (DPT).
   */
  _analysis() {
    const checkpoint = this.wal.lastCheckpoint();
    let startLsn = 1;
    
    if (checkpoint) {
      startLsn = checkpoint.lsn;
      // Restore ATT and DPT from checkpoint
      if (checkpoint.activeTxns) {
        for (const [txId, info] of checkpoint.activeTxns) {
          this.activeTxnTable.set(txId, { ...info });
        }
      }
      if (checkpoint.dirtyPages) {
        for (const [pageId, recLsn] of checkpoint.dirtyPages) {
          this.dirtyPageTable.set(pageId, recLsn);
        }
      }
    }
    
    const records = this.wal.recordsFrom(startLsn);
    
    for (const rec of records) {
      this.stats.analysisRecords++;
      
      switch (rec.type) {
        case LOG_TYPE.BEGIN:
          this.activeTxnTable.set(rec.txId, { status: 'active', lastLsn: rec.lsn });
          break;
          
        case LOG_TYPE.UPDATE:
        case LOG_TYPE.CLR:
          // Update ATT
          if (this.activeTxnTable.has(rec.txId)) {
            this.activeTxnTable.get(rec.txId).lastLsn = rec.lsn;
          } else {
            this.activeTxnTable.set(rec.txId, { status: 'active', lastLsn: rec.lsn });
          }
          // Update DPT: if page not already dirty, add it
          if (rec.pageId !== null && !this.dirtyPageTable.has(rec.pageId)) {
            this.dirtyPageTable.set(rec.pageId, rec.lsn);
          }
          break;
          
        case LOG_TYPE.COMMIT:
          if (this.activeTxnTable.has(rec.txId)) {
            this.activeTxnTable.get(rec.txId).status = 'committed';
          }
          break;
          
        case LOG_TYPE.ABORT:
          if (this.activeTxnTable.has(rec.txId)) {
            this.activeTxnTable.get(rec.txId).status = 'aborting';
          }
          break;
          
        case LOG_TYPE.END:
          this.activeTxnTable.delete(rec.txId);
          break;
      }
    }
  }

  // ============================================================
  // Phase 2: REDO
  // ============================================================
  
  /**
   * Redo all changes from the earliest recLsn in the DPT.
   * Idempotent: safe to redo an already-applied change.
   */
  _redo() {
    if (this.dirtyPageTable.size === 0) return;
    
    // Find the smallest recLsn in DPT
    let minRecLsn = Infinity;
    for (const recLsn of this.dirtyPageTable.values()) {
      if (recLsn < minRecLsn) minRecLsn = recLsn;
    }
    
    const records = this.wal.recordsFrom(minRecLsn);
    
    for (const rec of records) {
      if (rec.type !== LOG_TYPE.UPDATE && rec.type !== LOG_TYPE.CLR) continue;
      if (rec.pageId === null) continue;
      
      // Skip if page not in DPT
      if (!this.dirtyPageTable.has(rec.pageId)) continue;
      
      // Skip if record is older than when page became dirty
      const recLsn = this.dirtyPageTable.get(rec.pageId);
      if (rec.lsn < recLsn) continue;
      
      // Skip if page already has this change (pageLSN >= rec.lsn)
      const pageLsn = this.pageStore.getPageLsn(rec.pageId);
      if (pageLsn >= rec.lsn) continue;
      
      // Apply redo
      this.pageStore.applyRedo(rec.pageId, rec.after, rec.lsn);
      this.stats.redoRecords++;
    }
  }

  // ============================================================
  // Phase 3: UNDO
  // ============================================================
  
  /**
   * Undo all changes by transactions that were active at crash time.
   * Writes CLR (Compensation Log Records) to prevent re-undoing on a second crash.
   */
  _undo() {
    // Collect all active (uncommitted) transactions
    const toUndo = [];
    for (const [txId, info] of this.activeTxnTable) {
      if (info.status === 'active' || info.status === 'aborting') {
        toUndo.push({ txId, lastLsn: info.lastLsn });
      }
    }
    
    if (toUndo.length === 0) return;
    
    // Build a priority queue of LSNs to undo (process highest LSN first)
    // Use a simple sorted array since N is typically small
    let undoList = toUndo.map(t => ({ txId: t.txId, lsn: t.lastLsn }));
    
    while (undoList.length > 0) {
      // Pick the highest LSN to undo
      undoList.sort((a, b) => b.lsn - a.lsn);
      const { txId, lsn } = undoList.shift();
      
      // Find this log record
      const rec = this.wal.records.find(r => r.lsn === lsn);
      if (!rec) continue;
      
      if (rec.type === LOG_TYPE.UPDATE) {
        // Undo this update
        if (rec.pageId !== null && rec.before !== null) {
          this.pageStore.applyUndo(rec.pageId, rec.before);
        }
        
        // Write CLR (compensation log record)
        const clrLsn = this.wal.append({
          type: LOG_TYPE.CLR,
          txId,
          pageId: rec.pageId,
          after: rec.before,  // The CLR's "after" is the undo (before-image)
          undoNextLsn: rec.prevLsn, // Skip to the previous record in undo chain
        });
        this.stats.clrsWritten++;
        this.stats.undoRecords++;
        
        // Continue undoing: next record is prevLsn
        if (rec.prevLsn) {
          undoList.push({ txId, lsn: rec.prevLsn });
        } else {
          // No more records to undo — write END record
          this.wal.append({ type: LOG_TYPE.END, txId });
          this.activeTxnTable.delete(txId);
        }
      } else if (rec.type === LOG_TYPE.CLR) {
        // CLR: skip to undoNextLsn
        if (rec.undoNextLsn) {
          undoList.push({ txId, lsn: rec.undoNextLsn });
        } else {
          this.wal.append({ type: LOG_TYPE.END, txId });
          this.activeTxnTable.delete(txId);
        }
      } else if (rec.type === LOG_TYPE.BEGIN) {
        // Nothing to undo — write END
        this.wal.append({ type: LOG_TYPE.END, txId });
        this.activeTxnTable.delete(txId);
      } else {
        // Skip non-undoable records, continue with prevLsn
        if (rec.prevLsn) {
          undoList.push({ txId, lsn: rec.prevLsn });
        }
      }
    }
  }
}

/**
 * Simple in-memory page store for testing.
 */
export class InMemoryPageStore {
  constructor() {
    this._pages = new Map(); // pageId → {data, lsn}
  }

  getPageLsn(pageId) {
    const page = this._pages.get(pageId);
    return page ? page.lsn : 0;
  }

  applyRedo(pageId, afterImage, lsn) {
    this._pages.set(pageId, { data: afterImage, lsn });
  }

  applyUndo(pageId, beforeImage) {
    const page = this._pages.get(pageId);
    if (page) page.data = beforeImage;
    else this._pages.set(pageId, { data: beforeImage, lsn: 0 });
  }

  getData(pageId) {
    return this._pages.get(pageId)?.data ?? null;
  }
}
export { ARIESRecovery as AriesRecoveryManager };
