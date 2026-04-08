// aries-recovery.js — ARIES-style recovery with undo/redo logging
// Algorithm for Recovery and Isolation Exploiting Semantics
// Three phases: (1) Analysis, (2) Redo, (3) Undo
// Each log entry records before/after images for undo/redo.

export class ARIESRecovery {
  constructor() {
    this._log = []; // [{lsn, txnId, type, table, key, before, after}]
    this._nextLSN = 1;
    this._txnTable = new Map(); // txnId → { status, lastLSN }
    this._dirtyPages = new Map(); // pageId → recoveryLSN (first dirty LSN)
    this._data = new Map(); // key → value (simulated database)
    this._checkpoints = []; // [{lsn, txnTable, dirtyPages}]
    this.stats = { redone: 0, undone: 0, analysisOps: 0 };
  }

  begin(txnId) {
    this._txnTable.set(txnId, { status: 'active', lastLSN: 0 });
    this._appendLog(txnId, 'BEGIN', null, null, null, null);
  }

  write(txnId, key, newValue) {
    const oldValue = this._data.get(key) ?? null;
    const lsn = this._appendLog(txnId, 'UPDATE', null, key, oldValue, newValue);
    this._data.set(key, newValue);
    this._dirtyPages.set(key, Math.min(this._dirtyPages.get(key) || lsn, lsn));
    return lsn;
  }

  commit(txnId) {
    this._appendLog(txnId, 'COMMIT', null, null, null, null);
    const txn = this._txnTable.get(txnId);
    if (txn) txn.status = 'committed';
  }

  abort(txnId) {
    // Undo all writes by this txn
    this._undoTransaction(txnId);
    this._appendLog(txnId, 'ABORT', null, null, null, null);
    const txn = this._txnTable.get(txnId);
    if (txn) txn.status = 'aborted';
  }

  checkpoint() {
    const cp = {
      lsn: this._nextLSN - 1,
      txnTable: new Map(this._txnTable),
      dirtyPages: new Map(this._dirtyPages),
    };
    this._checkpoints.push(cp);
    this._appendLog(null, 'CHECKPOINT', null, null, null, null);
    return cp.lsn;
  }

  /**
   * Simulate a crash and recovery.
   * Clears in-memory state and recovers from log.
   */
  crashAndRecover() {
    // Save log (persistent) but clear volatile state
    const savedLog = [...this._log];
    this._data.clear();
    this._txnTable.clear();
    this._dirtyPages.clear();

    // Find last checkpoint
    const lastCheckpoint = this._checkpoints.length > 0
      ? this._checkpoints[this._checkpoints.length - 1]
      : null;

    // Phase 1: Analysis
    const activeTxns = new Set();
    const startLSN = lastCheckpoint ? lastCheckpoint.lsn : 0;
    
    if (lastCheckpoint) {
      for (const [txnId, info] of lastCheckpoint.txnTable) {
        if (info.status === 'active') activeTxns.add(txnId);
        this._txnTable.set(txnId, { ...info });
      }
      for (const [key, lsn] of lastCheckpoint.dirtyPages) {
        this._dirtyPages.set(key, lsn);
      }
    }

    for (const entry of savedLog) {
      if (entry.lsn <= startLSN) continue;
      this.stats.analysisOps++;
      
      if (entry.type === 'BEGIN') activeTxns.add(entry.txnId);
      if (entry.type === 'COMMIT' || entry.type === 'ABORT') activeTxns.delete(entry.txnId);
      if (entry.txnId) {
        this._txnTable.set(entry.txnId, { status: 'active', lastLSN: entry.lsn });
      }
    }

    // Phase 2: Redo (replay all updates from log)
    for (const entry of savedLog) {
      if (entry.type === 'UPDATE' && entry.key !== null) {
        this._data.set(entry.key, entry.after);
        this.stats.redone++;
      }
    }

    // Phase 3: Undo (rollback uncommitted transactions)
    for (const txnId of activeTxns) {
      for (let i = savedLog.length - 1; i >= 0; i--) {
        const entry = savedLog[i];
        if (entry.txnId === txnId && entry.type === 'UPDATE') {
          this._data.set(entry.key, entry.before);
          this.stats.undone++;
        }
      }
    }

    this._log = savedLog;
    return { activeTxns: [...activeTxns], redone: this.stats.redone, undone: this.stats.undone };
  }

  _undoTransaction(txnId) {
    for (let i = this._log.length - 1; i >= 0; i--) {
      const entry = this._log[i];
      if (entry.txnId === txnId && entry.type === 'UPDATE') {
        this._data.set(entry.key, entry.before);
        this._appendLog(txnId, 'CLR', null, entry.key, null, entry.before); // Compensation
      }
    }
  }

  _appendLog(txnId, type, table, key, before, after) {
    const lsn = this._nextLSN++;
    this._log.push({ lsn, txnId, type, table, key, before, after });
    if (txnId && this._txnTable.has(txnId)) {
      this._txnTable.get(txnId).lastLSN = lsn;
    }
    return lsn;
  }

  getValue(key) { return this._data.get(key); }
  getLog() { return [...this._log]; }
  getStats() { return { ...this.stats, logSize: this._log.length }; }
}
