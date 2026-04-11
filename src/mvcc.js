// mvcc.js — Multi-Version Concurrency Control for HenryDB
// PostgreSQL-style snapshot isolation with version chains.
// Each key maintains a version chain. Transactions see consistent snapshots.

/**
 * MVCCTransaction — represents an active transaction with snapshot state.
 */
export class MVCCTransaction {
  constructor(txId, manager, snapshot) {
    this.txId = txId;
    this.startTx = txId;
    this.writeSet = new Set();    // Track written keys (for rollback)
    this.undoLog = [];            // Undo functions for rollback
    this.manager = manager;
    this.committed = false;
    this.commitTxId = 0;          // Commit "timestamp" (nextTx at commit time)
    this.isolationLevel = 'REPEATABLE READ'; // Default like PostgreSQL
    // PostgreSQL-style snapshot: {xmin, xmax, activeSet}
    this.snapshot = snapshot || { xmin: txId, xmax: txId, activeSet: new Set() };
  }

  commit() { this.manager.commit(this); }
  rollback() { this.manager.rollback(this); }
  
  /** Refresh snapshot for READ COMMITTED (no-op for REPEATABLE READ). */
  refreshSnapshot() {
    if (this.isolationLevel !== 'READ COMMITTED') return;
    this.snapshot = this.manager._takeSnapshot(this.txId);
  }
}

/**
 * MVCCManager — manages multi-version concurrency control.
 * Key-value level MVCC with version chains and snapshot isolation.
 */
export class MVCCManager {
  constructor() {
    this._versions = new Map();      // key → [{value, txId, deleted}]
    this._nextTx = 1;
    this.activeTxns = new Map();     // txId → MVCCTransaction
    this.committedTxns = new Set();  // Track committed txIds for visibility
  }

  get nextTxId() { return this._nextTx; }

  /** Begin a new transaction with a snapshot. */
  begin(options = {}) {
    const txId = this._nextTx++;
    const snapshot = this._takeSnapshot(txId);
    const tx = new MVCCTransaction(txId, this, snapshot);
    if (options.isolationLevel) tx.isolationLevel = options.isolationLevel;
    this.activeTxns.set(txId, tx);
    return tx;
  }

  /** Take a PostgreSQL-style snapshot (xmin:xmax:xip_list). */
  _takeSnapshot(myTxId) {
    const activeSet = new Set();
    const xmax = this._nextTx;
    let xmin = xmax;
    for (const [id, otherTx] of this.activeTxns) {
      if (id === myTxId) continue;
      if (!otherTx.committed) {
        activeSet.add(id);
        if (id < xmin) xmin = id;
      }
    }
    return { xmin, xmax, activeSet };
  }

  /** Read a key at a transaction's snapshot. */
  read(tx, key) {
    const versions = this._versions.get(key);
    if (!versions) return undefined;

    // Walk version chain from newest to oldest
    for (let i = versions.length - 1; i >= 0; i--) {
      const v = versions[i];
      if (this._isVersionVisible(v, tx)) {
        return v.deleted ? undefined : v.value;
      }
    }
    return undefined;
  }

  /** Write a key in a transaction. Detects write-write conflicts. */
  write(tx, key, value) {
    const versions = this._versions.get(key);
    if (versions) {
      // Check for write-write conflicts
      const latest = versions[versions.length - 1];
      if (latest && latest.txId !== tx.txId && !latest.deleted) {
        const writerTx = this.activeTxns.get(latest.txId);
        if (writerTx && !writerTx.committed) {
          throw new Error(`Write-write conflict: key "${key}" modified by active tx ${latest.txId}`);
        }
      }
    }
    
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value, txId: tx.txId, deleted: false });
    tx.writeSet.add(key);
  }

  /** Delete a key in a transaction. */
  delete(tx, key) {
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value: undefined, txId: tx.txId, deleted: true });
    tx.writeSet.add(key);
  }

  /** Commit a transaction. */
  commit(tx) {
    tx.committed = true;
    tx.commitTxId = this._nextTx;
    this.committedTxns.add(tx.txId);
  }

  /** Rollback: remove all versions written by this transaction. */
  rollback(tx) {
    for (const key of tx.writeSet) {
      const versions = this._versions.get(key);
      if (versions) {
        this._versions.set(key, versions.filter(v => v.txId !== tx.txId));
      }
    }
    for (let i = tx.undoLog.length - 1; i >= 0; i--) {
      try { tx.undoLog[i](); } catch (e) { /* ignore */ }
    }
    this.activeTxns.delete(tx.txId);
  }

  /** Check if a version is visible to a transaction using snapshot rules. */
  _isVersionVisible(version, tx) {
    const writerTxId = version.txId;
    
    // Own writes are always visible
    if (writerTxId === tx.txId) return true;
    
    const snap = tx.snapshot;
    
    // Below xmin: committed before any active transaction's snapshot
    if (writerTxId < snap.xmin) {
      return this.committedTxns.has(writerTxId) || 
             (this.activeTxns.get(writerTxId)?.committed ?? false);
    }
    
    // At or above xmax: started after our snapshot → invisible
    if (writerTxId >= snap.xmax) return false;
    
    // In the active set: was in-progress when snapshot was taken → invisible
    if (snap.activeSet.has(writerTxId)) return false;
    
    // Between xmin and xmax, not in active set: committed at snapshot time
    return this.committedTxns.has(writerTxId) || 
           (this.activeTxns.get(writerTxId)?.committed ?? false);
  }

  /** Scan all visible key-value pairs for a transaction. */
  *scan(tx) {
    for (const [key] of this._versions) {
      const value = this.read(tx, key);
      if (value !== undefined) {
        yield { key, value };
      }
    }
  }

  /** Garbage collect old versions not needed by any active transaction. */
  gc() {
    let minActive = Infinity;
    for (const tx of this.activeTxns.values()) {
      if (!tx.committed && tx.txId < minActive) minActive = tx.txId;
    }
    
    let cleaned = 0, remaining = 0;
    for (const [key, versions] of this._versions) {
      if (versions.length <= 1) { remaining += versions.length; continue; }
      
      const visible = versions.filter((v, i) => {
        if (i === versions.length - 1) return true;           // always keep latest
        if (v.txId >= minActive) return true;                  // needed by active txn
        const next = versions[i + 1];
        if (next && next.txId >= minActive) return true;       // floor version
        return false;
      });
      
      cleaned += versions.length - visible.length;
      remaining += visible.length;
      this._versions.set(key, visible);
    }
    
    return { cleaned, remaining };
  }

  /** Full vacuum: remove all old versions. Only safe with no active transactions. */
  vacuum() {
    for (const tx of this.activeTxns.values()) {
      if (!tx.committed) throw new Error('VACUUM cannot run while transactions are active');
    }
    
    let cleaned = 0, keysRemoved = 0;
    for (const [key, versions] of this._versions) {
      if (versions.length > 1) {
        cleaned += versions.length - 1;
        this._versions.set(key, [versions[versions.length - 1]]);
      }
      const latest = this._versions.get(key);
      if (latest && latest[latest.length - 1]?.deleted) {
        this._versions.delete(key);
        keysRemoved++;
      }
    }
    
    return { cleaned, keysRemoved };
  }

  getStats() {
    let totalVersions = 0;
    for (const [, versions] of this._versions) totalVersions += versions.length;
    return {
      keys: this._versions.size,
      totalVersions,
      activeTxns: this.activeTxns.size,
      committedTxns: this.committedTxns.size,
    };
  }
}
