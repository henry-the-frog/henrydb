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

  /** Commit a transaction. Accepts tx object or txId. */
  commit(txOrId) {
    const tx = typeof txOrId === 'number' ? this.activeTxns.get(txOrId) : txOrId;
    if (!tx) throw new Error(`Transaction not found`);
    tx.committed = true;
    tx.commitTxId = this._nextTx;
    this.committedTxns.add(tx.txId);
  }

  /** Rollback: remove all versions written by this transaction. */
  rollback(txOrId) {
    const tx = typeof txOrId === 'number' ? this.activeTxns.get(txOrId) : txOrId;
    if (!tx) return; // Already cleaned up
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

  /** Check if a given txId's writes are visible to the given transaction. */
  isVisible(writerTxId, tx) {
    // Frozen tuples (txId 0) are always visible — used for recovered/pre-existing rows
    if (writerTxId === 0) return true;
    
    // Own writes are visible
    if (writerTxId === tx.txId) return true;
    
    const snap = tx.snapshot;
    
    // Must be committed
    const isCommitted = this.committedTxns.has(writerTxId) ||
      (this.activeTxns.get(writerTxId)?.committed ?? false);
    if (!isCommitted) return false;
    
    // Above or equal to xmax: started after snapshot → invisible
    if (writerTxId >= snap.xmax) return false;
    
    // In active set at snapshot time → invisible
    if (snap.activeSet.has(writerTxId)) return false;
    
    return true;
  }

  /** Compute the xmin horizon — the oldest txId that any active transaction might need. */
  computeXminHorizon() {
    let min = this._nextTx;
    for (const [id, tx] of this.activeTxns) {
      if (!tx.committed && id < min) min = id;
    }
    return min;
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

/**
 * MVCCHeap — MVCC-aware wrapper around HeapFile.
 * Provides transactional insert/delete/scan with visibility rules,
 * plus VACUUM garbage collection of dead tuples.
 */
export class MVCCHeap {
  constructor(heapFile) {
    this.heap = heapFile;
    // Track MVCC metadata per row: "pageId:slotIdx" → { xmin, xmax, deleted }
    // xmin = txId that created this row, xmax = txId that deleted it (0 = not deleted)
    this._rowMeta = new Map();
  }

  _key(pageId, slotIdx) { return `${pageId}:${slotIdx}`; }

  /** Insert a row within a transaction. Returns {pageId, slotIdx}. */
  insert(values, tx) {
    const rid = this.heap.insert(values);
    this._rowMeta.set(this._key(rid.pageId, rid.slotIdx), {
      xmin: tx.txId,
      xmax: 0,
      deleted: false,
      values  // Keep a copy for potential compaction
    });
    return rid;
  }

  /** Mark a row as deleted within a transaction. */
  delete(pageId, slotIdx, tx) {
    const key = this._key(pageId, slotIdx);
    const meta = this._rowMeta.get(key);
    if (!meta) throw new Error(`Row ${key} not found in MVCC metadata`);
    if (meta.xmax !== 0 && meta.xmax !== tx.txId) {
      throw new Error(`Row ${key} already deleted by tx ${meta.xmax}`);
    }
    meta.xmax = tx.txId;
    meta.deleted = true;
  }

  /** Scan rows visible to a transaction's snapshot. */
  *scan(tx) {
    for (const { pageId, slotIdx, values } of this.heap.scan()) {
      const key = this._key(pageId, slotIdx);
      const meta = this._rowMeta.get(key);
      if (!meta) continue; // No MVCC metadata — skip (shouldn't happen normally)
      if (this._isVisible(meta, tx)) {
        yield { pageId, slotIdx, values };
      }
    }
  }

  /** Check if a row is visible to a transaction based on xmin/xmax. */
  _isVisible(meta, tx) {
    const mgr = tx.manager;
    // Row created by this transaction and not deleted by it
    if (meta.xmin === tx.txId) {
      return !meta.deleted || meta.xmax !== tx.txId;
    }
    // xmin must be committed and visible in snapshot
    if (!this._isTxCommittedAndVisible(meta.xmin, tx, mgr)) return false;
    // If not deleted, it's visible
    if (meta.xmax === 0) return true;
    // If deleted by this tx, not visible
    if (meta.xmax === tx.txId) return false;
    // If deleter is committed and visible, row is not visible
    if (this._isTxCommittedAndVisible(meta.xmax, tx, mgr)) return false;
    return true;
  }

  _isTxCommittedAndVisible(txId, tx, mgr) {
    const snap = tx.snapshot;
    // Must be committed
    const isCommitted = mgr.committedTxns.has(txId) ||
      (mgr.activeTxns.get(txId)?.committed ?? false);
    if (!isCommitted) return false;
    // Must be visible in snapshot
    if (txId >= snap.xmax) return false;
    if (snap.activeSet.has(txId)) return false;
    return true;
  }

  /** VACUUM: remove dead tuples that no active transaction can see. */
  vacuum(mgr) {
    const horizon = mgr.computeXminHorizon();
    let deadTuplesRemoved = 0;
    let bytesFreed = 0;
    let pagesCompacted = 0;
    const pagesAffected = new Set();

    for (const [key, meta] of this._rowMeta) {
      // A tuple is dead if:
      // 1. It was deleted (xmax != 0)
      // 2. The deleter committed (xmax is in committedTxns)
      // 3. No active transaction can see it (xmax < horizon)
      if (meta.xmax === 0) continue; // Not deleted
      
      const deleterCommitted = mgr.committedTxns.has(meta.xmax) ||
        (mgr.activeTxns.get(meta.xmax)?.committed ?? false);
      if (!deleterCommitted) continue; // Deleter hasn't committed
      
      if (meta.xmax >= horizon) continue; // Active tx might still see it

      // Also check xmin < horizon (creator committed before horizon)
      const creatorCommitted = mgr.committedTxns.has(meta.xmin) ||
        (mgr.activeTxns.get(meta.xmin)?.committed ?? false);
      if (!creatorCommitted) continue;

      // Safe to remove this tuple
      const [pageIdStr, slotIdxStr] = key.split(':');
      const pageId = parseInt(pageIdStr, 10);
      const slotIdx = parseInt(slotIdxStr, 10);
      
      // Delete from physical heap
      const deleted = this.heap.delete(pageId, slotIdx);
      if (deleted) {
        deadTuplesRemoved++;
        bytesFreed += meta.values ? JSON.stringify(meta.values).length : 32;
        pagesAffected.add(pageId);
      }
      // Remove MVCC metadata
      this._rowMeta.delete(key);
    }

    return {
      deadTuplesRemoved,
      bytesFreed,
      pagesCompacted: pagesAffected.size
    };
  }

  get pageCount() { return this.heap.pageCount; }
}
