// mvcc.js — Multi-Version Concurrency Control
// Each row maintains a version chain. Transactions see a snapshot.
// Used in PostgreSQL, MySQL InnoDB, Oracle.

/**
 * MVCCTransaction — represents an active transaction with its state.
 * This is the object returned by MVCCManager.begin().
 */
export class MVCCTransaction {
  constructor(txId, manager, snapshot) {
    this.txId = txId;
    this.startTx = txId;
    this.writeSet = new Set();    // Track written keys (for rollback/WAL)
    this.undoLog = [];            // Undo functions for rollback
    this.manager = manager;       // Back-reference to MVCCManager
    this.committed = false;
    this.commitTxId = 0;          // The "commit timestamp" (nextTx at commit time)
    this.isolationLevel = 'REPEATABLE READ'; // Default like PostgreSQL
    this.suppressReadTracking = false; // SSI: suppress reads during scan-then-filter
    // PostgreSQL-style snapshot: {xmin, xmax, activeSet}
    this.snapshot = snapshot || { xmin: txId, xmax: txId, activeSet: new Set() };
  }

  commit() { this.manager.commit(this); }
  rollback() { this.manager.rollback(this); }
  
  /**
   * Refresh snapshot for READ COMMITTED isolation.
   * Takes a new snapshot reflecting the current state of transactions.
   * In REPEATABLE READ, this is a no-op (snapshot is frozen at BEGIN).
   */
  refreshSnapshot() {
    if (this.isolationLevel !== 'READ COMMITTED') return;
    this.snapshot = this.manager._takeSnapshot(this.txId);
  }
}

/**
 * MVCCManager — manages multi-version concurrency control.
 * begin() returns MVCCTransaction objects (not raw numbers).
 */
export class MVCCManager {
  constructor() {
    this._versions = new Map();   // key → [{value, txId, deleted}]
    this._nextTx = 1;
    this.activeTxns = new Map();  // txId → MVCCTransaction
    this.committedTxns = new Set(); // Track committed txIds for vacuum
    this.wal = [];                // Write-ahead log entries
  }

  get nextTxId() { return this._nextTx; }
  set nextTxId(v) { this._nextTx = v; }

  /** Begin a new transaction. Returns MVCCTransaction with snapshot. Options: {isolationLevel} */
  begin(options = {}) {
    const txId = this._nextTx++;
    const snapshot = this._takeSnapshot(txId);
    const tx = new MVCCTransaction(txId, this, snapshot);
    if (options.isolationLevel) tx.isolationLevel = options.isolationLevel;
    this.activeTxns.set(txId, tx);
    this.wal.push({ type: 'begin', txId });
    return tx;
  }

  /** Take a snapshot of current active transactions (PostgreSQL-style xmin:xmax:xip_list). */
  _takeSnapshot(myTxId) {
    const activeSet = new Set();
    const xmax = this._nextTx; // First unassigned txid
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

  /** Read a key at a transaction's snapshot. Accepts txId (number) or MVCCTransaction. */
  read(txIdOrTx, key) {
    const txId = typeof txIdOrTx === 'object' ? txIdOrTx.txId : txIdOrTx;
    const tx = this.activeTxns.get(txId);
    if (!tx) return undefined;

    const versions = this._versions.get(key);
    if (!versions) return undefined;

    // Find the latest version visible to this transaction's snapshot
    for (let i = versions.length - 1; i >= 0; i--) {
      const v = versions[i];
      
      // Own writes are always visible
      if (v.txId === txId) {
        this.recordRead(txId, key, v.txId);
        return v.deleted ? undefined : v.value;
      }
      
      // Check snapshot visibility: the writing transaction must have been
      // committed before our snapshot was taken
      if (this._isVisibleToSnapshot(v.txId, tx.snapshot)) {
        this.recordRead(txId, key, v.txId);
        return v.deleted ? undefined : v.value;
      }
    }
    return undefined;
  }

  /**
   * Check if a transaction's writes are visible to a given snapshot.
   * A transaction is visible if it committed before the snapshot was taken:
   * - txId < snapshot.xmin (committed before any active tx)
   * - txId < snapshot.xmax AND txId not in activeSet (committed, not active at snapshot time)
   */
  _isVisibleToSnapshot(writerTxId, snapshot) {
    // If the writer's txId is before the oldest active tx, it was committed
    if (writerTxId < snapshot.xmin) return true;
    // If >= xmax, it started after our snapshot — not visible
    if (writerTxId >= snapshot.xmax) return false;
    // Between xmin and xmax: visible if NOT in the active set at snapshot time
    return !snapshot.activeSet.has(writerTxId);
  }

  /** Write a key in a transaction. Accepts txId or MVCCTransaction. */
  write(txIdOrTx, key, value) {
    const txId = typeof txIdOrTx === 'object' ? txIdOrTx.txId : txIdOrTx;
    const tx = this.activeTxns.get(txId);
    
    // Write-write conflict detection:
    // If another concurrent transaction has written to this key and committed
    // AFTER our snapshot was taken, we have a conflict (first-committer-wins).
    // Versions committed BEFORE our snapshot are visible to us and can be overwritten.
    const versions = this._versions.get(key);
    if (versions && tx?.snapshot) {
      for (let i = versions.length - 1; i >= 0; i--) {
        const v = versions[i];
        if (v.txId === txId) break; // Own previous write, OK
        // Only check versions from transactions that were concurrent with us
        // A tx is concurrent if: (a) its txId >= our xmax (started after snapshot),
        // or (b) its txId is in our activeSet (was active at snapshot time)
        const isConcurrent = v.txId >= tx.snapshot.xmax || tx.snapshot.activeSet.has(v.txId);
        if (!isConcurrent) continue; // Committed before our snapshot — safe to overwrite
        
        const writerTx = this.activeTxns.get(v.txId);
        if (writerTx && writerTx.committed && v.txId !== txId) {
          throw new Error(`Write-write conflict on key "${key}": tx ${txId} conflicts with committed tx ${v.txId}`);
        }
        // If writer is still active (not committed, not our tx), also conflict
        if (writerTx && !writerTx.committed && v.txId !== txId) {
          throw new Error(`Write-write conflict on key "${key}": tx ${txId} conflicts with active tx ${v.txId}`);
        }
      }
    }
    
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value, txId, deleted: false });
    if (tx) tx.writeSet.add(key);
    this.recordWrite(txId, key);
    this.wal.push({ type: 'write', txId, key, value });
  }

  /** Delete a key in a transaction. Accepts txId or MVCCTransaction. */
  delete(txIdOrTx, key) {
    const txId = typeof txIdOrTx === 'object' ? txIdOrTx.txId : txIdOrTx;
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value: undefined, txId, deleted: true });
    const tx = this.activeTxns.get(txId);
    if (tx) tx.writeSet.add(key);
    this.wal.push({ type: 'delete', txId, key });
  }

  /** Commit a transaction. Accepts txId or MVCCTransaction. */
  commit(txIdOrTx) {
    const txId = typeof txIdOrTx === 'object' ? txIdOrTx.txId : txIdOrTx;
    const tx = this.activeTxns.get(txId);
    if (tx) {
      tx.committed = true;
      tx.commitTxId = this._nextTx; // Record commit "timestamp"
    }
    this.committedTxns.add(txId);
    this.wal.push({ type: 'commit', txId });
  }

  /** Compute the minimum xmin horizon — the lowest startTx of any active transaction. */
  computeXminHorizon() {
    if (this.activeTxns.size === 0) return this._nextTx;
    let min = Infinity;
    for (const tx of this.activeTxns.values()) {
      if (!tx.committed && tx.startTx < min) min = tx.startTx;
    }
    return min === Infinity ? this._nextTx : min;
  }

  /** Rollback: remove all versions written by this transaction. */
  rollback(txIdOrTx) {
    const txId = typeof txIdOrTx === 'object' ? txIdOrTx.txId : txIdOrTx;
    const tx = this.activeTxns.get(txId);
    if (!tx) return;
    for (const key of tx.writeSet) {
      const versions = this._versions.get(key);
      if (versions) {
        const filtered = versions.filter(v => v.txId !== txId);
        this._versions.set(key, filtered);
      }
    }
    // Also run undo log
    for (let i = tx.undoLog.length - 1; i >= 0; i--) {
      try { tx.undoLog[i](); } catch (e) { /* ignore */ }
    }
    this.activeTxns.delete(txId);
    this.wal.push({ type: 'rollback', txId });
  }

  /** Record a read (for SSI — no-op in basic MVCC). */
  recordRead(txId, key, readVersion) {
    // No-op in snapshot isolation. SSIManager overrides this.
  }

  /** Record a write (for SSI — no-op in basic MVCC). */
  recordWrite(txId, key) {
    // No-op in snapshot isolation. SSIManager overrides this.
  }

  /**
   * Check if a transaction's writes are visible to the given transaction.
   * Uses PostgreSQL-style snapshot for proper visibility:
   * - Own writes are always visible
   * - txId < snapshot.xmin: always visible (committed before any active txn)
   * - txId >= snapshot.xmax: always invisible (started after our snapshot)
   * - txId in snapshot.activeSet: invisible (was in-progress at snapshot time)
   * - Otherwise: visible (committed between xmin and xmax, not in activeSet)
   */
  isVisible(txId, tx) {
    if (txId === 0) return false;
    if (txId === tx.txId) return true;
    
    const snap = tx.snapshot;
    
    // Below xmin: was committed (or aborted) before our snapshot
    if (txId < snap.xmin) {
      // But check it wasn't aborted
      if (this.committedTxns.has(txId)) return true;
      const writerTx = this.activeTxns.get(txId);
      if (writerTx && writerTx.committed) return true;
      // txId < xmin but not committed = was aborted
      return false;
    }
    
    // At or above xmax: started after our snapshot, invisible
    if (txId >= snap.xmax) return false;
    
    // Between xmin and xmax: check if it was active at snapshot time
    if (snap.activeSet.has(txId)) {
      // Was in-progress when we took our snapshot → invisible
      // Even if it has since committed
      return false;
    }
    
    // Between xmin and xmax but NOT in activeSet → was committed at snapshot time
    // Verify it's actually committed (not aborted)
    if (this.committedTxns.has(txId)) return true;
    const writerTx = this.activeTxns.get(txId);
    if (writerTx && writerTx.committed) return true;
    
    return false;
  }

  /** Garbage collect: remove old versions not needed by any active transaction.
   * Keeps the latest version of each key and any version visible to active transactions.
   * Returns { cleaned, remaining } stats.
   */
  gc() {
    // Find minimum txId among uncommitted active transactions
    let minActive = Infinity;
    for (const tx of this.activeTxns.values()) {
      if (!tx.committed && tx.txId < minActive) minActive = tx.txId;
    }
    let cleaned = 0;
    let remaining = 0;
    
    for (const [key, versions] of this._versions) {
      if (versions.length <= 1) {
        remaining += versions.length;
        continue; // Nothing to clean if only 0-1 versions
      }
      
      // Keep: (1) latest version, (2) any version visible to active transactions
      const latest = versions[versions.length - 1];
      const visible = versions.filter((v, i) => {
        // Always keep the latest version
        if (i === versions.length - 1) return true;
        // Keep if visible to any active transaction (txId >= v.txId and v.txId >= minActive)
        if (v.txId >= minActive) return true;
        // Keep if it's the most recent version before minActive
        // (the "floor" version that active transactions would see)
        const nextVersion = versions[i + 1];
        if (nextVersion && nextVersion.txId >= minActive) return true;
        return false;
      });
      
      cleaned += versions.length - visible.length;
      remaining += visible.length;
      this._versions.set(key, visible);
    }
    
    return { cleaned, remaining };
  }

  /**
   * Full vacuum: remove all old versions. Only safe when no active transactions.
   * Returns { cleaned, keysRemoved } stats.
   */
  vacuum() {
    // Check for truly active (uncommitted) transactions
    for (const tx of this.activeTxns.values()) {
      if (!tx.committed) {
        throw new Error('VACUUM cannot run while transactions are active');
      }
    }
    
    let cleaned = 0;
    let keysRemoved = 0;
    
    for (const [key, versions] of this._versions) {
      if (versions.length > 1) {
        cleaned += versions.length - 1;
        // Keep only the latest version
        this._versions.set(key, [versions[versions.length - 1]]);
      }
      
      // Remove keys where the latest version is a delete
      const latest = versions[versions.length - 1];
      if (latest && latest.deleted) {
        this._versions.delete(key);
        keysRemoved++;
        cleaned++; // Also count the deleted marker
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
    };
  }
}

// Backward compatibility aliases
export { MVCCManager as MVCCStore };

/**
 * MVCCHeap — wraps a HeapFile with MVCC visibility rules.
 * Each row gets version metadata (xmin, xmax) for snapshot isolation.
 */
export class MVCCHeap {
  constructor(heap) {
    this._heap = heap;
    this._versions = new Map(); // "pageId:slotIdx" → {xmin, xmax}
  }

  /** Insert a row, tracked by the given transaction. */
  insert(values, tx) {
    const { pageId, slotIdx } = this._heap.insert(values);
    const key = `${pageId}:${slotIdx}`;
    // Hint bits: xminCommitted/xmaxCommitted cache commit status to avoid repeated lookups
    this._versions.set(key, { xmin: tx.txId, xmax: 0, xminCommitted: false, xmaxCommitted: false });
    tx.writeSet.add(key);
    if (tx.manager) tx.manager.wal.push({ type: 'heap-insert', txId: tx.txId, key, values });
    return { pageId, slotIdx };
  }

  /** Scan rows visible to the given transaction (snapshot isolation). */
  *scan(tx) {
    for (const row of this._heap.scan()) {
      const key = `${row.pageId}:${row.slotIdx}`;
      const ver = this._versions.get(key);
      if (!ver) { yield row; continue; }

      const created = this._isVisibleWithHints(ver, 'xmin', tx);
      const deleted = ver.xmax !== 0 && this._isVisibleWithHints(ver, 'xmax', tx);

      if (created && !deleted) {
        yield row;
      }
    }
  }

  /** Update a row: mark old as deleted, insert new. Detects write-write conflicts. */
  update(pageId, slotIdx, newValues, tx) {
    const key = `${pageId}:${slotIdx}`;
    const ver = this._versions.get(key);
    if (ver) {
      // Write-write conflict: another uncommitted txn already modified this row
      if (ver.xmax !== 0 && ver.xmax !== tx.txId) {
        const otherTx = tx.manager && tx.manager.activeTxns.get(ver.xmax);
        if (otherTx && !otherTx.committed) {
          throw new Error(`Write-write conflict: row ${key} already modified by tx ${ver.xmax}`);
        }
      }
      ver.xmax = tx.txId;
      tx.writeSet.add(`${key}:del`);
    }
    return this.insert(newValues, tx);
  }

  /** Delete a row: set xmax. Detects write-write conflicts. */
  delete(pageId, slotIdx, tx) {
    const key = `${pageId}:${slotIdx}`;
    const ver = this._versions.get(key);
    if (ver) {
      // Write-write conflict detection
      if (ver.xmax !== 0 && ver.xmax !== tx.txId) {
        const otherTx = tx.manager && tx.manager.activeTxns.get(ver.xmax);
        if (otherTx && !otherTx.committed) {
          throw new Error(`Write-write conflict: row ${key} already deleted by tx ${ver.xmax}`);
        }
      }
      ver.xmax = tx.txId;
      tx.writeSet.add(`${key}:del`);
    }
  }

  /**
   * VACUUM: remove dead tuples not needed by any active transaction.
   * A tuple is dead if xmax is set and committed and below the xmin horizon.
   */
  vacuum(mgr) {
    const horizon = mgr.computeXminHorizon();
    let deadTuplesRemoved = 0;
    const toRemove = [];

    for (const [key, ver] of this._versions) {
      if (ver.xmax === 0) continue;
      // Only reclaim if the deleting txn committed
      if (!mgr.committedTxns.has(ver.xmax)) {
        const delTx = mgr.activeTxns.get(ver.xmax);
        if (!delTx || !delTx.committed) continue;
      }
      // Only reclaim if no active snapshot can see this version
      if (ver.xmax < horizon) {
        toRemove.push(key);
        deadTuplesRemoved++;
      }
    }

    for (const key of toRemove) {
      this._versions.delete(key);
      // Optionally: physically delete from heap
      const [pageId, slotIdx] = key.split(':').map(Number);
      try { this._heap.delete(pageId, slotIdx); } catch (e) { /* ignore */ }
    }

    return { deadTuplesRemoved };
  }

  /**
   * Visibility check with hint bit optimization.
   * If the hint bit is set, skip the expensive isVisible() lookup.
   * Sets the hint bit on first determination (lazy — like PostgreSQL).
   * @param {object} ver - Version entry with hint bits
   * @param {'xmin'|'xmax'} field - Which field to check
   * @param {MVCCTransaction} tx - The reading transaction
   */
  _isVisibleWithHints(ver, field, tx) {
    const txId = ver[field];
    if (txId === 0) return false;
    if (txId === tx.txId) return true;
    
    const hintKey = field + 'Committed';
    
    // Fast path: hint bit already set → txId is definitely committed
    if (ver[hintKey]) {
      // Still need to check snapshot — committed doesn't mean visible
      return this._isVisibleToSnapshot(txId, tx);
    }
    
    // Slow path: check commit status
    const mgr = tx.manager;
    if (!mgr) return txId < tx.startTx; // fallback
    
    const result = mgr.isVisible(txId, tx);
    
    // Set hint bit if we determined the txId is committed
    // (Only set if it's actually committed, not just visible-to-this-snapshot)
    if (mgr.committedTxns.has(txId)) {
      ver[hintKey] = true;
    } else {
      const writerTx = mgr.activeTxns.get(txId);
      if (writerTx && writerTx.committed) {
        ver[hintKey] = true;
      }
    }
    
    return result;
  }
  
  /**
   * Check if a committed txId is visible to this transaction's snapshot.
   * Used by hint-bit fast path (already know it's committed).
   */
  _isVisibleToSnapshot(txId, tx) {
    const snap = tx.snapshot;
    if (txId < snap.xmin) return true; // committed before any active txn
    if (txId >= snap.xmax) return false; // started after snapshot
    if (snap.activeSet.has(txId)) return false; // was in-progress at snapshot time
    return true; // committed between xmin and xmax, not in active set
  }

  /** Visibility check for snapshot isolation (no hint bits). */
  _isVisible(txId, tx) {
    if (txId === 0) return false;
    if (txId === tx.txId) return true;
    const mgr = tx.manager;
    if (mgr) return mgr.isVisible(txId, tx);
    return txId < tx.startTx;
  }
}
