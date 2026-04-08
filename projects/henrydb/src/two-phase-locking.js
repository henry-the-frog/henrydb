// two-phase-locking.js — Row-level 2PL (Two-Phase Locking)
// Growing phase: acquire locks, never release.
// Shrinking phase: release locks, never acquire.
// Guarantees serializability of concurrent transactions.

/**
 * TwoPhaseLocking — 2PL transaction manager.
 */
export class TwoPhaseLocking {
  constructor() {
    // Row lock table: `table:rowId` → { mode: 'S'|'X', holders: Set<txnId> }
    this._lockTable = new Map();
    // Per-txn state: txnId → { phase: 'growing'|'shrinking', locks: Set<resourceId> }
    this._txns = new Map();
    this.stats = { grants: 0, blocks: 0, aborts: 0 };
  }

  /**
   * Begin a transaction.
   */
  begin(txnId) {
    this._txns.set(txnId, { phase: 'growing', locks: new Set() });
  }

  /**
   * Acquire a shared (read) lock.
   */
  lockShared(txnId, table, rowId) {
    return this._acquire(txnId, `${table}:${rowId}`, 'S');
  }

  /**
   * Acquire an exclusive (write) lock.
   */
  lockExclusive(txnId, table, rowId) {
    return this._acquire(txnId, `${table}:${rowId}`, 'X');
  }

  _acquire(txnId, resourceId, mode) {
    const txn = this._txns.get(txnId);
    if (!txn) throw new Error(`Transaction ${txnId} not found`);
    if (txn.phase === 'shrinking') {
      throw new Error(`Cannot acquire lock in shrinking phase (2PL violation)`);
    }

    // Already hold this lock?
    if (txn.locks.has(resourceId)) {
      const lock = this._lockTable.get(resourceId);
      if (lock && lock.mode === 'X') return true; // Already have exclusive
      if (mode === 'S') return true; // Already have shared, want shared
      // Upgrade S→X
      if (lock.holders.size === 1 && lock.holders.has(txnId)) {
        lock.mode = 'X';
        return true;
      }
      this.stats.blocks++;
      return false; // Can't upgrade with other holders
    }

    // Check compatibility
    if (this._lockTable.has(resourceId)) {
      const lock = this._lockTable.get(resourceId);
      
      // Exclusive lock held by someone else
      if (lock.mode === 'X' && !lock.holders.has(txnId)) {
        this.stats.blocks++;
        return false;
      }
      
      // Requesting exclusive but shared held by others
      if (mode === 'X' && lock.holders.size > 0 && !lock.holders.has(txnId)) {
        this.stats.blocks++;
        return false;
      }

      // Compatible: add to holders
      if (mode === 'X') lock.mode = 'X';
      lock.holders.add(txnId);
    } else {
      this._lockTable.set(resourceId, { mode, holders: new Set([txnId]) });
    }

    txn.locks.add(resourceId);
    this.stats.grants++;
    return true;
  }

  /**
   * Commit: release all locks (shrinking phase).
   */
  commit(txnId) {
    this._releaseTxn(txnId);
  }

  /**
   * Abort: release all locks.
   */
  abort(txnId) {
    this._releaseTxn(txnId);
    this.stats.aborts++;
  }

  _releaseTxn(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn) return;

    txn.phase = 'shrinking';
    for (const resourceId of txn.locks) {
      const lock = this._lockTable.get(resourceId);
      if (lock) {
        lock.holders.delete(txnId);
        if (lock.holders.size === 0) this._lockTable.delete(resourceId);
      }
    }
    this._txns.delete(txnId);
  }

  /**
   * Check if a transaction holds a specific lock.
   */
  holdsLock(txnId, table, rowId) {
    const txn = this._txns.get(txnId);
    return txn ? txn.locks.has(`${table}:${rowId}`) : false;
  }

  /**
   * Get lock info for a resource.
   */
  getLockInfo(table, rowId) {
    return this._lockTable.get(`${table}:${rowId}`) || null;
  }

  getStats() {
    return {
      ...this.stats,
      activeTxns: this._txns.size,
      activeLocks: this._lockTable.size,
    };
  }
}
