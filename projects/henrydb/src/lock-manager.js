// lock-manager.js — Lock Manager with compatibility matrix
// Supports: S (shared), X (exclusive), IS (intention shared), IX (intention exclusive), SIX
// Compatibility matrix determines which lock modes can coexist.
// Used for multigranularity locking (database → table → page → row).

const MODES = { S: 'S', X: 'X', IS: 'IS', IX: 'IX', SIX: 'SIX' };

// Compatibility matrix: can lock mode A be granted if mode B is held?
const COMPATIBLE = {
  //       IS    IX    S     SIX   X
  IS:  { IS: true,  IX: true,  S: true,  SIX: true,  X: false },
  IX:  { IS: true,  IX: true,  S: false, SIX: false, X: false },
  S:   { IS: true,  IX: false, S: true,  SIX: false, X: false },
  SIX: { IS: true,  IX: false, S: false, SIX: false, X: false },
  X:   { IS: false, IX: false, S: false, SIX: false, X: false },
};

/**
 * LockManager — multigranularity lock manager.
 */
export class LockManager {
  constructor() {
    // resourceId → { holders: Map<txnId, mode>, waitQueue: [{txnId, mode, resolve, reject}] }
    this._locks = new Map();
    this.stats = { grants: 0, waits: 0, upgrades: 0, releases: 0, deadlockAborts: 0 };
  }

  /**
   * Acquire a lock. Returns immediately if compatible, otherwise waits.
   * 
   * @param {string} txnId
   * @param {string} resourceId
   * @param {string} mode - 'S' | 'X' | 'IS' | 'IX' | 'SIX'
   * @returns {Promise<boolean>} true if granted
   */
  async acquire(txnId, resourceId, mode) {
    if (!COMPATIBLE[mode]) throw new Error(`Invalid lock mode: ${mode}`);

    if (!this._locks.has(resourceId)) {
      this._locks.set(resourceId, { holders: new Map(), waitQueue: [] });
    }

    const lock = this._locks.get(resourceId);

    // Already hold this or stronger lock?
    if (lock.holders.has(txnId)) {
      const currentMode = lock.holders.get(txnId);
      if (currentMode === mode || this._isStronger(currentMode, mode)) {
        return true; // Already have sufficient lock
      }
      // Upgrade attempt
      if (this._canGrant(lock, txnId, mode)) {
        lock.holders.set(txnId, mode);
        this.stats.upgrades++;
        return true;
      }
    }

    // Can we grant immediately?
    if (this._canGrant(lock, txnId, mode)) {
      lock.holders.set(txnId, mode);
      this.stats.grants++;
      return true;
    }

    // Must wait
    this.stats.waits++;
    return new Promise((resolve, reject) => {
      lock.waitQueue.push({ txnId, mode, resolve, reject });
    });
  }

  /**
   * Try to acquire without waiting. Returns true if granted, false otherwise.
   */
  tryAcquire(txnId, resourceId, mode) {
    if (!this._locks.has(resourceId)) {
      this._locks.set(resourceId, { holders: new Map(), waitQueue: [] });
    }

    const lock = this._locks.get(resourceId);

    if (lock.holders.has(txnId)) {
      const currentMode = lock.holders.get(txnId);
      if (currentMode === mode || this._isStronger(currentMode, mode)) return true;
    }

    if (this._canGrant(lock, txnId, mode)) {
      lock.holders.set(txnId, mode);
      this.stats.grants++;
      return true;
    }

    return false;
  }

  /**
   * Release a lock held by a transaction on a resource.
   */
  release(txnId, resourceId) {
    const lock = this._locks.get(resourceId);
    if (!lock || !lock.holders.has(txnId)) return false;

    lock.holders.delete(txnId);
    this.stats.releases++;

    // Try to grant waiting requests
    this._processWaitQueue(lock);

    // Clean up empty locks
    if (lock.holders.size === 0 && lock.waitQueue.length === 0) {
      this._locks.delete(resourceId);
    }

    return true;
  }

  /**
   * Release all locks held by a transaction.
   */
  releaseAll(txnId) {
    for (const [resourceId, lock] of this._locks) {
      if (lock.holders.has(txnId)) {
        this.release(txnId, resourceId);
      }
      // Remove from wait queues
      lock.waitQueue = lock.waitQueue.filter(w => {
        if (w.txnId === txnId) {
          w.reject(new Error('Transaction aborted'));
          return false;
        }
        return true;
      });
    }
  }

  /**
   * Check if a transaction holds a lock on a resource.
   */
  isHeldBy(txnId, resourceId) {
    const lock = this._locks.get(resourceId);
    return lock ? lock.holders.has(txnId) : false;
  }

  /**
   * Get the lock mode held by a transaction.
   */
  getLockMode(txnId, resourceId) {
    const lock = this._locks.get(resourceId);
    return lock ? lock.holders.get(txnId) : null;
  }

  _canGrant(lock, txnId, requestedMode) {
    for (const [holderId, heldMode] of lock.holders) {
      if (holderId === txnId) continue; // Skip self
      if (!COMPATIBLE[requestedMode][heldMode]) return false;
    }
    return true;
  }

  _processWaitQueue(lock) {
    const remaining = [];
    for (const waiter of lock.waitQueue) {
      if (this._canGrant(lock, waiter.txnId, waiter.mode)) {
        lock.holders.set(waiter.txnId, waiter.mode);
        this.stats.grants++;
        waiter.resolve(true);
      } else {
        remaining.push(waiter);
      }
    }
    lock.waitQueue = remaining;
  }

  _isStronger(held, requested) {
    const strength = { IS: 0, S: 1, IX: 2, SIX: 3, X: 4 };
    return strength[held] >= strength[requested];
  }

  getStats() {
    let totalHolders = 0, totalWaiters = 0;
    for (const lock of this._locks.values()) {
      totalHolders += lock.holders.size;
      totalWaiters += lock.waitQueue.length;
    }
    return { ...this.stats, activeResources: this._locks.size, totalHolders, totalWaiters };
  }
}

export { MODES, COMPATIBLE };

// Alias for tests expecting LockMode
const LockMode = { SHARED: 'S', EXCLUSIVE: 'X', INTENT_SHARED: 'IS', INTENT_EXCLUSIVE: 'IX', SHARED_INTENT_EXCLUSIVE: 'SIX', ...MODES };
export { LockMode };

// Add synchronous lock/unlock aliases to LockManager prototype
LockManager.prototype.lock = function(txnId, resourceId, mode) {
  if (!this._locks.has(resourceId)) {
    this._locks.set(resourceId, { holders: new Map(), waitQueue: [] });
  }
  const lock = this._locks.get(resourceId);
  if (lock.holders.has(txnId)) return true;
  if (this._canGrant(lock, txnId, mode)) {
    lock.holders.set(txnId, mode);
    return true;
  }
  return false;
};

LockManager.prototype.unlock = function(txnId, resourceId) {
  if (resourceId !== undefined) {
    this.release(txnId, resourceId);
  } else {
    this.releaseAll(txnId);
  }
};
