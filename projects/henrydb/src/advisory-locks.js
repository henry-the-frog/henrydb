// advisory-locks.js — PostgreSQL-compatible advisory locks for HenryDB
// Session-level and transaction-level advisory locks.
// pg_advisory_lock, pg_try_advisory_lock, pg_advisory_unlock

/**
 * AdvisoryLockManager — manages cooperative application-level locks.
 * 
 * Lock types:
 * - Session-level: held until explicitly released or session ends
 * - Transaction-level: released automatically when transaction ends
 * 
 * Lock modes:
 * - Exclusive: only one holder at a time
 * - Shared: multiple readers, no writers
 */
export class AdvisoryLockManager {
  constructor() {
    // lockKey → { mode, holders: Map<sessionId, { count, level }> }
    this._locks = new Map();
    // sessionId → Set<lockKey> — for session cleanup
    this._sessionLocks = new Map();
    this._waitQueue = new Map(); // lockKey → [{sessionId, mode, resolve, reject}]
    this._stats = {
      acquired: 0,
      released: 0,
      tryFailed: 0,
      deadlockDetected: 0,
    };
  }

  /**
   * Acquire an advisory lock (blocking).
   * Returns true when acquired. Blocks if lock is held incompatibly.
   */
  async lock(sessionId, key, options = {}) {
    const mode = options.mode || 'exclusive';
    const level = options.level || 'session';
    const lockKey = this._normalizeKey(key);

    if (this._tryAcquire(sessionId, lockKey, mode, level)) {
      return true;
    }

    // Wait for lock
    return new Promise((resolve, reject) => {
      if (!this._waitQueue.has(lockKey)) {
        this._waitQueue.set(lockKey, []);
      }
      const timeoutMs = options.timeoutMs || 30000;
      const timer = setTimeout(() => {
        // Remove from wait queue
        const queue = this._waitQueue.get(lockKey);
        if (queue) {
          const idx = queue.findIndex(w => w.sessionId === sessionId);
          if (idx >= 0) queue.splice(idx, 1);
        }
        reject(new Error(`Advisory lock timeout on key ${lockKey}`));
      }, timeoutMs);

      this._waitQueue.get(lockKey).push({
        sessionId, mode, level,
        resolve: () => { clearTimeout(timer); resolve(true); },
        reject: (err) => { clearTimeout(timer); reject(err); },
      });
    });
  }

  /**
   * Try to acquire a lock without blocking.
   * Returns true if acquired, false if would block.
   */
  tryLock(sessionId, key, options = {}) {
    const mode = options.mode || 'exclusive';
    const level = options.level || 'session';
    const lockKey = this._normalizeKey(key);

    const acquired = this._tryAcquire(sessionId, lockKey, mode, level);
    if (!acquired) {
      this._stats.tryFailed++;
    }
    return acquired;
  }

  /**
   * Release an advisory lock.
   * Returns true if the lock was held and released.
   */
  unlock(sessionId, key) {
    const lockKey = this._normalizeKey(key);
    return this._release(sessionId, lockKey);
  }

  /**
   * Release all session-level locks for a session (disconnect cleanup).
   */
  releaseSession(sessionId) {
    const locks = this._sessionLocks.get(sessionId);
    if (!locks) return 0;

    let released = 0;
    for (const lockKey of [...locks]) {
      if (this._release(sessionId, lockKey)) released++;
    }
    this._sessionLocks.delete(sessionId);
    return released;
  }

  /**
   * Release all transaction-level locks for a session (COMMIT/ROLLBACK).
   */
  releaseTransaction(sessionId) {
    const locks = this._sessionLocks.get(sessionId);
    if (!locks) return 0;

    let released = 0;
    for (const lockKey of [...locks]) {
      const lock = this._locks.get(lockKey);
      if (!lock) continue;
      const holder = lock.holders.get(sessionId);
      if (holder && holder.level === 'transaction') {
        if (this._release(sessionId, lockKey)) released++;
      }
    }
    return released;
  }

  /**
   * Check if a lock is held by any session.
   */
  isLocked(key) {
    const lockKey = this._normalizeKey(key);
    const lock = this._locks.get(lockKey);
    return lock ? lock.holders.size > 0 : false;
  }

  /**
   * Check if a specific session holds a lock.
   */
  isLockedBy(sessionId, key) {
    const lockKey = this._normalizeKey(key);
    const lock = this._locks.get(lockKey);
    return lock ? lock.holders.has(sessionId) : false;
  }

  /**
   * List all active locks.
   */
  listLocks() {
    const result = [];
    for (const [key, lock] of this._locks) {
      for (const [sessionId, info] of lock.holders) {
        result.push({
          key,
          sessionId,
          mode: lock.mode,
          level: info.level,
          count: info.count,
        });
      }
    }
    return result;
  }

  getStats() {
    return {
      ...this._stats,
      activeLocks: this._locks.size,
      waitingRequests: [...this._waitQueue.values()].reduce((s, q) => s + q.length, 0),
    };
  }

  // --- Internal ---

  _tryAcquire(sessionId, lockKey, mode, level) {
    const lock = this._locks.get(lockKey);

    if (!lock) {
      // No lock exists — create it
      this._locks.set(lockKey, {
        mode,
        holders: new Map([[sessionId, { count: 1, level }]]),
      });
      this._trackSession(sessionId, lockKey);
      this._stats.acquired++;
      return true;
    }

    // Same session re-acquiring (recursive lock)
    if (lock.holders.has(sessionId)) {
      lock.holders.get(sessionId).count++;
      this._stats.acquired++;
      return true;
    }

    // Check compatibility
    if (mode === 'shared' && lock.mode === 'shared') {
      // Multiple shared holders OK
      lock.holders.set(sessionId, { count: 1, level });
      this._trackSession(sessionId, lockKey);
      this._stats.acquired++;
      return true;
    }

    // Exclusive lock requested or held — can't acquire
    return false;
  }

  _release(sessionId, lockKey) {
    const lock = this._locks.get(lockKey);
    if (!lock) return false;

    const holder = lock.holders.get(sessionId);
    if (!holder) return false;

    holder.count--;
    if (holder.count <= 0) {
      lock.holders.delete(sessionId);
      
      // Remove from session tracking
      const sessionLocks = this._sessionLocks.get(sessionId);
      if (sessionLocks) sessionLocks.delete(lockKey);
    }

    // Clean up empty lock
    if (lock.holders.size === 0) {
      this._locks.delete(lockKey);
    }

    this._stats.released++;

    // Wake up waiters
    this._processWaitQueue(lockKey);

    return true;
  }

  _processWaitQueue(lockKey) {
    const queue = this._waitQueue.get(lockKey);
    if (!queue || queue.length === 0) return;

    const waiter = queue[0];
    if (this._tryAcquire(waiter.sessionId, lockKey, waiter.mode, waiter.level)) {
      queue.shift();
      waiter.resolve();
      if (queue.length === 0) this._waitQueue.delete(lockKey);
    }
  }

  _trackSession(sessionId, lockKey) {
    if (!this._sessionLocks.has(sessionId)) {
      this._sessionLocks.set(sessionId, new Set());
    }
    this._sessionLocks.get(sessionId).add(lockKey);
  }

  _normalizeKey(key) {
    if (Array.isArray(key)) return key.join(':');
    return String(key);
  }
}
