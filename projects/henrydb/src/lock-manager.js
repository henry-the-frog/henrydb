// lock-manager.js — Two-Phase Locking with deadlock detection
// Manages shared (S) and exclusive (X) locks on resources.
// Deadlock detection via waits-for graph cycle detection.

const SHARED = 'S';
const EXCLUSIVE = 'X';

export class LockManager {
  constructor() {
    this._locks = new Map();    // resource → {holders: Set<txId>, mode, queue: []}
    this._txLocks = new Map();  // txId → Set<resource>
  }

  /** Acquire a lock. Returns true if granted, false if would deadlock. */
  acquire(txId, resource, mode = EXCLUSIVE) {
    if (!this._txLocks.has(txId)) this._txLocks.set(txId, new Set());
    
    let lock = this._locks.get(resource);
    if (!lock) {
      lock = { holders: new Set(), mode: null, queue: [] };
      this._locks.set(resource, lock);
    }

    // Already hold the lock?
    if (lock.holders.has(txId)) {
      if (mode === EXCLUSIVE && lock.mode === SHARED) {
        // Upgrade: only if we're the only holder
        if (lock.holders.size === 1) {
          lock.mode = EXCLUSIVE;
          return true;
        }
        return false; // Can't upgrade with other holders
      }
      return true;
    }

    // Can we grant immediately?
    if (lock.holders.size === 0) {
      lock.holders.add(txId);
      lock.mode = mode;
      this._txLocks.get(txId).add(resource);
      return true;
    }

    if (mode === SHARED && lock.mode === SHARED) {
      lock.holders.add(txId);
      this._txLocks.get(txId).add(resource);
      return true;
    }

    // Would block — check for deadlock
    if (this._wouldDeadlock(txId, resource)) return false;

    // Queue the request (in real impl, would block)
    lock.queue.push({ txId, mode });
    return false; // Would block
  }

  /** Release all locks for a transaction. */
  release(txId) {
    const resources = this._txLocks.get(txId);
    if (!resources) return;

    for (const resource of resources) {
      const lock = this._locks.get(resource);
      if (lock) {
        lock.holders.delete(txId);
        if (lock.holders.size === 0) {
          // Grant to next in queue
          if (lock.queue.length > 0) {
            const next = lock.queue.shift();
            lock.holders.add(next.txId);
            lock.mode = next.mode;
            if (!this._txLocks.has(next.txId)) this._txLocks.set(next.txId, new Set());
            this._txLocks.get(next.txId).add(resource);
          } else {
            this._locks.delete(resource);
          }
        }
      }
    }
    this._txLocks.delete(txId);
  }

  /** Deadlock detection: BFS on waits-for graph. */
  _wouldDeadlock(txId, resource) {
    const lock = this._locks.get(resource);
    if (!lock) return false;

    // BFS from holders to see if any path leads back to txId
    const visited = new Set();
    const queue = [...lock.holders];
    
    while (queue.length > 0) {
      const holder = queue.shift();
      if (holder === txId) return true; // Cycle!
      if (visited.has(holder)) continue;
      visited.add(holder);
      
      // What resources does this holder want?
      const holderResources = this._txLocks.get(holder);
      if (holderResources) {
        for (const res of holderResources) {
          const resLock = this._locks.get(res);
          if (resLock) {
            for (const { txId: waitingTx } of resLock.queue) {
              if (waitingTx === holder) {
                // This holder is waiting on someone
                for (const h of resLock.holders) queue.push(h);
              }
            }
          }
        }
      }
    }
    return false;
  }

  getStats() {
    return {
      activeResources: this._locks.size,
      activeTxns: this._txLocks.size,
    };
  }
}

export { SHARED, EXCLUSIVE };
