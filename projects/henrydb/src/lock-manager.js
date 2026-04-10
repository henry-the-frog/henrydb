// lock-manager.js — Two-Phase Locking with deadlock detection
// Manages shared (S), exclusive (X), and intention (IS, IX, SIX) locks.
// Deadlock detection via waits-for graph cycle detection.

const SHARED = 'S';
const EXCLUSIVE = 'X';

// Lock compatibility matrix (true = compatible)
// IS and IX are intention locks: IS = intent to read child, IX = intent to write child
const COMPAT = {
  'IS':  { 'IS': true,  'IX': true,  'S': true,  'SIX': true,  'X': false },
  'IX':  { 'IS': true,  'IX': true,  'S': false, 'SIX': false, 'X': false },
  'S':   { 'IS': true,  'IX': false, 'S': true,  'SIX': false, 'X': false },
  'SIX': { 'IS': true,  'IX': false, 'S': false, 'SIX': false, 'X': false },
  'X':   { 'IS': false, 'IX': false, 'S': false, 'SIX': false, 'X': false },
};

function isCompatible(mode1, mode2) {
  if (!COMPAT[mode1]) return mode1 === mode2 && mode1 === SHARED;
  return !!COMPAT[mode1][mode2];
}

export class LockManager {
  constructor() {
    this._locks = new Map();    // resource → {holders: Set<txId>, mode, queue: []}
    this._txLocks = new Map();  // txId → Set<resource>
    this.stats = { grants: 0, releases: 0, waits: 0, deadlocks: 0 };
  }

  /** Check if mode1 is strictly stronger than mode2 */
  _isStronger(mode1, mode2) {
    const strength = { 'IS': 0, 'S': 1, 'IX': 2, 'SIX': 3, 'X': 4 };
    return (strength[mode1] || 0) > (strength[mode2] || 0);
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
      // Lock upgrade: if requesting stronger mode, try to upgrade
      if (this._isStronger(mode, lock.mode)) {
        if (lock.holders.size === 1) {
          lock.mode = mode;
          this.stats.grants++;
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
      this.stats.grants++;
      return true;
    }

    if (isCompatible(mode, lock.mode)) {
      lock.holders.add(txId);
      this._txLocks.get(txId).add(resource);
      this.stats.grants++;
      return true;
    }

    // Would block — check for deadlock
    if (this._wouldDeadlock(txId, resource)) return false;

    // Queue the request (in real impl, would block)
    lock.queue.push({ txId, mode });
    return false; // Would block
  }

  /** Release locks for a transaction. If resource specified, release only that lock. */
  release(txId, resource) {
    const resources = this._txLocks.get(txId);
    if (!resources) return;

    const toRelease = resource ? [resource] : [...resources];
    
    for (const res of toRelease) {
      if (!resources.has(res)) continue;
      const lock = this._locks.get(res);
      if (lock) {
        lock.holders.delete(txId);
        this.stats.releases++;
        resources.delete(res);
        if (lock.holders.size === 0) {
          // Grant to next in queue
          if (lock.queue.length > 0) {
            const next = lock.queue.shift();
            lock.holders.add(next.txId);
            lock.mode = next.mode;
            this.stats.grants++;
            if (!this._txLocks.has(next.txId)) this._txLocks.set(next.txId, new Set());
            this._txLocks.get(next.txId).add(res);
          } else {
            this._locks.delete(res);
          }
        }
      }
    }
    if (resources.size === 0) {
      this._txLocks.delete(txId);
    }
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
