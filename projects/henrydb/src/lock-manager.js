// lock-manager.js — Lock manager with deadlock detection for HenryDB
// Supports shared (S) and exclusive (X) locks.
// Detects deadlocks via wait-for graph cycle detection.

const LockMode = { SHARED: 'S', EXCLUSIVE: 'X' };

/**
 * Lock request in the queue.
 */
class LockRequest {
  constructor(txId, mode) {
    this.txId = txId;
    this.mode = mode;
    this.granted = false;
  }
}

/**
 * Lock Manager with deadlock detection.
 */
export class LockManager {
  constructor() {
    this._lockTable = new Map(); // resource → LockRequest[]
    this._txLocks = new Map();   // txId → Set<resource>
    this._waitFor = new Map();   // txId → Set<txId> (wait-for graph)
  }

  /**
   * Acquire a lock. Returns true if granted, throws on deadlock.
   */
  lock(txId, resource, mode = LockMode.EXCLUSIVE) {
    if (!this._lockTable.has(resource)) {
      this._lockTable.set(resource, []);
    }
    if (!this._txLocks.has(txId)) {
      this._txLocks.set(txId, new Set());
    }

    const requests = this._lockTable.get(resource);
    
    // Check if this tx already holds a compatible lock
    const existing = requests.find(r => r.txId === txId && r.granted);
    if (existing) {
      if (existing.mode === LockMode.EXCLUSIVE || mode === LockMode.SHARED) {
        return true; // Already have sufficient lock
      }
      // Upgrade S → X: need to check compatibility
      if (this._canGrant(resource, txId, LockMode.EXCLUSIVE)) {
        existing.mode = LockMode.EXCLUSIVE;
        return true;
      }
    }

    // Check if lock can be granted immediately
    if (this._canGrant(resource, txId, mode)) {
      const req = new LockRequest(txId, mode);
      req.granted = true;
      requests.push(req);
      this._txLocks.get(txId).add(resource);
      return true;
    }

    // Cannot grant immediately — check for deadlock before waiting
    const holders = this._getHolders(resource);
    this._addWaitEdges(txId, holders);
    
    if (this._detectDeadlock(txId)) {
      this._removeWaitEdges(txId);
      throw new Error(`Deadlock detected: transaction ${txId} would cause a cycle`);
    }

    // In a real system we'd block here. In our simulation, we just queue it.
    const req = new LockRequest(txId, mode);
    requests.push(req);
    this._txLocks.get(txId).add(resource);
    return false; // Not granted (would need to wait)
  }

  /**
   * Release all locks held by a transaction.
   */
  unlock(txId) {
    const resources = this._txLocks.get(txId);
    if (!resources) return;

    for (const resource of resources) {
      const requests = this._lockTable.get(resource);
      if (!requests) continue;
      
      // Remove this tx's requests
      const filtered = requests.filter(r => r.txId !== txId);
      this._lockTable.set(resource, filtered);
      
      // Try to grant waiting requests
      this._grantWaiting(resource);
    }

    this._txLocks.delete(txId);
    this._waitFor.delete(txId);
    
    // Remove this tx from other wait-for edges
    for (const [, waitSet] of this._waitFor) {
      waitSet.delete(txId);
    }
  }

  /**
   * Release a specific lock.
   */
  unlockResource(txId, resource) {
    const requests = this._lockTable.get(resource);
    if (!requests) return;
    
    const filtered = requests.filter(r => r.txId !== txId);
    this._lockTable.set(resource, filtered);
    
    const txResources = this._txLocks.get(txId);
    if (txResources) txResources.delete(resource);
    
    this._grantWaiting(resource);
  }

  /**
   * Check if a lock can be granted.
   */
  _canGrant(resource, txId, mode) {
    const requests = this._lockTable.get(resource) || [];
    const granted = requests.filter(r => r.granted && r.txId !== txId);
    
    if (granted.length === 0) return true;
    
    if (mode === LockMode.SHARED) {
      // S lock: compatible with other S locks
      return granted.every(r => r.mode === LockMode.SHARED);
    }
    
    // X lock: incompatible with everything
    return false;
  }

  _getHolders(resource) {
    const requests = this._lockTable.get(resource) || [];
    return requests.filter(r => r.granted).map(r => r.txId);
  }

  _addWaitEdges(txId, holders) {
    if (!this._waitFor.has(txId)) {
      this._waitFor.set(txId, new Set());
    }
    for (const holder of holders) {
      if (holder !== txId) {
        this._waitFor.get(txId).add(holder);
      }
    }
  }

  _removeWaitEdges(txId) {
    this._waitFor.delete(txId);
  }

  /**
   * Detect deadlock using DFS cycle detection on the wait-for graph.
   */
  _detectDeadlock(startTx) {
    const visited = new Set();
    const stack = new Set();
    
    const dfs = (tx) => {
      if (stack.has(tx)) return true; // Cycle!
      if (visited.has(tx)) return false;
      
      visited.add(tx);
      stack.add(tx);
      
      const waitsFor = this._waitFor.get(tx) || new Set();
      for (const dep of waitsFor) {
        if (dfs(dep)) return true;
      }
      
      stack.delete(tx);
      return false;
    };
    
    return dfs(startTx);
  }

  _grantWaiting(resource) {
    const requests = this._lockTable.get(resource) || [];
    for (const req of requests) {
      if (!req.granted && this._canGrant(resource, req.txId, req.mode)) {
        req.granted = true;
        // Remove wait edges
        this._removeWaitEdges(req.txId);
      }
    }
  }

  /**
   * Get current lock state for debugging.
   */
  state() {
    const locks = {};
    for (const [resource, requests] of this._lockTable) {
      locks[resource] = requests.map(r => ({
        tx: r.txId,
        mode: r.mode,
        granted: r.granted,
      }));
    }
    return {
      locks,
      waitFor: Object.fromEntries(
        [...this._waitFor.entries()].map(([tx, deps]) => [tx, [...deps]])
      ),
    };
  }
}

export { LockMode };
