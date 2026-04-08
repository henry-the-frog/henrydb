// deadlock-detector.js — Wait-for graph cycle detection
// Maintains a directed graph where edges represent "transaction A waits for transaction B".
// Periodically checks for cycles, which indicate deadlocks.
// When a deadlock is detected, selects a victim transaction to abort.

/**
 * DeadlockDetector — wait-for graph with cycle detection.
 */
export class DeadlockDetector {
  constructor(options = {}) {
    this.victimPolicy = options.victimPolicy || 'youngest'; // 'youngest' | 'cheapest'
    
    // Wait-for graph: txnId → Set<txnId it waits for>
    this._waitFor = new Map();
    // Transaction metadata: txnId → { startTime, cost }
    this._txnMeta = new Map();
    
    this.stats = { checks: 0, deadlocksDetected: 0, victimsChosen: 0 };
  }

  /**
   * Register a transaction.
   */
  registerTxn(txnId, meta = {}) {
    this._txnMeta.set(txnId, {
      startTime: meta.startTime || Date.now(),
      cost: meta.cost || 0,
    });
    if (!this._waitFor.has(txnId)) {
      this._waitFor.set(txnId, new Set());
    }
  }

  /**
   * Record that txnA is waiting for txnB (txnA wants a lock held by txnB).
   */
  addWait(txnA, txnB) {
    if (!this._waitFor.has(txnA)) this._waitFor.set(txnA, new Set());
    this._waitFor.get(txnA).add(txnB);
  }

  /**
   * Remove a wait edge (lock granted or txn aborted).
   */
  removeWait(txnA, txnB) {
    if (this._waitFor.has(txnA)) {
      this._waitFor.get(txnA).delete(txnB);
    }
  }

  /**
   * Remove all edges for a transaction (txn committed/aborted).
   */
  removeTxn(txnId) {
    this._waitFor.delete(txnId);
    this._txnMeta.delete(txnId);
    // Remove edges pointing to this txn
    for (const [, waitsFor] of this._waitFor) {
      waitsFor.delete(txnId);
    }
  }

  /**
   * Check for deadlocks. Returns array of cycles found.
   * Each cycle is an array of txn IDs forming the cycle.
   */
  detectDeadlocks() {
    this.stats.checks++;
    const cycles = [];
    const visited = new Set();
    const inStack = new Set();
    const path = [];

    for (const txnId of this._waitFor.keys()) {
      if (!visited.has(txnId)) {
        this._dfs(txnId, visited, inStack, path, cycles);
      }
    }

    if (cycles.length > 0) {
      this.stats.deadlocksDetected += cycles.length;
    }

    return cycles;
  }

  _dfs(node, visited, inStack, path, cycles) {
    visited.add(node);
    inStack.add(node);
    path.push(node);

    const waitsFor = this._waitFor.get(node) || new Set();
    for (const neighbor of waitsFor) {
      if (!visited.has(neighbor)) {
        this._dfs(neighbor, visited, inStack, path, cycles);
      } else if (inStack.has(neighbor)) {
        // Found a cycle!
        const cycleStart = path.indexOf(neighbor);
        cycles.push(path.slice(cycleStart));
      }
    }

    path.pop();
    inStack.delete(node);
  }

  /**
   * Detect deadlocks and choose victims to abort.
   * Returns array of victim txn IDs.
   */
  resolveDeadlocks() {
    const cycles = this.detectDeadlocks();
    const victims = [];

    for (const cycle of cycles) {
      const victim = this._chooseVictim(cycle);
      if (victim !== null) {
        victims.push(victim);
        this.removeTxn(victim);
        this.stats.victimsChosen++;
      }
    }

    return victims;
  }

  _chooseVictim(cycle) {
    if (cycle.length === 0) return null;

    if (this.victimPolicy === 'youngest') {
      // Abort the youngest transaction (most recent start)
      let youngest = cycle[0];
      let youngestTime = 0;
      for (const txnId of cycle) {
        const meta = this._txnMeta.get(txnId);
        if (meta && meta.startTime > youngestTime) {
          youngestTime = meta.startTime;
          youngest = txnId;
        }
      }
      return youngest;
    }

    if (this.victimPolicy === 'cheapest') {
      // Abort the transaction with lowest cost
      let cheapest = cycle[0];
      let cheapestCost = Infinity;
      for (const txnId of cycle) {
        const meta = this._txnMeta.get(txnId);
        if (meta && meta.cost < cheapestCost) {
          cheapestCost = meta.cost;
          cheapest = txnId;
        }
      }
      return cheapest;
    }

    return cycle[0]; // Default: first in cycle
  }

  getStats() {
    return {
      ...this.stats,
      activeTransactions: this._txnMeta.size,
      waitEdges: [...this._waitFor.values()].reduce((s, set) => s + set.size, 0),
    };
  }
}
