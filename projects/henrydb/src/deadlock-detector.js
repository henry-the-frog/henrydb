// deadlock-detector.js — Wait-for graph deadlock detection for HenryDB
// Detects deadlocks via cycle detection in the wait-for graph.
// Selects victim transaction with lowest cost to abort.

/**
 * WaitForGraph — directed graph where edge A→B means "A waits for B".
 */
export class WaitForGraph {
  constructor() {
    this._edges = new Map(); // txId → Set<txId> (waits for)
    this._edgeInfo = new Map(); // `${from}:${to}` → { resource, timestamp }
  }

  /**
   * Add a wait-for edge: txA is waiting for txB.
   */
  addEdge(waiter, holder, resource = null) {
    if (!this._edges.has(waiter)) {
      this._edges.set(waiter, new Set());
    }
    this._edges.get(waiter).add(holder);
    this._edgeInfo.set(`${waiter}:${holder}`, {
      resource,
      timestamp: Date.now(),
    });
  }

  /**
   * Remove a wait-for edge.
   */
  removeEdge(waiter, holder) {
    const waitees = this._edges.get(waiter);
    if (waitees) {
      waitees.delete(holder);
      if (waitees.size === 0) this._edges.delete(waiter);
    }
    this._edgeInfo.delete(`${waiter}:${holder}`);
  }

  /**
   * Remove all edges involving a transaction (completed/aborted).
   */
  removeTransaction(txId) {
    // Remove outgoing edges
    this._edges.delete(txId);
    
    // Remove incoming edges
    for (const [waiter, holders] of this._edges) {
      holders.delete(txId);
      if (holders.size === 0) this._edges.delete(waiter);
    }

    // Clean edge info
    for (const key of [...this._edgeInfo.keys()]) {
      if (key.startsWith(`${txId}:`) || key.endsWith(`:${txId}`)) {
        this._edgeInfo.delete(key);
      }
    }
  }

  /**
   * Detect all cycles (deadlocks) in the wait-for graph.
   * Returns an array of cycles, where each cycle is an array of transaction IDs.
   */
  detectCycles() {
    const visited = new Set();
    const recStack = new Set();
    const cycles = [];

    for (const node of this._edges.keys()) {
      if (!visited.has(node)) {
        this._dfs(node, visited, recStack, [], cycles);
      }
    }

    return cycles;
  }

  _dfs(node, visited, recStack, path, cycles) {
    visited.add(node);
    recStack.add(node);
    path.push(node);

    const neighbors = this._edges.get(node) || new Set();
    for (const neighbor of neighbors) {
      if (!visited.has(neighbor)) {
        this._dfs(neighbor, visited, recStack, [...path], cycles);
      } else if (recStack.has(neighbor)) {
        // Found a cycle — extract it
        const cycleStart = path.indexOf(neighbor);
        if (cycleStart >= 0) {
          const cycle = path.slice(cycleStart);
          cycle.push(neighbor); // Close the cycle
          cycles.push(cycle);
        }
      }
    }

    recStack.delete(node);
  }

  /**
   * Get all edges in the graph (for visualization).
   */
  getEdges() {
    const edges = [];
    for (const [waiter, holders] of this._edges) {
      for (const holder of holders) {
        const info = this._edgeInfo.get(`${waiter}:${holder}`) || {};
        edges.push({
          waiter,
          holder,
          resource: info.resource,
          waitingSince: info.timestamp,
        });
      }
    }
    return edges;
  }

  get size() {
    let count = 0;
    for (const holders of this._edges.values()) count += holders.size;
    return count;
  }
}

/**
 * DeadlockDetector — monitors for deadlocks and selects victims.
 */
export class DeadlockDetector {
  constructor(options = {}) {
    this.graph = new WaitForGraph();
    this.checkIntervalMs = options.checkIntervalMs || 1000;
    this._txInfo = new Map(); // txId → { startTime, statementsExecuted, rowsModified }
    this._stats = {
      checksPerformed: 0,
      deadlocksDetected: 0,
      victimsSelected: 0,
    };
    this._timer = null;
  }

  /**
   * Register a transaction.
   */
  registerTransaction(txId, info = {}) {
    this._txInfo.set(txId, {
      startTime: Date.now(),
      statementsExecuted: info.statementsExecuted || 0,
      rowsModified: info.rowsModified || 0,
      priority: info.priority || 0,
    });
  }

  // Alias for tests
  registerTxn(txId, info) { return this.registerTransaction(txId, info); }
  addWait(waiter, holder, resource) { return this.recordWait(waiter, holder, resource); }
  resolveDeadlocks() { return this.check(); }

  /**
   * Record that txA is waiting for txB on a resource.
   */
  recordWait(waiter, holder, resource) {
    this.graph.addEdge(waiter, holder, resource);
  }

  /**
   * Record that a wait has been resolved (lock acquired).
   */
  resolveWait(waiter, holder) {
    this.graph.removeEdge(waiter, holder);
  }

  /**
   * Remove a completed/aborted transaction.
   */
  removeTransaction(txId) {
    this.graph.removeTransaction(txId);
    this._txInfo.delete(txId);
  }

  /**
   * Check for deadlocks and select victims.
   * Returns array of { cycle, victim, reason }.
   */
  check() {
    this._stats.checksPerformed++;
    const cycles = this.graph.detectCycles();

    if (cycles.length === 0) return [];

    const results = [];
    for (const cycle of cycles) {
      this._stats.deadlocksDetected++;
      const victim = this._selectVictim(cycle);
      this._stats.victimsSelected++;
      results.push({
        cycle: cycle.slice(0, -1), // Remove duplicate closing node
        victim: victim.txId,
        reason: victim.reason,
      });
    }

    return results;
  }

  /**
   * Start periodic deadlock checking.
   */
  startMonitoring() {
    this._timer = setInterval(() => this.check(), this.checkIntervalMs);
  }

  /**
   * Stop monitoring.
   */
  stopMonitoring() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  getStats() {
    return {
      ...this._stats,
      activeTransactions: this._txInfo.size,
      waitEdges: this.graph.size,
    };
  }

  /**
   * Select the best victim from a cycle.
   * Strategy: abort the transaction with the least work done.
   */
  _selectVictim(cycle) {
    const candidates = cycle.slice(0, -1); // Remove duplicate
    let bestVictim = candidates[0];
    let bestCost = Infinity;

    for (const txId of candidates) {
      const info = this._txInfo.get(txId);
      if (!info) continue;

      // Cost = statements * 10 + rows modified + priority bonus
      // Lower cost = more expendable
      const cost = (info.statementsExecuted * 10) + info.rowsModified + (info.priority * 1000);
      if (cost < bestCost) {
        bestCost = cost;
        bestVictim = txId;
      }
    }

    return {
      txId: bestVictim,
      reason: `Lowest cost transaction in deadlock cycle (cost=${bestCost})`,
    };
  }
}
