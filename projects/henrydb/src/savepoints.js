// savepoints.js — Savepoint support for HenryDB transactions
// SAVEPOINT name, RELEASE SAVEPOINT name, ROLLBACK TO SAVEPOINT name
// Nested savepoints with snapshot stacking.

/**
 * Savepoint — a named point within a transaction that can be rolled back to.
 */
class Savepoint {
  constructor(name, snapshot) {
    this.name = name;
    this.snapshot = snapshot; // Deep copy of state at savepoint time
    this.createdAt = Date.now();
    this.released = false;
  }
}

/**
 * SavepointManager — manages savepoints within a transaction.
 * Uses a stack-based approach: savepoints are nested.
 */
export class SavepointManager {
  constructor(options = {}) {
    this._savepoints = []; // Stack of Savepoint objects
    this._savepointNames = new Map(); // name → index in stack
    this.snapshotFn = options.snapshotFn || (() => ({})); // Function to capture current state
    this.restoreFn = options.restoreFn || (() => {}); // Function to restore state
    this._stats = {
      created: 0,
      released: 0,
      rolledBack: 0,
    };
  }

  /**
   * SAVEPOINT name — create a named savepoint.
   */
  savepoint(name) {
    const lowerName = name.toLowerCase();
    
    // If savepoint with same name exists, replace it
    if (this._savepointNames.has(lowerName)) {
      const idx = this._savepointNames.get(lowerName);
      this._savepoints[idx] = new Savepoint(lowerName, this.snapshotFn());
    } else {
      const sp = new Savepoint(lowerName, this.snapshotFn());
      this._savepoints.push(sp);
      this._savepointNames.set(lowerName, this._savepoints.length - 1);
    }

    this._stats.created++;
    return { name: lowerName, depth: this._savepoints.length };
  }

  /**
   * RELEASE SAVEPOINT name — release (destroy) a savepoint and all newer ones.
   * The changes since the savepoint are kept.
   */
  release(name) {
    const lowerName = name.toLowerCase();
    const idx = this._savepointNames.get(lowerName);
    if (idx === undefined) {
      throw new Error(`Savepoint '${name}' does not exist`);
    }

    // Remove this savepoint and all newer ones
    const removed = this._savepoints.splice(idx);
    for (const sp of removed) {
      this._savepointNames.delete(sp.name);
    }

    // Rebuild name map
    this._rebuildNameMap();

    this._stats.released += removed.length;
    return { released: removed.length };
  }

  /**
   * ROLLBACK TO SAVEPOINT name — restore state to the savepoint.
   * The savepoint itself is kept; newer savepoints are destroyed.
   */
  rollbackTo(name) {
    const lowerName = name.toLowerCase();
    const idx = this._savepointNames.get(lowerName);
    if (idx === undefined) {
      throw new Error(`Savepoint '${name}' does not exist`);
    }

    const sp = this._savepoints[idx];

    // Restore state
    this.restoreFn(sp.snapshot);

    // Remove savepoints newer than this one (but keep this one)
    const removed = this._savepoints.splice(idx + 1);
    for (const rsp of removed) {
      this._savepointNames.delete(rsp.name);
    }

    // Take a fresh snapshot for the kept savepoint
    sp.snapshot = this.snapshotFn();

    this._rebuildNameMap();
    this._stats.rolledBack++;

    return { rolledBackTo: lowerName, removedSavepoints: removed.length };
  }

  /**
   * Clear all savepoints (COMMIT or ROLLBACK).
   */
  clear() {
    const count = this._savepoints.length;
    this._savepoints = [];
    this._savepointNames.clear();
    return count;
  }

  /**
   * Check if a savepoint exists.
   */
  has(name) {
    return this._savepointNames.has(name.toLowerCase());
  }

  /**
   * Get the current nesting depth.
   */
  get depth() {
    return this._savepoints.length;
  }

  /**
   * List all active savepoints.
   */
  list() {
    return this._savepoints.map((sp, idx) => ({
      name: sp.name,
      depth: idx + 1,
      createdAt: sp.createdAt,
    }));
  }

  getStats() {
    return {
      ...this._stats,
      activeSavepoints: this._savepoints.length,
    };
  }

  _rebuildNameMap() {
    this._savepointNames.clear();
    for (let i = 0; i < this._savepoints.length; i++) {
      this._savepointNames.set(this._savepoints[i].name, i);
    }
  }
}
