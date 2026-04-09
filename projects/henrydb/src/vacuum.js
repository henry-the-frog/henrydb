// vacuum.js — VACUUM and dead tuple management for HenryDB
// Tracks dead tuples, reclaims space, auto-vacuum triggers.

/**
 * DeadTupleTracker — tracks dead (deleted/updated) tuples per table.
 */
class DeadTupleTracker {
  constructor(tableName) {
    this.tableName = tableName;
    this.deadTuples = 0;
    this.liveTuples = 0;
    this.lastVacuum = null;
    this.lastAutoVacuum = null;
    this.vacuumCount = 0;
    this.autoVacuumCount = 0;
    this.insertsSinceLastVacuum = 0;
    this.updatesSinceLastVacuum = 0;
    this.deletesSinceLastVacuum = 0;
  }

  recordInsert(count = 1) {
    this.liveTuples += count;
    this.insertsSinceLastVacuum += count;
  }

  recordUpdate(count = 1) {
    this.deadTuples += count; // Old version becomes dead
    this.updatesSinceLastVacuum += count;
  }

  recordDelete(count = 1) {
    this.liveTuples -= count;
    this.deadTuples += count;
    this.deletesSinceLastVacuum += count;
  }

  getBloatRatio() {
    const total = this.liveTuples + this.deadTuples;
    return total > 0 ? this.deadTuples / total : 0;
  }

  vacuum() {
    const reclaimed = this.deadTuples;
    this.deadTuples = 0;
    this.insertsSinceLastVacuum = 0;
    this.updatesSinceLastVacuum = 0;
    this.deletesSinceLastVacuum = 0;
    this.lastVacuum = Date.now();
    this.vacuumCount++;
    return reclaimed;
  }
}

/**
 * VacuumManager — manages vacuum operations across all tables.
 */
export class VacuumManager {
  constructor(options = {}) {
    this._tables = new Map(); // table → DeadTupleTracker
    this.autoVacuumEnabled = options.autoVacuum !== false;
    this.autoVacuumThreshold = options.threshold || 50; // Min dead tuples to trigger
    this.autoVacuumScaleFactor = options.scaleFactor || 0.2; // % of live tuples
    this.autoVacuumNaptime = options.naptime || 60000; // Check interval (ms)
    this._timer = null;
    this._stats = {
      manualVacuums: 0,
      autoVacuums: 0,
      totalReclaimed: 0,
    };
  }

  /**
   * Register a table for tracking.
   */
  registerTable(tableName, initialRows = 0) {
    const tracker = new DeadTupleTracker(tableName);
    tracker.liveTuples = initialRows;
    this._tables.set(tableName.toLowerCase(), tracker);
  }

  /**
   * Get tracker for a table.
   */
  getTracker(tableName) {
    return this._tables.get(tableName.toLowerCase());
  }

  /**
   * Record a DML operation for dead tuple tracking.
   */
  recordDML(tableName, operation, count = 1) {
    const tracker = this._tables.get(tableName.toLowerCase());
    if (!tracker) return;

    switch (operation.toUpperCase()) {
      case 'INSERT': tracker.recordInsert(count); break;
      case 'UPDATE': tracker.recordUpdate(count); break;
      case 'DELETE': tracker.recordDelete(count); break;
    }
  }

  /**
   * VACUUM a specific table.
   */
  vacuum(tableName, options = {}) {
    const tracker = this._tables.get(tableName.toLowerCase());
    if (!tracker) throw new Error(`Table '${tableName}' not registered`);

    const reclaimed = tracker.vacuum();
    this._stats.manualVacuums++;
    this._stats.totalReclaimed += reclaimed;

    return {
      table: tableName,
      deadTuplesReclaimed: reclaimed,
      liveTuples: tracker.liveTuples,
      full: options.full || false,
    };
  }

  /**
   * VACUUM all tables.
   */
  vacuumAll() {
    const results = [];
    for (const [name, tracker] of this._tables) {
      if (tracker.deadTuples > 0) {
        results.push(this.vacuum(name));
      }
    }
    return results;
  }

  /**
   * Check which tables need auto-vacuum.
   */
  checkAutoVacuum() {
    const needsVacuum = [];

    for (const [name, tracker] of this._tables) {
      const threshold = this.autoVacuumThreshold +
        (this.autoVacuumScaleFactor * tracker.liveTuples);

      if (tracker.deadTuples >= threshold) {
        needsVacuum.push({
          table: name,
          deadTuples: tracker.deadTuples,
          threshold: Math.round(threshold),
          bloatRatio: +(tracker.getBloatRatio() * 100).toFixed(1),
        });
      }
    }

    return needsVacuum;
  }

  /**
   * Run auto-vacuum cycle: check and vacuum tables that need it.
   */
  runAutoVacuum() {
    const candidates = this.checkAutoVacuum();
    const results = [];

    for (const candidate of candidates) {
      const tracker = this._tables.get(candidate.table);
      const reclaimed = tracker.vacuum();
      tracker.lastAutoVacuum = Date.now();
      tracker.autoVacuumCount++;
      this._stats.autoVacuums++;
      this._stats.totalReclaimed += reclaimed;
      results.push({
        table: candidate.table,
        deadTuplesReclaimed: reclaimed,
      });
    }

    return results;
  }

  /**
   * Start the auto-vacuum daemon.
   */
  startDaemon() {
    if (this._timer) return;
    this._timer = setInterval(() => {
      if (this.autoVacuumEnabled) {
        this.runAutoVacuum();
      }
    }, this.autoVacuumNaptime);
  }

  /**
   * Stop the auto-vacuum daemon.
   */
  stopDaemon() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  /**
   * Get bloat estimation for all tables.
   */
  getBloatReport() {
    const report = [];
    for (const [name, tracker] of this._tables) {
      report.push({
        table: name,
        liveTuples: tracker.liveTuples,
        deadTuples: tracker.deadTuples,
        bloatRatio: +(tracker.getBloatRatio() * 100).toFixed(1),
        lastVacuum: tracker.lastVacuum,
        lastAutoVacuum: tracker.lastAutoVacuum,
        vacuumCount: tracker.vacuumCount,
        autoVacuumCount: tracker.autoVacuumCount,
      });
    }
    return report.sort((a, b) => b.bloatRatio - a.bloatRatio);
  }

  getStats() {
    return {
      ...this._stats,
      registeredTables: this._tables.size,
      autoVacuumEnabled: this.autoVacuumEnabled,
    };
  }
}
