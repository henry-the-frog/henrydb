// migrations.js — Schema migration system for HenryDB
// Versioned up/down migrations with tracking table and transaction support.

/**
 * MigrationRunner — manages database schema migrations.
 * 
 * Usage:
 *   const runner = new MigrationRunner(db);
 *   runner.add(1, 'Create users table',
 *     'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
 *     'DROP TABLE users'
 *   );
 *   runner.up();     // Apply all pending migrations
 *   runner.down();   // Rollback last migration
 *   runner.status(); // Show migration status
 */
export class MigrationRunner {
  constructor(db) {
    this.db = db;
    this.migrations = [];
    this._ensureTrackingTable();
  }

  _ensureTrackingTable() {
    try {
      this.db.execute(`CREATE TABLE IF NOT EXISTS _migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL
      )`);
    } catch(e) {
      // Table might already exist
      try {
        this.db.execute('SELECT version FROM _migrations LIMIT 1');
      } catch(e2) {
        this.db.execute(`CREATE TABLE _migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )`);
      }
    }
  }

  /**
   * Register a migration.
   * @param {number} version - Unique version number (must be sequential)
   * @param {string} name - Human-readable migration name
   * @param {string|string[]} up - SQL to apply migration (string or array of statements)
   * @param {string|string[]} down - SQL to rollback migration
   */
  add(version, name, up, down) {
    this.migrations.push({
      version,
      name,
      up: Array.isArray(up) ? up : [up],
      down: Array.isArray(down) ? down : [down],
    });
    // Keep sorted by version
    this.migrations.sort((a, b) => a.version - b.version);
    return this;
  }

  /**
   * Get the current applied version.
   * @returns {number} Highest applied version, or 0 if none
   */
  currentVersion() {
    const result = this.db.execute('SELECT MAX(version) as v FROM _migrations');
    return result.rows[0]?.v || 0;
  }

  /**
   * Get list of applied migrations.
   */
  applied() {
    return this.db.execute('SELECT * FROM _migrations ORDER BY version').rows;
  }

  /**
   * Get pending (unapplied) migrations.
   */
  pending() {
    const current = this.currentVersion();
    return this.migrations.filter(m => m.version > current);
  }

  /**
   * Apply all pending migrations (or up to a specific version).
   * @param {number} [targetVersion] - Apply up to this version (inclusive)
   * @returns {{applied: number[], errors: string[]}}
   */
  up(targetVersion) {
    const current = this.currentVersion();
    const target = targetVersion || Math.max(...this.migrations.map(m => m.version));
    const toApply = this.migrations.filter(m => m.version > current && m.version <= target);
    
    const applied = [];
    const errors = [];

    for (const migration of toApply) {
      try {
        this.db.execute('BEGIN');
        for (const sql of migration.up) {
          this.db.execute(sql);
        }
        const now = new Date().toISOString();
        this.db.execute(`INSERT INTO _migrations (version, name, applied_at) VALUES (${migration.version}, '${migration.name.replace(/'/g, "''")}', '${now}')`);
        this.db.execute('COMMIT');
        applied.push(migration.version);
      } catch(e) {
        try { this.db.execute('ROLLBACK'); } catch(e2) {}
        errors.push(`Migration ${migration.version} (${migration.name}) failed: ${e.message}`);
        break; // Stop on first error
      }
    }

    return { applied, errors };
  }

  /**
   * Rollback the last applied migration (or down to a specific version).
   * @param {number} [targetVersion] - Rollback down to this version (exclusive)
   * @returns {{rolledBack: number[], errors: string[]}}
   */
  down(targetVersion = null) {
    const current = this.currentVersion();
    const target = targetVersion !== null ? targetVersion : current - 1;
    
    // Get migrations to rollback (in reverse order)
    const toRollback = this.migrations
      .filter(m => m.version > target && m.version <= current)
      .reverse();
    
    const rolledBack = [];
    const errors = [];

    for (const migration of toRollback) {
      try {
        this.db.execute('BEGIN');
        for (const sql of migration.down) {
          this.db.execute(sql);
        }
        this.db.execute(`DELETE FROM _migrations WHERE version = ${migration.version}`);
        this.db.execute('COMMIT');
        rolledBack.push(migration.version);
      } catch(e) {
        try { this.db.execute('ROLLBACK'); } catch(e2) {}
        errors.push(`Rollback ${migration.version} (${migration.name}) failed: ${e.message}`);
        break;
      }
    }

    return { rolledBack, errors };
  }

  /**
   * Get migration status summary.
   */
  status() {
    const current = this.currentVersion();
    const appliedList = this.applied();
    const pendingList = this.pending();
    
    return {
      currentVersion: current,
      applied: appliedList.length,
      pending: pendingList.length,
      total: this.migrations.length,
      migrations: this.migrations.map(m => ({
        version: m.version,
        name: m.name,
        status: appliedList.some(a => a.version === m.version) ? 'applied' : 'pending',
        appliedAt: appliedList.find(a => a.version === m.version)?.applied_at || null,
      })),
    };
  }

  /**
   * Reset: rollback all migrations.
   */
  reset() {
    return this.down(0);
  }

  /**
   * Redo: rollback last migration and re-apply it.
   */
  redo() {
    const current = this.currentVersion();
    if (current === 0) return { applied: [], errors: ['No migrations to redo'] };
    
    const downResult = this.down(current - 1);
    if (downResult.errors.length) return downResult;
    
    return this.up(current);
  }
}
