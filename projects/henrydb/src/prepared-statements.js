// prepared-statements.js — PREPARE/EXECUTE with parameter binding and plan caching
// PostgreSQL-compatible prepared statement system.
// PREPARE name (type, type, ...) AS SELECT ...;
// EXECUTE name (value, value, ...);
// DEALLOCATE name;

/**
 * PreparedStatementCache — manages prepared statements and their cached plans.
 */
export class PreparedStatementCache {
  constructor(db, options = {}) {
    this.db = db;
    this.maxStatements = options.maxStatements || 100;
    this._statements = new Map(); // name → PreparedStatement
    this._stats = {
      prepares: 0,
      executions: 0,
      cacheHits: 0,
      deallocations: 0,
      evictions: 0,
    };
  }

  /**
   * Prepare a statement.
   * PREPARE name (type, ...) AS sql_with_$1_params
   */
  prepare(name, sql, paramTypes = []) {
    const lowerName = name.toLowerCase();

    if (this._statements.has(lowerName)) {
      throw new Error(`Prepared statement '${name}' already exists`);
    }

    // Evict if at capacity
    if (this._statements.size >= this.maxStatements) {
      this._evictLRU();
    }

    const stmt = {
      name: lowerName,
      sql,
      paramTypes,
      paramCount: (sql.match(/\$\d+/g) || []).length,
      executionCount: 0,
      totalTimeMs: 0,
      lastExecuted: null,
      createdAt: Date.now(),
    };

    this._statements.set(lowerName, stmt);
    this._stats.prepares++;
    return stmt;
  }

  /**
   * Execute a prepared statement with parameter values.
   */
  execute(name, params = []) {
    const lowerName = name.toLowerCase();
    const stmt = this._statements.get(lowerName);
    if (!stmt) {
      throw new Error(`Prepared statement '${name}' does not exist`);
    }

    // Validate parameter count
    if (params.length < stmt.paramCount) {
      throw new Error(`Expected ${stmt.paramCount} parameters, got ${params.length}`);
    }

    // Substitute parameters
    let sql = stmt.sql;
    for (let i = params.length; i >= 1; i--) {
      const value = params[i - 1];
      const replacement = this._formatParam(value);
      sql = sql.replace(new RegExp(`\\$${i}`, 'g'), replacement);
    }

    const startTime = performance.now();
    const result = this.db.execute(sql);
    const elapsed = performance.now() - startTime;

    stmt.executionCount++;
    stmt.totalTimeMs += elapsed;
    stmt.lastExecuted = Date.now();
    this._stats.executions++;
    this._stats.cacheHits++;

    return result;
  }

  /**
   * Deallocate (remove) a prepared statement.
   */
  deallocate(name) {
    if (name === 'ALL') {
      const count = this._statements.size;
      this._statements.clear();
      this._stats.deallocations += count;
      return count;
    }

    const lowerName = name.toLowerCase();
    if (!this._statements.has(lowerName)) {
      throw new Error(`Prepared statement '${name}' does not exist`);
    }
    this._statements.delete(lowerName);
    this._stats.deallocations++;
    return 1;
  }

  /**
   * Check if a prepared statement exists.
   */
  has(name) {
    return this._statements.has(name.toLowerCase());
  }

  /**
   * Get statement metadata.
   */
  describe(name) {
    const stmt = this._statements.get(name.toLowerCase());
    if (!stmt) throw new Error(`Prepared statement '${name}' does not exist`);
    return {
      name: stmt.name,
      sql: stmt.sql,
      paramTypes: stmt.paramTypes,
      paramCount: stmt.paramCount,
      executionCount: stmt.executionCount,
      avgTimeMs: stmt.executionCount > 0 ? +(stmt.totalTimeMs / stmt.executionCount).toFixed(3) : 0,
      lastExecuted: stmt.lastExecuted,
    };
  }

  /**
   * List all prepared statements.
   */
  list() {
    return [...this._statements.values()].map(stmt => ({
      name: stmt.name,
      sql: stmt.sql,
      paramCount: stmt.paramCount,
      executionCount: stmt.executionCount,
    }));
  }

  getStats() {
    return {
      ...this._stats,
      activeStatements: this._statements.size,
    };
  }

  _formatParam(value) {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'number') return String(value);
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    // Escape single quotes
    const escaped = String(value).replace(/'/g, "''");
    return `'${escaped}'`;
  }

  _evictLRU() {
    let oldestName = null;
    let oldestTime = Infinity;
    for (const [name, stmt] of this._statements) {
      const lastUsed = stmt.lastExecuted || stmt.createdAt;
      if (lastUsed < oldestTime) {
        oldestTime = lastUsed;
        oldestName = name;
      }
    }
    if (oldestName) {
      this._statements.delete(oldestName);
      this._stats.evictions++;
    }
  }
}
