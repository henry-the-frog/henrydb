// materialized-views.js — Materialized view system for HenryDB
// CREATE MATERIALIZED VIEW, REFRESH, stale detection, dependency tracking.

/**
 * MaterializedView — stores pre-computed query results.
 */
class MaterializedView {
  constructor(name, sql, db) {
    this.name = name;
    this.sql = sql;
    this.db = db;
    this.rows = [];
    this.columns = [];
    this.lastRefresh = null;
    this.isPopulated = false;
    this.stale = true;
    this.dependentTables = extractTablesFromSQL(sql);
    this.refreshCount = 0;
    this.rowCount = 0;
  }

  /**
   * Refresh the materialized view by re-executing the query.
   */
  refresh() {
    const result = this.db.execute(this.sql);
    this.rows = result.rows || [];
    this.columns = this.rows.length > 0 ? Object.keys(this.rows[0]) : [];
    this.lastRefresh = Date.now();
    this.isPopulated = true;
    this.stale = false;
    this.refreshCount++;
    this.rowCount = this.rows.length;
    return { rowCount: this.rows.length };
  }

  /**
   * Query the materialized view's stored data.
   */
  query(filter = null) {
    if (!this.isPopulated) {
      throw new Error(`Materialized view '${this.name}' has not been populated. Use REFRESH.`);
    }

    if (!filter) return { rows: [...this.rows], stale: this.stale };

    const filtered = this.rows.filter(row => {
      return Object.entries(filter).every(([col, val]) => row[col] === val);
    });
    return { rows: filtered, stale: this.stale };
  }

  /**
   * Mark as stale (called when dependent tables change).
   */
  markStale() {
    this.stale = true;
  }

  getStats() {
    return {
      name: this.name,
      sql: this.sql,
      isPopulated: this.isPopulated,
      stale: this.stale,
      rowCount: this.rowCount,
      columns: this.columns,
      lastRefresh: this.lastRefresh,
      refreshCount: this.refreshCount,
      dependentTables: [...this.dependentTables],
    };
  }
}

/**
 * MaterializedViewManager — manages all materialized views.
 */
export class MaterializedViewManager {
  constructor(db) {
    this.db = db;
    this._views = new Map(); // name → MaterializedView
    this._tableDeps = new Map(); // table → Set<view names>
  }

  /**
   * Create a new materialized view.
   */
  create(name, sql, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._views.has(lowerName) && !options.orReplace) {
      throw new Error(`Materialized view '${name}' already exists`);
    }

    const mv = new MaterializedView(lowerName, sql, this.db);
    this._views.set(lowerName, mv);

    // Register table dependencies
    for (const table of mv.dependentTables) {
      if (!this._tableDeps.has(table)) {
        this._tableDeps.set(table, new Set());
      }
      this._tableDeps.get(table).add(lowerName);
    }

    // Auto-populate unless WITH NO DATA
    if (!options.noData) {
      mv.refresh();
    }

    return mv.getStats();
  }

  /**
   * Refresh a materialized view.
   */
  refresh(name, options = {}) {
    const mv = this._views.get(name.toLowerCase());
    if (!mv) throw new Error(`Materialized view '${name}' does not exist`);

    if (options.concurrently && !mv.isPopulated) {
      throw new Error('REFRESH CONCURRENTLY requires the view to be already populated');
    }

    return mv.refresh();
  }

  /**
   * Query a materialized view.
   */
  query(name, filter = null) {
    const mv = this._views.get(name.toLowerCase());
    if (!mv) throw new Error(`Materialized view '${name}' does not exist`);
    return mv.query(filter);
  }

  /**
   * Drop a materialized view.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    const mv = this._views.get(lowerName);
    if (!mv) {
      if (ifExists) return false;
      throw new Error(`Materialized view '${name}' does not exist`);
    }

    // Remove table dependencies
    for (const table of mv.dependentTables) {
      const deps = this._tableDeps.get(table);
      if (deps) {
        deps.delete(lowerName);
        if (deps.size === 0) this._tableDeps.delete(table);
      }
    }

    this._views.delete(lowerName);
    return true;
  }

  /**
   * Notify that a table has been modified.
   * Marks all dependent materialized views as stale.
   */
  notifyTableChange(tableName) {
    const lowerTable = tableName.toLowerCase();
    const deps = this._tableDeps.get(lowerTable);
    if (!deps) return 0;

    let staleCount = 0;
    for (const viewName of deps) {
      const mv = this._views.get(viewName);
      if (mv) {
        mv.markStale();
        staleCount++;
      }
    }
    return staleCount;
  }

  /**
   * Get all stale views.
   */
  getStaleViews() {
    const stale = [];
    for (const mv of this._views.values()) {
      if (mv.stale) stale.push(mv.name);
    }
    return stale;
  }

  /**
   * Refresh all stale views.
   */
  refreshAllStale() {
    const refreshed = [];
    for (const mv of this._views.values()) {
      if (mv.stale) {
        mv.refresh();
        refreshed.push(mv.name);
      }
    }
    return refreshed;
  }

  /**
   * List all materialized views.
   */
  list() {
    return [...this._views.values()].map(mv => mv.getStats());
  }

  has(name) {
    return this._views.has(name.toLowerCase());
  }
}

/**
 * Extract table names from SQL.
 */
function extractTablesFromSQL(sql) {
  const tables = new Set();
  const upper = sql.toUpperCase();
  
  const fromMatch = upper.match(/\bFROM\s+(\w+)/g);
  if (fromMatch) {
    for (const m of fromMatch) {
      const table = m.match(/FROM\s+(\w+)/i);
      if (table) tables.add(table[1].toLowerCase());
    }
  }

  const joinMatch = upper.match(/\bJOIN\s+(\w+)/g);
  if (joinMatch) {
    for (const m of joinMatch) {
      const table = m.match(/JOIN\s+(\w+)/i);
      if (table) tables.add(table[1].toLowerCase());
    }
  }

  return tables;
}
