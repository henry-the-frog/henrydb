// materialized-view.js — Materialized views with auto-refresh
// A materialized view caches the result of a query.
// It can be refreshed manually or automatically when base tables change.

export class MaterializedView {
  constructor(name, query, options = {}) {
    this.name = name;
    this.query = query; // Function: () => Array<Object>
    this._data = null;
    this._lastRefreshed = null;
    this._stale = true;
    this.refreshMode = options.refreshMode || 'manual'; // 'manual' | 'eager' | 'lazy'
    this.ttlMs = options.ttlMs || 0; // 0 = no expiry
    this.stats = { refreshes: 0, reads: 0, staleReads: 0 };
  }

  refresh() {
    this._data = this.query();
    this._lastRefreshed = Date.now();
    this._stale = false;
    this.stats.refreshes++;
    return this._data;
  }

  read() {
    this.stats.reads++;
    
    if (this._data === null || (this.refreshMode === 'lazy' && this._stale)) {
      this.refresh();
    }

    if (this.ttlMs > 0 && this._lastRefreshed && Date.now() - this._lastRefreshed > this.ttlMs) {
      this._stale = true;
      if (this.refreshMode === 'lazy') this.refresh();
    }

    if (this._stale) this.stats.staleReads++;
    return this._data;
  }

  invalidate() { this._stale = true; }
  get isStale() { return this._stale; }
  get lastRefreshed() { return this._lastRefreshed; }
  get rowCount() { return this._data ? this._data.length : 0; }
}

/**
 * MaterializedViewManager — manages multiple views with dependency tracking.
 */
export class MaterializedViewManager {
  constructor() {
    this._views = new Map();
    this._dependencies = new Map(); // tableName → Set<viewName>
  }

  create(name, query, baseTables = [], options = {}) {
    const view = new MaterializedView(name, query, options);
    this._views.set(name, view);
    
    for (const table of baseTables) {
      if (!this._dependencies.has(table)) this._dependencies.set(table, new Set());
      this._dependencies.get(table).add(name);
    }

    if (options.refreshMode === 'eager') view.refresh();
    return view;
  }

  get(name) {
    const view = this._views.get(name);
    return view ? view.read() : null;
  }

  refresh(name) {
    const view = this._views.get(name);
    return view ? view.refresh() : null;
  }

  refreshAll() {
    for (const view of this._views.values()) view.refresh();
  }

  /**
   * Notify that a base table changed. Invalidates dependent views.
   */
  notifyTableChange(tableName) {
    const deps = this._dependencies.get(tableName);
    if (!deps) return;

    for (const viewName of deps) {
      const view = this._views.get(viewName);
      if (view) {
        view.invalidate();
        if (view.refreshMode === 'eager') view.refresh();
      }
    }
  }

  drop(name) {
    this._views.delete(name);
    for (const deps of this._dependencies.values()) deps.delete(name);
  }

  list() {
    return [...this._views.entries()].map(([name, v]) => ({
      name,
      rows: v.rowCount,
      stale: v.isStale,
      lastRefreshed: v.lastRefreshed,
      refreshMode: v.refreshMode,
    }));
  }
}
