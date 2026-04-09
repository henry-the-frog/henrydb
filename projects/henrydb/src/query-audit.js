// query-audit.js — Query audit log for HenryDB
// Tracks all SQL statements with timing, categorization, and analysis.

/**
 * QueryAudit — logs and analyzes query execution history.
 * 
 * Usage:
 *   const audit = new QueryAudit({ maxEntries: 1000 });
 *   audit.log({ sql: 'SELECT * FROM users', duration: 12.5, rows: 42 });
 *   audit.slowQueries(100);  // queries > 100ms
 *   audit.summary();          // aggregate statistics
 */
export class QueryAudit {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 10000;
    this._entries = [];
    this._stats = {
      total: 0,
      selects: 0,
      inserts: 0,
      updates: 0,
      deletes: 0,
      ddl: 0,
      errors: 0,
      totalDuration: 0,
    };
  }

  /**
   * Log a query execution.
   * @param {Object} entry - { sql, duration, rows?, error?, user?, tables? }
   */
  log(entry) {
    const type = this._classifySQL(entry.sql);
    const record = {
      id: ++this._stats.total,
      sql: entry.sql,
      type,
      duration: entry.duration || 0,
      rows: entry.rows || 0,
      error: entry.error || null,
      user: entry.user || 'default',
      timestamp: entry.timestamp || Date.now(),
      tables: entry.tables || this._extractTables(entry.sql),
    };
    
    this._entries.push(record);
    this._updateStats(record);
    
    // Evict oldest if over limit
    if (this._entries.length > this.maxEntries) {
      this._entries.shift();
    }
    
    return record;
  }

  /**
   * Classify SQL statement type.
   */
  _classifySQL(sql) {
    const norm = sql.trim().toUpperCase();
    if (norm.startsWith('SELECT') || norm.startsWith('WITH')) return 'SELECT';
    if (norm.startsWith('INSERT')) return 'INSERT';
    if (norm.startsWith('UPDATE')) return 'UPDATE';
    if (norm.startsWith('DELETE')) return 'DELETE';
    if (norm.startsWith('CREATE') || norm.startsWith('DROP') || norm.startsWith('ALTER')) return 'DDL';
    if (norm.startsWith('BEGIN') || norm.startsWith('COMMIT') || norm.startsWith('ROLLBACK')) return 'TCL';
    return 'OTHER';
  }

  /**
   * Extract table names from SQL (basic heuristic).
   */
  _extractTables(sql) {
    const matches = sql.match(/(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+(\w+)/gi);
    if (!matches) return [];
    return [...new Set(matches.map(m => m.split(/\s+/).pop().toLowerCase()))];
  }

  /**
   * Update aggregate stats.
   */
  _updateStats(record) {
    this._stats.totalDuration += record.duration;
    if (record.error) this._stats.errors++;
    switch(record.type) {
      case 'SELECT': this._stats.selects++; break;
      case 'INSERT': this._stats.inserts++; break;
      case 'UPDATE': this._stats.updates++; break;
      case 'DELETE': this._stats.deletes++; break;
      case 'DDL': this._stats.ddl++; break;
    }
  }

  /**
   * Get slow queries (above threshold ms).
   */
  slowQueries(thresholdMs = 100) {
    return this._entries
      .filter(e => e.duration > thresholdMs)
      .sort((a, b) => b.duration - a.duration);
  }

  /**
   * Get most frequent queries.
   */
  frequentQueries(limit = 10) {
    const counts = new Map();
    for (const e of this._entries) {
      const key = e.sql.trim().toLowerCase().replace(/\s+/g, ' ');
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([sql, count]) => ({ sql, count }));
  }

  /**
   * Get queries for a specific table.
   */
  queriesForTable(table) {
    const t = table.toLowerCase();
    return this._entries.filter(e => e.tables.includes(t));
  }

  /**
   * Get error queries.
   */
  errors() {
    return this._entries.filter(e => e.error);
  }

  /**
   * Get recent queries.
   */
  recent(limit = 20) {
    return this._entries.slice(-limit).reverse();
  }

  /**
   * Get aggregate summary.
   */
  summary() {
    const entries = this._entries;
    const durations = entries.map(e => e.duration).filter(d => d > 0);
    
    return {
      ...this._stats,
      entries: entries.length,
      avgDuration: durations.length > 0 ? +(this._stats.totalDuration / durations.length).toFixed(2) : 0,
      maxDuration: durations.length > 0 ? Math.max(...durations) : 0,
      p50: this._percentile(durations, 50),
      p95: this._percentile(durations, 95),
      p99: this._percentile(durations, 99),
      tablesAccessed: [...new Set(entries.flatMap(e => e.tables))],
      errorRate: this._stats.total > 0 ? +(this._stats.errors / this._stats.total * 100).toFixed(1) : 0,
    };
  }

  /**
   * Calculate percentile of sorted array.
   */
  _percentile(arr, p) {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.ceil((p / 100) * sorted.length) - 1;
    return +(sorted[Math.max(0, idx)]).toFixed(2);
  }

  /**
   * Clear all entries.
   */
  clear() {
    this._entries.length = 0;
    Object.keys(this._stats).forEach(k => this._stats[k] = 0);
  }

  /**
   * Get all entries.
   */
  get entries() { return [...this._entries]; }
  get size() { return this._entries.length; }
}
