// profiler.js — Query profiler for HenryDB
// Tracks query execution metrics: parse time, plan time, execution time, rows scanned.

/**
 * Query Profiler: instruments database operations.
 */
export class QueryProfiler {
  constructor() {
    this._queries = [];
    this._slowThresholdMs = 100;
  }

  /**
   * Profile a query execution.
   */
  profile(sql, executeFn) {
    const parseStart = performance.now();
    const entry = {
      sql: sql.length > 200 ? sql.slice(0, 200) + '...' : sql,
      timestamp: Date.now(),
    };

    try {
      const execStart = performance.now();
      entry.parseTimeMs = execStart - parseStart;
      
      const result = executeFn();
      
      entry.execTimeMs = performance.now() - execStart;
      entry.totalTimeMs = performance.now() - parseStart;
      entry.rowsReturned = result?.rows?.length ?? 0;
      entry.success = true;
      entry.slow = entry.totalTimeMs > this._slowThresholdMs;
    } catch (err) {
      entry.totalTimeMs = performance.now() - parseStart;
      entry.success = false;
      entry.error = err.message;
    }

    this._queries.push(entry);
    return entry;
  }

  /**
   * Get slow queries.
   */
  slowQueries() {
    return this._queries.filter(q => q.slow);
  }

  /**
   * Get query statistics.
   */
  stats() {
    if (this._queries.length === 0) return null;
    
    const successful = this._queries.filter(q => q.success);
    const times = successful.map(q => q.totalTimeMs);
    
    return {
      totalQueries: this._queries.length,
      successCount: successful.length,
      errorCount: this._queries.filter(q => !q.success).length,
      avgTimeMs: times.length ? times.reduce((a, b) => a + b, 0) / times.length : 0,
      maxTimeMs: times.length ? Math.max(...times) : 0,
      minTimeMs: times.length ? Math.min(...times) : 0,
      p95TimeMs: this._percentile(times, 95),
      p99TimeMs: this._percentile(times, 99),
      slowQueryCount: this.slowQueries().length,
    };
  }

  /**
   * Reset profiler.
   */
  reset() {
    this._queries = [];
  }

  /**
   * Set slow query threshold.
   */
  setSlowThreshold(ms) {
    this._slowThresholdMs = ms;
  }

  get queryCount() { return this._queries.length; }

  _percentile(sorted, p) {
    if (sorted.length === 0) return 0;
    const s = [...sorted].sort((a, b) => a - b);
    const idx = Math.ceil(s.length * p / 100) - 1;
    return s[Math.max(0, idx)];
  }
}
