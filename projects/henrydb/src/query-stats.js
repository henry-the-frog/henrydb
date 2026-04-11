// query-stats.js — Query statistics collector (pg_stat_statements equivalent)
//
// Tracks execution statistics for normalized queries:
// - Call count
// - Total/min/max/mean execution time
// - Total/min/max/mean rows returned
// - First/last execution time
//
// Accessible via: SELECT * FROM pg_stat_statements (virtual table)

import { normalizeSQL } from './plan-cache.js';

/**
 * QueryStats — per-query execution statistics.
 */
class QueryStatsEntry {
  constructor(normalizedQuery) {
    this.query = normalizedQuery;
    this.calls = 0;
    this.totalTime = 0;  // ms
    this.minTime = Infinity;
    this.maxTime = 0;
    this.totalRows = 0;
    this.minRows = Infinity;
    this.maxRows = 0;
    this.firstSeen = Date.now();
    this.lastSeen = 0;
    this.errors = 0;
  }

  record(timeMs, rows) {
    this.calls++;
    this.totalTime += timeMs;
    this.minTime = Math.min(this.minTime, timeMs);
    this.maxTime = Math.max(this.maxTime, timeMs);
    this.totalRows += rows;
    this.minRows = Math.min(this.minRows, rows);
    this.maxRows = Math.max(this.maxRows, rows);
    this.lastSeen = Date.now();
  }

  recordError() {
    this.calls++;
    this.errors++;
    this.lastSeen = Date.now();
  }

  get meanTime() { return this.calls > 0 ? this.totalTime / this.calls : 0; }
  get meanRows() { return this.calls > 0 ? this.totalRows / this.calls : 0; }
}

/**
 * QueryStatsCollector — collects and manages query statistics.
 */
export class QueryStatsCollector {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 1000;
    this._stats = new Map(); // normalizedQuery → QueryStatsEntry
    this._enabled = true;
  }

  /**
   * Record a query execution.
   */
  record(sql, timeMs, rowCount) {
    if (!this._enabled) return;
    
    const key = normalizeSQL(sql);
    let entry = this._stats.get(key);
    if (!entry) {
      // Evict oldest entry if at capacity
      if (this._stats.size >= this.maxEntries) {
        this._evictOldest();
      }
      entry = new QueryStatsEntry(key);
      this._stats.set(key, entry);
    }
    entry.record(timeMs, rowCount);
  }

  /**
   * Record a query error.
   */
  recordError(sql) {
    if (!this._enabled) return;
    
    const key = normalizeSQL(sql);
    let entry = this._stats.get(key);
    if (!entry) {
      entry = new QueryStatsEntry(key);
      this._stats.set(key, entry);
    }
    entry.recordError();
  }

  /**
   * Get all statistics, sorted by total time (descending).
   */
  getAll(options = {}) {
    const entries = [...this._stats.values()].map(e => ({
      query: e.query,
      calls: e.calls,
      total_time_ms: Math.round(e.totalTime * 1000) / 1000,
      mean_time_ms: Math.round(e.meanTime * 1000) / 1000,
      min_time_ms: Math.round(e.minTime * 1000) / 1000,
      max_time_ms: Math.round((e.maxTime === 0 ? 0 : e.maxTime) * 1000) / 1000,
      total_rows: e.totalRows,
      mean_rows: Math.round(e.meanRows * 10) / 10,
      errors: e.errors,
    }));

    const sortBy = options.sortBy || 'total_time';
    switch (sortBy) {
      case 'calls': entries.sort((a, b) => b.calls - a.calls); break;
      case 'mean_time': entries.sort((a, b) => b.mean_time_ms - a.mean_time_ms); break;
      case 'total_rows': entries.sort((a, b) => b.total_rows - a.total_rows); break;
      default: entries.sort((a, b) => b.total_time_ms - a.total_time_ms);
    }

    const limit = options.limit || entries.length;
    return entries.slice(0, limit);
  }

  /**
   * Get top N slowest queries.
   */
  getSlowest(n = 10) {
    return this.getAll({ sortBy: 'mean_time', limit: n });
  }

  /**
   * Get top N most called queries.
   */
  getMostCalled(n = 10) {
    return this.getAll({ sortBy: 'calls', limit: n });
  }

  /**
   * Get summary statistics.
   */
  summary() {
    const all = this.getAll();
    const totalCalls = all.reduce((s, e) => s + e.calls, 0);
    const totalTime = all.reduce((s, e) => s + e.total_time_ms, 0);
    const totalErrors = all.reduce((s, e) => s + e.errors, 0);
    
    return {
      uniqueQueries: all.length,
      totalCalls,
      totalTimeMs: Math.round(totalTime * 1000) / 1000,
      totalErrors,
      avgCallsPerQuery: all.length > 0 ? Math.round(totalCalls / all.length * 10) / 10 : 0,
    };
  }

  /**
   * Reset all statistics.
   */
  reset() {
    this._stats.clear();
  }

  /**
   * Enable/disable collection.
   */
  enable() { this._enabled = true; }
  disable() { this._enabled = false; }

  _evictOldest() {
    let oldestKey = null;
    let oldestTime = Infinity;
    for (const [key, entry] of this._stats) {
      if (entry.lastSeen < oldestTime) {
        oldestTime = entry.lastSeen;
        oldestKey = key;
      }
    }
    if (oldestKey) this._stats.delete(oldestKey);
  }
}
