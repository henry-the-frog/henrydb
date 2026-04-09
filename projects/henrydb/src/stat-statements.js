// stat-statements.js — pg_stat_statements equivalent for HenryDB
// Tracks normalized query execution statistics.

import { normalizeSQL } from './plan-cache.js';

/**
 * StatStatements — tracks execution statistics for normalized queries.
 */
export class StatStatements {
  constructor(options = {}) {
    this.maxStatements = options.maxStatements || 5000;
    this._statements = new Map(); // normalizedSQL → stats
    this._queryIdCounter = 1;
  }

  /**
   * Record a query execution.
   */
  record(sql, options = {}) {
    const normalized = normalizeSQL(sql);
    let entry = this._statements.get(normalized);

    if (!entry) {
      if (this._statements.size >= this.maxStatements) {
        this._evictLeastUsed();
      }
      entry = {
        queryId: this._queryIdCounter++,
        query: normalized,
        calls: 0,
        totalTimeMs: 0,
        minTimeMs: Infinity,
        maxTimeMs: 0,
        meanTimeMs: 0,
        sumSquaresMs: 0, // For stddev calculation
        rows: 0,
        sharedBlksHit: 0,
        sharedBlksRead: 0,
        firstSeen: Date.now(),
        lastSeen: null,
      };
      this._statements.set(normalized, entry);
    }

    const execTime = options.executionTimeMs || 0;
    const rowCount = options.rows || 0;

    entry.calls++;
    entry.totalTimeMs += execTime;
    entry.rows += rowCount;
    entry.lastSeen = Date.now();

    if (execTime < entry.minTimeMs) entry.minTimeMs = execTime;
    if (execTime > entry.maxTimeMs) entry.maxTimeMs = execTime;

    // Welford's online algorithm for mean and variance
    const oldMean = entry.meanTimeMs;
    entry.meanTimeMs = entry.totalTimeMs / entry.calls;
    entry.sumSquaresMs += (execTime - oldMean) * (execTime - entry.meanTimeMs);

    if (options.blksHit) entry.sharedBlksHit += options.blksHit;
    if (options.blksRead) entry.sharedBlksRead += options.blksRead;

    return entry;
  }

  /**
   * Get statistics for all tracked queries.
   */
  getAll(options = {}) {
    let entries = [...this._statements.values()].map(e => ({
      ...e,
      stddevTimeMs: e.calls > 1
        ? +Math.sqrt(e.sumSquaresMs / (e.calls - 1)).toFixed(3)
        : 0,
      meanTimeMs: +e.meanTimeMs.toFixed(3),
      minTimeMs: e.minTimeMs === Infinity ? 0 : +e.minTimeMs.toFixed(3),
      maxTimeMs: +e.maxTimeMs.toFixed(3),
      totalTimeMs: +e.totalTimeMs.toFixed(3),
      avgRows: e.calls > 0 ? +(e.rows / e.calls).toFixed(1) : 0,
    }));

    // Sort
    const sortBy = options.sortBy || 'total_time';
    switch (sortBy) {
      case 'total_time': entries.sort((a, b) => b.totalTimeMs - a.totalTimeMs); break;
      case 'calls': entries.sort((a, b) => b.calls - a.calls); break;
      case 'mean_time': entries.sort((a, b) => b.meanTimeMs - a.meanTimeMs); break;
      case 'max_time': entries.sort((a, b) => b.maxTimeMs - a.maxTimeMs); break;
      case 'rows': entries.sort((a, b) => b.rows - a.rows); break;
    }

    if (options.limit) entries = entries.slice(0, options.limit);
    return entries;
  }

  /**
   * Get a specific query's stats by normalized SQL.
   */
  get(sql) {
    return this._statements.get(normalizeSQL(sql)) || null;
  }

  /**
   * Reset all statistics.
   */
  reset() {
    const count = this._statements.size;
    this._statements.clear();
    return count;
  }

  /**
   * Reset stats for a specific query.
   */
  resetQuery(sql) {
    return this._statements.delete(normalizeSQL(sql));
  }

  /**
   * Get summary statistics.
   */
  getSummary() {
    let totalCalls = 0;
    let totalTime = 0;
    let totalRows = 0;

    for (const entry of this._statements.values()) {
      totalCalls += entry.calls;
      totalTime += entry.totalTimeMs;
      totalRows += entry.rows;
    }

    return {
      uniqueQueries: this._statements.size,
      totalCalls,
      totalTimeMs: +totalTime.toFixed(3),
      totalRows,
      avgCallsPerQuery: this._statements.size > 0
        ? +(totalCalls / this._statements.size).toFixed(1)
        : 0,
    };
  }

  /**
   * Get top N queries by a metric.
   */
  topN(n, metric = 'total_time') {
    return this.getAll({ sortBy: metric, limit: n });
  }

  _evictLeastUsed() {
    let leastKey = null;
    let leastCalls = Infinity;
    for (const [key, entry] of this._statements) {
      if (entry.calls < leastCalls) {
        leastCalls = entry.calls;
        leastKey = key;
      }
    }
    if (leastKey) this._statements.delete(leastKey);
  }
}
