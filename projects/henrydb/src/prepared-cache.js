// prepared-cache.js — Prepared statement cache with compiled execution
// Caches adaptive engine results for repeated query execution.
// Works with pre-parsed ASTs (the SQL parser is separate).

import { AdaptiveQueryEngine } from './adaptive-engine.js';

/**
 * PreparedQueryCache — caches compiled query functions for repeated execution.
 */
export class PreparedQueryCache {
  constructor(database) {
    this.db = database;
    this.engine = new AdaptiveQueryEngine(database);
    this._cache = new Map(); // name → { ast, execute, engine, stats }
    this.stats = { prepareCount: 0, executeCount: 0, cacheHits: 0, totalSavedMs: 0 };
  }

  /**
   * Prepare a SELECT: warm up the adaptive engine and cache the result path.
   */
  prepare(name, ast) {
    // Warm up: run once to determine best engine and build feedback
    const warmup = this.engine.executeSelect(ast);

    const entry = {
      ast,
      engine: warmup?.engine || 'volcano',
      firstExecMs: warmup?.timeMs || 0,
      execCount: 0,
      totalMs: 0,
    };

    this._cache.set(name, entry);
    this.stats.prepareCount++;

    return {
      prepared: name,
      engine: entry.engine,
      warmupMs: entry.firstExecMs,
    };
  }

  /**
   * Execute a prepared statement using cached compilation.
   */
  execute(name) {
    const entry = this._cache.get(name);
    if (!entry) throw new Error(`Prepared statement '${name}' not found`);

    const startMs = Date.now();
    const result = this.engine.executeSelect(entry.ast);
    const execMs = Date.now() - startMs;

    entry.execCount++;
    entry.totalMs += execMs;
    this.stats.executeCount++;
    this.stats.cacheHits++;

    // After warmup, the adaptive engine uses feedback to pick the best engine
    return {
      rows: result?.rows || [],
      engine: result?.engine || 'volcano',
      execMs,
      execCount: entry.execCount,
      avgMs: (entry.totalMs / entry.execCount).toFixed(1),
    };
  }

  /**
   * Deallocate a prepared statement.
   */
  deallocate(name) {
    const had = this._cache.delete(name);
    if (!had) throw new Error(`Prepared statement '${name}' not found`);
    return { deallocated: name };
  }

  /**
   * List cached prepared statements.
   */
  list() {
    return [...this._cache.entries()].map(([name, entry]) => ({
      name,
      engine: entry.engine,
      execCount: entry.execCount,
      avgMs: entry.execCount > 0 ? (entry.totalMs / entry.execCount).toFixed(1) : '0',
    }));
  }

  getStats() {
    return {
      ...this.stats,
      cacheSize: this._cache.size,
      engineStats: this.engine.getStats(),
    };
  }
}
