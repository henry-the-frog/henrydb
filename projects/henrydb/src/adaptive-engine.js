// adaptive-engine.js — Adaptive query execution for HenryDB
// Automatically selects the best execution strategy based on query characteristics:
// - Vectorized: best for large scans, joins with high selectivity, analytics
// - Compiled (codegen): best for selective queries, small-medium joins
// - Compiled (closure): fallback for moderate workloads
// - Volcano: fallback for unsupported patterns (subqueries, etc.)
//
// Uses cost-based decisions from the planner plus runtime feedback.

import { QueryPlanner } from './planner.js';
import { VectorizedCodeGen } from './vectorized-codegen.js';
import { QueryCodeGen } from './query-codegen.js';
import { CompiledQueryEngine } from './compiled-query.js';

/**
 * AdaptiveQueryEngine — picks the best execution strategy per query.
 */
export class AdaptiveQueryEngine {
  constructor(database, { compileThreshold = 5000 } = {}) {
    this.db = database;
    this.planner = new QueryPlanner(database);
    this.vectorized = new VectorizedCodeGen(database);
    this.codegen = new QueryCodeGen(database);
    this.compiled = new CompiledQueryEngine(database, { compileThreshold });
    this._compileThreshold = compileThreshold;
    
    this.stats = {
      total: 0,
      vectorized: 0,
      codegen: 0,
      compiled: 0,
      volcano: 0,
      totalMs: 0,
      decisions: [],
    };
    
    // Runtime feedback: track which engine was fastest for similar queries
    this._feedback = new Map(); // query shape hash → { vectorized: ms, codegen: ms, ... }
  }

  /**
   * Execute a SELECT with adaptive strategy selection.
   * Returns { rows: [...], engine: string, timeMs: number }
   */
  executeSelect(ast) {
    const startMs = Date.now();
    this.stats.total++;

    // 1. Analyze the query
    const analysis = this._analyzeQuery(ast);

    // 2. Check runtime feedback for this query shape
    const shapeKey = this._queryShapeKey(ast);
    const feedback = this._feedback.get(shapeKey);

    // 3. Select engine
    let engine;
    if (feedback && feedback.samples >= 3) {
      // Use the fastest observed engine
      engine = this._selectFromFeedback(feedback);
    } else {
      // Cost-based selection
      engine = this._selectEngine(analysis);
    }

    // 4. Execute with selected engine
    let result;
    let selectedEngine = engine;

    try {
      switch (engine) {
        case 'vectorized':
          result = this.vectorized.execute(ast);
          break;
        case 'codegen':
          result = this.codegen.execute(ast);
          break;
        case 'compiled':
          result = this.compiled.executeSelect(ast);
          break;
        default:
          result = null;
      }
    } catch (e) {
      result = null;
    }

    // 5. Fallback chain: vectorized → codegen → compiled → volcano
    if (!result && engine !== 'codegen') {
      result = this.codegen.execute(ast);
      if (result) selectedEngine = 'codegen';
    }
    if (!result && engine !== 'compiled') {
      result = this.compiled.executeSelect(ast);
      if (result) selectedEngine = 'compiled';
    }
    if (!result) {
      // Volcano fallback — use standard db.execute
      selectedEngine = 'volcano';
    }

    const timeMs = Date.now() - startMs;

    // 6. Update stats
    this.stats[selectedEngine]++;
    this.stats.totalMs += timeMs;
    this.stats.decisions.push({
      shape: shapeKey,
      chosen: engine,
      actual: selectedEngine,
      timeMs,
      analysis: analysis.reason,
    });
    // Keep only last 100 decisions
    if (this.stats.decisions.length > 100) {
      this.stats.decisions = this.stats.decisions.slice(-50);
    }

    // 7. Update feedback
    this._updateFeedback(shapeKey, selectedEngine, timeMs);

    if (result) {
      return { rows: result.rows, engine: selectedEngine, timeMs };
    }

    // True fallback — shouldn't normally reach here
    return null;
  }

  /**
   * Analyze query characteristics for engine selection.
   */
  _analyzeQuery(ast) {
    const tableName = ast.from?.table;
    const table = tableName ? this.db.tables.get(tableName) : null;
    const tableStats = tableName ? this.planner.getStats(tableName) : null;
    const rowCount = tableStats?.rowCount || 0;
    const joinCount = ast.joins?.length || 0;
    const hasAggregation = ast.columns?.some(c => c.aggregate || c.fn);
    const hasLimit = !!ast.limit?.value;
    const limitValue = ast.limit?.value || Infinity;
    const hasWhere = !!ast.where;
    const hasSubquery = ast.where?.subquery || false;
    const hasGroupBy = !!ast.groupBy;

    // Estimate selectivity
    let estimatedSelectivity = 1.0;
    if (hasWhere && rowCount > 0) {
      const plan = this.planner.plan(ast);
      estimatedSelectivity = (plan.estimatedRows || rowCount) / rowCount;
    }

    const analysis = {
      tableName,
      rowCount,
      joinCount,
      hasAggregation,
      hasLimit,
      limitValue,
      hasWhere,
      hasSubquery,
      hasGroupBy,
      estimatedSelectivity,
      reason: '',
    };

    return analysis;
  }

  /**
   * Select execution engine based on query analysis.
   */
  _selectEngine(analysis) {
    const { rowCount, joinCount, hasAggregation, hasLimit, limitValue,
            hasWhere, hasSubquery, estimatedSelectivity } = analysis;

    // Can't compile subqueries
    if (hasSubquery) {
      analysis.reason = 'subquery → volcano';
      return 'volcano';
    }

    // Tiny tables: not worth compiling. Benchmarking (Apr 21) showed compiled
    // engine is 3x slower than interpreter at 1K rows.
    if (rowCount < this._compileThreshold) {
      analysis.reason = `small table (${rowCount} rows, threshold ${this._compileThreshold}) → volcano`;
      return 'volcano';
    }

    // Large scans without selective filters: vectorized wins
    if (rowCount > 500 && estimatedSelectivity > 0.3 && !hasLimit) {
      analysis.reason = `large scan (${rowCount} rows, ${(estimatedSelectivity * 100).toFixed(0)}% sel) → vectorized`;
      return 'vectorized';
    }

    // Joins: vectorized for large, codegen for medium
    if (joinCount > 0) {
      if (rowCount > 200) {
        analysis.reason = `join (${joinCount} tables, ${rowCount} rows) → vectorized`;
        return 'vectorized';
      }
      analysis.reason = `small join (${joinCount} tables, ${rowCount} rows) → codegen`;
      return 'codegen';
    }

    // Highly selective queries (LIMIT or narrow filter): codegen
    if (hasLimit && limitValue < rowCount * 0.1) {
      analysis.reason = `selective LIMIT ${limitValue}/${rowCount} → codegen`;
      return 'codegen';
    }

    if (estimatedSelectivity < 0.1) {
      analysis.reason = `highly selective (${(estimatedSelectivity * 100).toFixed(1)}%) → codegen`;
      return 'codegen';
    }

    // Analytics: vectorized
    if (hasAggregation) {
      analysis.reason = `aggregation → vectorized`;
      return 'vectorized';
    }

    // Default: vectorized for large tables, compiled for medium
    if (rowCount > 500) {
      analysis.reason = `default large (${rowCount} rows) → vectorized`;
      return 'vectorized';
    }

    analysis.reason = `default medium (${rowCount} rows) → compiled`;
    return 'compiled';
  }

  /**
   * Select engine from runtime feedback (use fastest observed).
   */
  _selectFromFeedback(feedback) {
    let best = 'compiled';
    let bestMs = Infinity;

    for (const [engine, data] of Object.entries(feedback.engines)) {
      if (data.avgMs < bestMs) {
        bestMs = data.avgMs;
        best = engine;
      }
    }

    return best;
  }

  /**
   * Update runtime feedback for a query shape.
   */
  _updateFeedback(shapeKey, engine, timeMs) {
    if (!this._feedback.has(shapeKey)) {
      this._feedback.set(shapeKey, { samples: 0, engines: {} });
    }
    const fb = this._feedback.get(shapeKey);
    fb.samples++;

    if (!fb.engines[engine]) {
      fb.engines[engine] = { totalMs: 0, count: 0, avgMs: 0 };
    }
    const e = fb.engines[engine];
    e.totalMs += timeMs;
    e.count++;
    e.avgMs = e.totalMs / e.count;
  }

  /**
   * Generate a shape key for a query (for feedback caching).
   * Queries with similar structure get the same key.
   */
  _queryShapeKey(ast) {
    const parts = [];
    parts.push(ast.from?.table || '?');
    parts.push(ast.joins?.length || 0);
    parts.push(ast.where ? 'W' : '');
    parts.push(ast.limit ? `L${ast.limit.value}` : '');
    parts.push(ast.groupBy ? 'G' : '');
    parts.push(ast.columns?.some(c => c.aggregate || c.fn) ? 'A' : '');
    return parts.join(':');
  }

  /**
   * Get engine selection statistics.
   */
  getStats() {
    return {
      ...this.stats,
      decisions: undefined, // Exclude verbose decisions from summary
      breakdown: {
        vectorized: `${this.stats.vectorized}/${this.stats.total}`,
        codegen: `${this.stats.codegen}/${this.stats.total}`,
        compiled: `${this.stats.compiled}/${this.stats.total}`,
        volcano: `${this.stats.volcano}/${this.stats.total}`,
      },
      avgMs: this.stats.total > 0 ? (this.stats.totalMs / this.stats.total).toFixed(1) : 0,
    };
  }

  /**
   * Get the last N engine decisions (for debugging/EXPLAIN).
   */
  getDecisions(n = 10) {
    return this.stats.decisions.slice(-n);
  }
}
