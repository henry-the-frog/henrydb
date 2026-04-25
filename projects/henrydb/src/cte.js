// cte.js — Common Table Expression (CTE) support
// Implements WITH clause: WITH name AS (SELECT ...) SELECT ... FROM name
// Supports multiple CTEs and CTE-to-CTE references.

import { median } from './percentile.js';

/**
 * CTEResolver — resolves Common Table Expressions before query execution.
 * 
 * Takes a query with CTEs and returns a plan where CTE references
 * are replaced with materialized temporary tables.
 */
export class CTEResolver {
  constructor(executor) {
    // executor: function that takes a SQL-like query object and returns rows
    this._executor = executor;
    this._cache = new Map(); // CTE name → materialized rows
  }

  /**
   * Resolve a query with CTEs.
   * 
   * @param {Object} query - { ctes: [{name, query}], mainQuery }
   * @returns {Array<Object>} Final result rows
   */
  resolve(query) {
    this._cache.clear();

    if (query.ctes && query.ctes.length > 0) {
      // Materialize each CTE in order (later CTEs can reference earlier ones)
      for (const cte of query.ctes) {
        const rows = this._executeCTE(cte);
        this._cache.set(cte.name, rows);
      }
    }

    // Execute main query with CTEs as virtual tables
    return this._executeWithCTEs(query.mainQuery);
  }

  /**
   * Execute a CTE, allowing references to previously materialized CTEs.
   */
  _executeCTE(cte) {
    return this._executeWithCTEs(cte.query);
  }

  /**
   * Execute a query, replacing CTE references with cached data.
   */
  _executeWithCTEs(query) {
    // Replace table references with CTE data
    const resolvedQuery = this._resolveReferences(query);
    return this._executor(resolvedQuery, this._cache);
  }

  _resolveReferences(query) {
    if (!query) return query;
    return { ...query, _cteData: this._cache };
  }

  /**
   * Get materialized CTE data by name.
   */
  getCTEData(name) {
    return this._cache.get(name);
  }
}

/**
 * Simple CTE executor that works with in-memory row arrays.
 * Supports basic SELECT, WHERE, JOIN between CTEs and base tables.
 */
export class SimpleCTEEngine {
  constructor() {
    this._tables = new Map(); // name → rows
  }

  /**
   * Register a base table.
   */
  addTable(name, rows) {
    this._tables.set(name, rows);
  }

  /**
   * Execute a query with CTE support.
   * 
   * @param {Object} query
   *   { ctes: [{name, select, from, where}], select, from, where, join, groupBy, orderBy }
   */
  execute(query) {
    const resolver = new CTEResolver((q, cteData) => {
      return this._executeQuery(q, cteData);
    });
    return resolver.resolve(query);
  }

  _executeQuery(query, cteData = new Map()) {
    const allTables = new Map([...this._tables, ...cteData]);

    // FROM clause
    let rows = [];
    if (query.from) {
      const tableName = query.from;
      rows = allTables.get(tableName) || [];
      rows = rows.map(r => ({ ...r })); // Clone
    }

    // JOIN
    if (query.join) {
      const { table, on } = query.join;
      const rightRows = allTables.get(table) || [];
      const joined = [];
      for (const left of rows) {
        for (const right of rightRows) {
          if (this._evaluateJoinCondition(left, right, on)) {
            joined.push({ ...left, ...right });
          }
        }
      }
      rows = joined;
    }

    // WHERE
    if (query.where) {
      rows = rows.filter(row => this._evaluateCondition(row, query.where));
    }

    // GROUP BY + aggregates
    if (query.groupBy) {
      rows = this._groupBy(rows, query.groupBy, query.select);
    }

    // SELECT (projection)
    if (query.select && !query.groupBy) {
      if (query.select !== '*') {
        rows = rows.map(row => {
          const result = {};
          for (const col of query.select) {
            if (typeof col === 'string') {
              result[col] = row[col];
            } else if (col.expr && col.alias) {
              result[col.alias] = this._evalExpr(row, col.expr);
            }
          }
          return result;
        });
      }
    }

    // ORDER BY
    if (query.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction = 'ASC' } of query.orderBy) {
          const va = a[column], vb = b[column];
          let cmp = va < vb ? -1 : va > vb ? 1 : 0;
          if (direction === 'DESC') cmp = -cmp;
          if (cmp !== 0) return cmp;
        }
        return 0;
      });
    }

    // LIMIT
    if (query.limit !== undefined) {
      rows = rows.slice(0, query.limit);
    }

    return rows;
  }

  _evaluateCondition(row, cond) {
    if (cond.op === 'EQ') return row[cond.left] === cond.right;
    if (cond.op === 'GT') return row[cond.left] > cond.right;
    if (cond.op === 'LT') return row[cond.left] < cond.right;
    if (cond.op === 'GE') return row[cond.left] >= cond.right;
    if (cond.op === 'LE') return row[cond.left] <= cond.right;
    if (cond.op === 'AND') return this._evaluateCondition(row, cond.left) && this._evaluateCondition(row, cond.right);
    if (cond.op === 'COL_EQ') return row[cond.left] === row[cond.right];
    return true;
  }

  _evaluateJoinCondition(left, right, on) {
    return left[on.left] === right[on.right];
  }

  _groupBy(rows, groupCols, selectCols) {
    const groups = new Map();
    for (const row of rows) {
      const key = groupCols.map(c => row[c]).join('|');
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(row);
    }

    return [...groups.values()].map(group => {
      const result = {};
      // Group key columns
      for (const col of groupCols) result[col] = group[0][col];
      // Aggregates
      if (selectCols) {
        for (const col of selectCols) {
          if (typeof col === 'object' && col.agg) {
            const vals = group.map(r => r[col.column]);
            switch (col.agg) {
              case 'SUM': result[col.alias] = vals.reduce((a, b) => a + b, 0); break;
              case 'COUNT': result[col.alias] = vals.length; break;
              case 'AVG': result[col.alias] = vals.reduce((a, b) => a + b, 0) / vals.length; break;
              case 'MIN': result[col.alias] = Math.min(...vals); break;
              case 'MAX': result[col.alias] = Math.max(...vals); break;
              case 'MEDIAN': result[col.alias] = median(vals); break;
            }
          }
        }
      }
      return result;
    });
  }

  _evalExpr(row, expr) {
    if (typeof expr === 'string') return row[expr];
    if (expr.op === '+') return this._evalExpr(row, expr.left) + this._evalExpr(row, expr.right);
    if (expr.op === '*') return this._evalExpr(row, expr.left) * this._evalExpr(row, expr.right);
    return expr;
  }
}
