// index-advisor.js — Workload-based index recommendation for HenryDB
//
// Analyzes query patterns and suggests CREATE INDEX statements that would
// improve performance. Inspired by PostgreSQL's pg_qualstats + hypothetical indexes.
//
// Usage:
//   const advisor = new IndexAdvisor(db);
//   advisor.analyze('SELECT * FROM orders WHERE status = "shipped"');
//   advisor.analyze('SELECT * FROM orders WHERE user_id = 5 AND status = "pending"');
//   const recommendations = advisor.recommend();
//   // → [{ table: 'orders', columns: ['status'], improvement: 'high', sql: 'CREATE INDEX ...' }]

import { parse } from './sql.js';

/**
 * IndexAdvisor — analyzes queries and recommends indexes.
 */
export class IndexAdvisor {
  constructor(db) {
    this.db = db;
    this._columnAccess = new Map(); // "table.column" → { filters: N, joins: N, orderBy: N, groupBy: N }
    this._compositeAccess = new Map(); // "table:col1,col2" → count
    this._queryCount = 0;
    this._existingIndexes = this._collectExistingIndexes();
  }

  /**
   * Analyze a query to collect column access patterns.
   */
  analyze(sql) {
    let ast;
    try {
      ast = parse(sql);
    } catch (e) {
      return; // Skip unparseable queries
    }
    this._queryCount++;

    if (ast.type === 'EXPLAIN') ast = ast.statement;
    if (ast.type !== 'SELECT') return;

    const tableName = ast.from?.table;
    if (!tableName) return;

    // Build alias → table name map
    const aliasMap = new Map();
    aliasMap.set(tableName, tableName);
    if (ast.from?.alias) aliasMap.set(ast.from.alias, tableName);
    for (const join of ast.joins || []) {
      const jt = typeof join.table === 'string' ? join.table : join.table?.table;
      aliasMap.set(jt, jt);
      if (join.alias) aliasMap.set(join.alias, jt);
    }

    // Analyze WHERE clause
    if (ast.where) {
      this._analyzeWhere(ast.where, tableName, 'filter', aliasMap);
    }

    // Analyze JOIN conditions
    for (const join of ast.joins || []) {
      const joinTable = typeof join.table === 'string' ? join.table : join.table?.table;
      if (join.on) {
        this._analyzeJoinCondition(join.on, tableName, joinTable, aliasMap);
      }
    }

    // Analyze ORDER BY
    if (ast.orderBy) {
      for (const sort of ast.orderBy) {
        const col = sort.column || sort.expr?.column;
        if (col) {
          const { table, column } = this._resolveColumn(col, tableName, aliasMap);
          this._recordAccess(table, column, 'orderBy');
        }
      }
    }

    // Analyze GROUP BY
    if (ast.groupBy) {
      const groupCols = [];
      for (const g of ast.groupBy) {
        const col = typeof g === 'string' ? g : g.column || g;
        if (col) {
          const { table, column } = this._resolveColumn(col, tableName, aliasMap);
          this._recordAccess(table, column, 'groupBy');
          groupCols.push(`${table}.${column}`);
        }
      }
      if (groupCols.length > 1) {
        const key = groupCols.sort().join(',');
        this._compositeAccess.set(key, (this._compositeAccess.get(key) || 0) + 1);
      }
    }
  }

  /**
   * Analyze a batch of queries.
   */
  analyzeBatch(queries) {
    for (const sql of queries) {
      this.analyze(sql);
    }
  }

  /**
   * Generate index recommendations based on analyzed workload.
   * @returns {Array} Sorted by estimated impact.
   */
  recommend() {
    const recommendations = [];

    for (const [key, stats] of this._columnAccess) {
      const [table, column] = key.split('.');
      
      // Skip if index already exists
      if (this._hasIndex(table, column)) continue;
      
      // Skip if table doesn't exist
      if (!this.db.tables.has(table)) continue;
      
      const tableRows = this._getRowCount(table);
      const totalAccess = stats.filter + stats.join + stats.orderBy + stats.groupBy;
      
      if (totalAccess === 0) continue;

      // Calculate impact score
      let impact = 0;
      let reasons = [];

      if (stats.filter > 0) {
        // Filter benefit: proportional to table size and frequency
        impact += stats.filter * Math.log2(Math.max(2, tableRows)) * 10;
        reasons.push(`used in ${stats.filter} WHERE clause(s)`);
      }
      if (stats.join > 0) {
        // Join benefit: huge for large tables
        impact += stats.join * Math.log2(Math.max(2, tableRows)) * 20;
        reasons.push(`used in ${stats.join} JOIN condition(s)`);
      }
      if (stats.orderBy > 0) {
        // Sort elimination benefit
        impact += stats.orderBy * Math.log2(Math.max(2, tableRows)) * 5;
        reasons.push(`used in ${stats.orderBy} ORDER BY clause(s)`);
      }
      if (stats.groupBy > 0) {
        impact += stats.groupBy * Math.log2(Math.max(2, tableRows)) * 5;
        reasons.push(`used in ${stats.groupBy} GROUP BY clause(s)`);
      }

      const level = impact > 100 ? 'high' : impact > 30 ? 'medium' : 'low';
      const indexName = `idx_${table}_${column}`;

      recommendations.push({
        table,
        columns: [column],
        impact: Math.round(impact * 10) / 10,
        level,
        reason: reasons.join('; '),
        sql: `CREATE INDEX ${indexName} ON ${table} (${column})`,
        tableRows,
        accessCount: totalAccess,
      });
    }

    // Check for composite index opportunities
    for (const [key, count] of this._compositeAccess) {
      const cols = key.split(',');
      if (cols.length < 2) continue;
      
      // Extract table name from first column
      const parts = cols.map(c => {
        const [t, col] = c.split('.');
        return { table: t, column: col };
      });
      
      // All columns must be from the same table
      const tables = new Set(parts.map(p => p.table));
      if (tables.size !== 1) continue;
      
      const table = parts[0].table;
      const columns = parts.map(p => p.column);
      const indexName = `idx_${table}_${columns.join('_')}`;
      
      // Skip if composite index already exists
      if (this._hasCompositeIndex(table, columns)) continue;
      
      const tableRows = this._getRowCount(table);
      const impact = count * Math.log2(Math.max(2, tableRows)) * 15;

      recommendations.push({
        table,
        columns,
        impact: Math.round(impact * 10) / 10,
        level: impact > 100 ? 'high' : impact > 30 ? 'medium' : 'low',
        reason: `composite columns used together in ${count} GROUP BY/WHERE clause(s)`,
        sql: `CREATE INDEX ${indexName} ON ${table} (${columns.join(', ')})`,
        tableRows,
        accessCount: count,
      });
    }

    // Sort by impact (highest first)
    recommendations.sort((a, b) => b.impact - a.impact);
    return recommendations;
  }

  /**
   * Get a summary of the analysis.
   */
  summary() {
    const recs = this.recommend();
    return {
      queriesAnalyzed: this._queryCount,
      columnsTracked: this._columnAccess.size,
      recommendations: recs.length,
      highImpact: recs.filter(r => r.level === 'high').length,
      mediumImpact: recs.filter(r => r.level === 'medium').length,
      lowImpact: recs.filter(r => r.level === 'low').length,
      topRecommendations: recs.slice(0, 5),
    };
  }

  // ===== Helpers =====

  _analyzeWhere(expr, defaultTable, type, aliasMap) {
    if (!expr) return;

    if (expr.type === 'COMPARE' || expr.type === 'binary') {
      const cols = this._extractColumns(expr);
      for (const col of cols) {
        const { table, column } = this._resolveColumn(col, defaultTable, aliasMap);
        this._recordAccess(table, column, type);
      }
      if (cols.length > 1) {
        const resolved = cols.map(c => this._resolveColumn(c, defaultTable, aliasMap));
        const tables = new Set(resolved.map(r => r.table));
        if (tables.size === 1) {
          const key = resolved.map(r => `${r.table}.${r.column}`).sort().join(',');
          this._compositeAccess.set(key, (this._compositeAccess.get(key) || 0) + 1);
        }
      }
      return;
    }

    if (expr.type === 'AND' || expr.type === 'OR') {
      this._analyzeWhere(expr.left, defaultTable, type, aliasMap);
      this._analyzeWhere(expr.right, defaultTable, type, aliasMap);
      
      if (expr.type === 'AND') {
        const leftCols = this._extractAllColumns(expr.left, defaultTable, aliasMap);
        const rightCols = this._extractAllColumns(expr.right, defaultTable, aliasMap);
        const allCols = [...leftCols, ...rightCols];
        const tables = new Set(allCols.map(c => c.table));
        if (tables.size === 1 && allCols.length >= 2) {
          const key = allCols.map(c => `${c.table}.${c.column}`).sort().join(',');
          this._compositeAccess.set(key, (this._compositeAccess.get(key) || 0) + 1);
        }
      }
      return;
    }

    if (expr.type === 'BETWEEN' || expr.type === 'IN' || expr.type === 'IS_NULL' || expr.type === 'LIKE') {
      const col = expr.column?.name || expr.column?.column;
      if (col) {
        const { table, column } = this._resolveColumn(col, defaultTable, aliasMap);
        this._recordAccess(table, column, type);
      }
    }
  }

  _analyzeJoinCondition(expr, leftTable, rightTable, aliasMap) {
    if ((expr.type === 'COMPARE' && expr.op === 'EQ') || (expr.type === 'binary' && expr.operator === '=')) {
      const leftCol = expr.left?.name || expr.left?.column;
      const rightCol = expr.right?.name || expr.right?.column;
      
      if (leftCol) {
        const { table, column } = this._resolveColumn(leftCol, leftTable, aliasMap);
        this._recordAccess(table, column, 'join');
      }
      if (rightCol) {
        const { table, column } = this._resolveColumn(rightCol, rightTable, aliasMap);
        this._recordAccess(table, column, 'join');
      }
    }
  }

  _extractColumns(expr) {
    const cols = [];
    if (expr.left?.type === 'column_ref') cols.push(expr.left.name || expr.left.column);
    if (expr.right?.type === 'column_ref') cols.push(expr.right.name || expr.right.column);
    return cols.filter(Boolean);
  }

  _extractAllColumns(expr, defaultTable, aliasMap) {
    const results = [];
    if (!expr) return results;
    if (expr.type === 'column_ref') {
      results.push(this._resolveColumn(expr.name || expr.column, defaultTable, aliasMap));
    }
    if (expr.left) results.push(...this._extractAllColumns(expr.left, defaultTable, aliasMap));
    if (expr.right) results.push(...this._extractAllColumns(expr.right, defaultTable, aliasMap));
    return results;
  }

  _resolveColumn(col, defaultTable, aliasMap) {
    if (col.includes('.')) {
      const [alias, column] = col.split('.');
      const table = aliasMap?.get(alias) || alias;
      return { table, column };
    }
    return { table: defaultTable, column: col };
  }

  _recordAccess(table, column, type) {
    const key = `${table}.${column}`;
    if (!this._columnAccess.has(key)) {
      this._columnAccess.set(key, { filter: 0, join: 0, orderBy: 0, groupBy: 0 });
    }
    const stats = this._columnAccess.get(key);
    if (type === 'filter') stats.filter++;
    else if (type === 'join') stats.join++;
    else if (type === 'orderBy') stats.orderBy++;
    else if (type === 'groupBy') stats.groupBy++;
  }

  _collectExistingIndexes() {
    const indexes = new Map();
    // Check indexCatalog (the main index registry)
    if (this.db.indexCatalog) {
      for (const [name, meta] of this.db.indexCatalog) {
        const table = meta.table;
        const cols = meta.columns || [meta.column];
        if (!indexes.has(table)) indexes.set(table, []);
        indexes.get(table).push({ name, columns: cols.filter(Boolean) });
      }
    }
    // Also check _indexes if it exists
    if (this.db._indexes) {
      for (const [table, tableIndexes] of this.db._indexes) {
        for (const [name, idx] of tableIndexes) {
          const cols = idx.columns || [idx.column];
          if (!indexes.has(table)) indexes.set(table, []);
          indexes.get(table).push({ name, columns: cols.filter(Boolean) });
        }
      }
    }
    return indexes;
  }

  _hasIndex(table, column) {
    const tableIndexes = this._existingIndexes.get(table) || [];
    return tableIndexes.some(idx => idx.columns.length === 1 && idx.columns[0] === column);
  }

  _hasCompositeIndex(table, columns) {
    const tableIndexes = this._existingIndexes.get(table) || [];
    const sorted = [...columns].sort();
    return tableIndexes.some(idx => {
      const idxSorted = [...idx.columns].sort();
      return idxSorted.length === sorted.length && idxSorted.every((c, i) => c === sorted[i]);
    });
  }

  _getRowCount(table) {
    const t = this.db.tables.get(table);
    if (!t) return 0;
    return t.heap?._rowCount || t.heap?.tupleCount || 0;
  }
}
