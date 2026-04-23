// volcano-select.js — Extracted from db.js (2026-04-23)
// Volcano engine SELECT attempt: builds and executes Volcano query plan

import { buildPlan } from './volcano-planner.js';

/**
 * Try to execute a SELECT using the Volcano query engine.
 * Returns null if the query cannot be handled by Volcano.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed SELECT AST
 * @returns {object|null} { rows, columns } or null
 */
export function tryVolcanoSelect(db, ast) {
    // SELECT without FROM: now supported via Volcano (single empty row)
    if (db._outerRow) return null; // Correlated context (LATERAL JOIN) — use legacy path
    if (ast.joins?.some(j => j.lateral)) return null; // LATERAL JOINs — use legacy path
    // Derived tables in FROM — now supported in Volcano (materialized)
    if (ast.recursive) return null; // Recursive CTEs
    if (ast.ctes?.some(c => c.recursive)) return null; // Individual recursive CTEs
    // Skip CTEs containing window functions (UNION now supported)
    if (ast.ctes?.some(c => {
      const s = JSON.stringify(c);
      return s.includes('"type":"window"') || s.includes('"over":{');
    })) return null;
    if (ast.pivot) return null; // PIVOT queries
    if (ast.unpivot) return null; // UNPIVOT queries
    // Window functions — now supported in Volcano via Window iterator
    // (including nested in expressions like CASE WHEN ROW_NUMBER() OVER ... = 1)
    // Function-wrapped aggregates (COALESCE(SUM(x), 0)) — now supported in Volcano
    // Skip unsupported aggregate functions
    const unsupportedAggs = ['PERCENTILE_CONT', 'PERCENTILE_DISC', 
      'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP', 'MODE', 'MEDIAN',
      'REGR_SLOPE', 'REGR_INTERCEPT', 'CORR', 'COVAR_POP', 'COVAR_SAMP'];
    const hasUnsupportedAgg = ast.columns.some(c => 
      c.type === 'aggregate' && unsupportedAggs.includes(c.func?.toUpperCase())
    );
    if (hasUnsupportedAgg) return null;
    // Aggregate FILTER clause — now supported in Volcano
    // Also check HAVING and subqueries for unsupported aggregates
    const astStr = JSON.stringify(ast);
    if (unsupportedAggs.some(a => astStr.includes(`"func":"${a}"`) || astStr.includes(`"func":"${a.toLowerCase()}"`) )) return null;
    // Check for derived tables in nested subqueries
    // Derived tables (__subquery) — now supported in Volcano
    // Skip JSON operations
    if (astStr.includes('"->>"') || astStr.toLowerCase().includes('"json_') || astStr.includes('"JSON_')) {
      return null;
    }
    // Skip MATCH AGAINST (fulltext search) — not handled by Volcano predicate builder
    if (astStr.includes('MATCH_AGAINST') || astStr.includes('TS_MATCH')) return null;
    
    // Build tables map including materialized CTE views
    let volcanoTables = db.tables;
    if (db.views.size > 0) {
      volcanoTables = new Map(db.tables);
      for (const [name, view] of db.views) {
        if (view.materializedRows && !volcanoTables.has(name)) {
          const rows = view.materializedRows;
          const rawKeys = rows.length > 0 ? Object.keys(rows[0]) : [];
          const schema = rawKeys.map(k => ({ name: k }));
          volcanoTables.set(name, {
            heap: {
              scan: function*() { for (const r of rows) yield { values: schema.map(c => r[c.name]), pageId: 0, slotIdx: 0 }; },
              rowCount: rows.length,
              tupleCount: rows.length
            },
            schema
          });
        }
      }
    }
    
    const plan = volcanoBuildPlan(ast, volcanoTables, db._indexes, db._tableStats);
    if (!plan) return null;
    
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) {
      // Clean up row keys: use unqualified names, strip internal _keys
      // Keep qualified names when there would be column name collisions
      const clean = {};
      const seen = new Set();
      // First pass: collect unqualified names that appear multiple times
      const nameCounts = {};
      for (const k of Object.keys(row)) {
        if (k.startsWith('_')) continue;
        const unqual = k.includes('.') ? k.split('.').pop() : k;
        nameCounts[unqual] = (nameCounts[unqual] || 0) + 1;
      }
      for (const [k, v] of Object.entries(row)) {
        if (k.startsWith('_')) continue;
        if (k.includes('.')) {
          const unqual = k.split('.').pop();
          if (nameCounts[unqual] > 1) {
            // Column name collision — keep qualified name
            clean[k] = v;
          } else if (!(unqual in clean)) {
            clean[unqual] = v;
          }
        } else {
          clean[k] = v;
        }
      }
      rows.push(clean);
    }
    plan.close();
    return { rows, columns: rows.length > 0 ? Object.keys(rows[0]) : [] };
  }
