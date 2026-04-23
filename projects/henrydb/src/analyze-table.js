// analyze-table.js — Extracted from db.js (2026-04-23)
// ANALYZE TABLE using QueryPlanner for detailed statistics

import { QueryPlanner } from './planner.js';

/**
 * ANALYZE one or all tables using the QueryPlanner's analyzeTable method.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed ANALYZE AST
 * @returns {object} Analysis results with per-column statistics
 */
export function analyzeTable(db, ast) {
  const planner = new QueryPlanner(db);
  const tables = ast.table ? [ast.table] : [...db.tables.keys()];
  const results = [];

  for (const tableName of tables) {
    if (!db.tables.has(tableName)) continue;
    const stats = planner.analyzeTable(tableName);
    results.push({
      table: tableName,
      rows: stats.rowCount,
      pages: stats.pageCount,
      columns: [...stats.columns.entries()].map(([name, cs]) => ({
        name,
        ndv: cs.ndv,
        nulls: cs.nullCount,
        min: cs.min,
        max: cs.max,
        avg_width: Math.round(cs.avgWidth),
      })),
    });
  }

  return {
    type: 'ANALYZE',
    tables: results,
    message: `Analyzed ${results.length} table(s): ${results.map(r => `${r.table}(${r.rows} rows)`).join(', ')}`,
  };
}
