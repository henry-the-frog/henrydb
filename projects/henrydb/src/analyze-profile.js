// analyze-profile.js — Extracted from db.js (2026-04-23)
// ANALYZE TABLE and query profiling

import { parse } from './sql.js';

/**
 * Handle ANALYZE TABLE — collect column statistics for the optimizer.
 * @param {object} db - Database instance
 * @param {string} sql - Full ANALYZE SQL string
 * @returns {object} Statistics result
 */
export function handleAnalyze(db, sql) {
  const match = sql.match(/ANALYZE\s+(?:TABLE\s+)?(\w+)/i);
  if (!match) throw new Error('Invalid ANALYZE syntax. Use: ANALYZE TABLE name');
  const tableName = match[1].replace(/;$/, '');
  const table = db.tables.get(tableName) || db.tables.get(tableName.toLowerCase());
  if (!table) throw new Error(`Table "${tableName}" not found`);
  
  const allRows = db.execute(`SELECT * FROM ${tableName}`).rows;
  const rowCount = allRows.length;
  
  const columns = {};
  for (const col of table.schema) {
    const values = allRows.map(r => r[col.name]);
    const distinct = new Set(values.filter(v => v !== null && v !== undefined)).size;
    const nulls = values.filter(v => v === null || v === undefined).length;
    
    const numericVals = values.filter(v => typeof v === 'number' && !isNaN(v));
    const min = numericVals.length > 0 ? Math.min(...numericVals) : null;
    const max = numericVals.length > 0 ? Math.max(...numericVals) : null;
    
    columns[col.name] = { distinct, nulls, min, max, selectivity: distinct > 0 ? 1 / distinct : 1 };
  }
  
  db._tableStats.set(tableName, { rowCount, columns, analyzedAt: Date.now() });
  
  return {
    type: 'ROWS',
    rows: Object.entries(columns).map(([col, stats]) => ({
      column: col,
      distinct_values: stats.distinct,
      null_count: stats.nulls,
      min: stats.min,
      max: stats.max,
      selectivity: stats.selectivity.toFixed(4),
    })),
  };
}

/**
 * Execute a query with detailed timing profile.
 * @param {object} db - Database instance
 * @param {string} sql - SQL query to profile
 * @returns {object} { result, profile } with phase-level timing
 */
export function profile(db, sql) {
  const phases = [];
  const t0 = performance.now();
  
  const parseStart = performance.now();
  let ast = db._planCache.get(sql);
  const cached = !!ast;
  if (!ast) {
    ast = parse(sql);
    if (ast.type === 'SELECT') db._planCache.put(sql, ast);
  }
  const parseEnd = performance.now();
  phases.push({ name: 'PARSE', durationMs: parseEnd - parseStart, cached });
  
  const execStart = performance.now();
  const result = db.execute_ast(ast);
  const execEnd = performance.now();
  phases.push({ name: 'EXECUTE', durationMs: execEnd - execStart, rows: result?.rows?.length || 0 });
  
  const totalMs = performance.now() - t0;
  
  const lines = [`Query: ${sql.slice(0, 80)}${sql.length > 80 ? '...' : ''}`];
  lines.push('─'.repeat(60));
  lines.push(`${'Phase'.padEnd(15)} ${'Duration'.padStart(12)} ${'Pct'.padStart(6)} Details`);
  lines.push('─'.repeat(60));
  for (const p of phases) {
    const pct = totalMs > 0 ? (p.durationMs / totalMs * 100).toFixed(1) : '0.0';
    const details = p.cached ? '(cached)' : p.rows !== undefined ? `${p.rows} rows` : '';
    lines.push(`${p.name.padEnd(15)} ${(p.durationMs.toFixed(3) + 'ms').padStart(12)} ${(pct + '%').padStart(6)} ${details}`);
  }
  lines.push('─'.repeat(60));
  lines.push(`${'TOTAL'.padEnd(15)} ${(totalMs.toFixed(3) + 'ms').padStart(12)} ${'100%'.padStart(6)}`);
  
  return {
    result,
    profile: {
      totalMs: parseFloat(totalMs.toFixed(3)),
      phases,
      formatted: lines.join('\n'),
    },
  };
}
