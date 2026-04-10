// vectorized-bridge.js — Bridge between HenryDB's row-oriented engine and vectorized execution
// Converts row-major data to column batches, runs vectorized operators, converts back.

import { ColumnBatch, VecScanOperator, VecFilterOperator, VecHashAggOperator, collectRows, BATCH_SIZE } from './vectorized.js';

/**
 * Determine if a query can benefit from vectorized execution.
 * Currently targets: GROUP BY with simple aggregate functions.
 */
export function canVectorize(ast) {
  if (!ast.groupBy) return false;
  if (!ast.columns) return false;
  
  // Check all columns are either GROUP BY refs or simple aggregates
  for (const col of ast.columns) {
    if (col.type === 'aggregate') {
      const fn = col.func?.toUpperCase();
      if (!['SUM', 'COUNT', 'MIN', 'MAX', 'AVG'].includes(fn)) return false;
      // Only support simple column arguments (not expressions)
      if (col.arg !== '*' && typeof col.arg === 'object') return false;
    }
    // Non-aggregate columns must be GROUP BY references
  }
  
  // Must have at least one aggregate
  const hasAgg = ast.columns.some(c => c.type === 'aggregate');
  if (!hasAgg) return false;
  
  // GROUP BY must be simple column names
  if (ast.groupBy.some(g => typeof g !== 'string')) return false;
  
  return true;
}

/**
 * Execute a GROUP BY + aggregate query using the vectorized engine.
 * Takes rows (array of {col: value} objects) and the AST, returns result rows.
 * 
 * @param {Array<Object>} rows - Input rows (row-major, keyed by column name)
 * @param {Object} ast - Parsed query AST with groupBy and columns
 * @returns {Array<Object>} Result rows
 */
export function vectorizedGroupBy(rows, ast) {
  if (rows.length === 0) return [];
  
  // 1. Determine column layout: group columns + aggregate source columns
  const groupCols = ast.groupBy; // ['department', 'region']
  const aggSpecs = ast.columns.filter(c => c.type === 'aggregate');
  
  // Collect all needed source columns
  const sourceColNames = [...new Set([
    ...groupCols,
    ...aggSpecs.map(a => a.arg).filter(a => a !== '*' && typeof a === 'string')
  ])];
  
  // 2. Convert rows to columnar format
  const numericData = [];
  for (const row of rows) {
    const arr = sourceColNames.map(name => row[name] ?? null);
    numericData.push(arr);
  }
  
  const numCols = sourceColNames.length;
  const scan = new VecScanOperator(numericData, numCols);
  
  // 3. Build the GROUP BY column index
  const groupByColIdx = sourceColNames.indexOf(groupCols[0]); // Single group column for now
  
  // 4. Build aggregate specs for vectorized engine
  const vecAggs = aggSpecs.map(spec => {
    const fn = spec.func.toLowerCase();
    if (spec.arg === '*') {
      // COUNT(*) — use any column
      return { colIdx: 0, fn: fn === 'count' ? 'count' : fn };
    }
    const colIdx = sourceColNames.indexOf(spec.arg);
    return { colIdx, fn };
  });
  
  // 5. Run vectorized aggregation
  const agg = new VecHashAggOperator(scan, groupByColIdx, vecAggs);
  const resultBatches = collectRows(agg);
  
  // 6. Convert back to row objects
  const resultRows = [];
  for (const row of resultBatches) {
    const obj = {};
    // First column is the group key
    obj[groupCols[0]] = row[0];
    // Remaining columns are aggregates
    for (let i = 0; i < aggSpecs.length; i++) {
      const name = aggSpecs[i].alias || `${aggSpecs[i].func}(${aggSpecs[i].arg})`;
      obj[name] = row[1 + i];
    }
    resultRows.push(obj);
  }
  
  return resultRows;
}

/**
 * Benchmark: compare row-at-a-time vs vectorized for an aggregate query.
 */
export function benchmarkVectorizedAgg(rows, groupColName, aggColName) {
  const numericData = rows.map(r => [r[groupColName], r[aggColName]]);
  
  // Row-at-a-time
  const t0 = performance.now();
  const groups = new Map();
  for (const row of rows) {
    const key = row[groupColName];
    groups.set(key, (groups.get(key) || 0) + row[aggColName]);
  }
  const rowTime = performance.now() - t0;
  
  // Vectorized
  const t1 = performance.now();
  const scan = new VecScanOperator(numericData, 2);
  const agg = new VecHashAggOperator(scan, 0, [{ colIdx: 1, fn: 'sum' }]);
  collectRows(agg);
  const vecTime = performance.now() - t1;
  
  return { rowTime, vecTime, speedup: rowTime / vecTime, groups: groups.size };
}
