// vectorized-bridge.js — Bridge between HenryDB's row-oriented engine and vectorized execution
// Converts row-major data to column batches, runs vectorized operators, converts back.

import { ColumnBatch, VecScanOperator, VecFilterOperator, VecHashAggOperator, collectRows, BATCH_SIZE } from './vectorized.js';

/**
 * Determine if a query can benefit from vectorized execution.
 * Currently targets: GROUP BY with simple aggregate functions.
 */
export function canVectorize(ast) {
  if (!ast.groupBy) return false;
  if (!Array.isArray(ast.groupBy)) return false; // ROLLUP/CUBE/GROUPING SETS
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
 * Supports multiple GROUP BY columns via composite key hashing.
 * 
 * @param {Array<Object>} rows - Input rows (row-major, keyed by column name)
 * @param {Object} ast - Parsed query AST with groupBy and columns
 * @returns {Array<Object>} Result rows matching standard path format
 */
export function vectorizedGroupBy(rows, ast) {
  if (rows.length === 0) return [];
  
  // 1. Determine column layout
  const groupCols = ast.groupBy; // ['department', 'region']
  const aggSpecs = ast.columns.filter(c => c.type === 'aggregate');
  
  // Collect all needed source columns
  const sourceColNames = [...new Set([
    ...groupCols,
    ...aggSpecs.map(a => a.arg).filter(a => a !== '*' && typeof a === 'string')
  ])];
  
  // 2. For multi-group: use composite key approach in JS (bypass VecHashAggOperator limitation)
  // For single-group: use the vectorized engine directly
  if (groupCols.length === 1) {
    return _vectorizedSingleGroup(rows, ast, groupCols, aggSpecs, sourceColNames);
  }
  return _vectorizedMultiGroup(rows, ast, groupCols, aggSpecs, sourceColNames);
}

/**
 * Single group column — uses vectorized VecHashAggOperator directly.
 */
function _vectorizedSingleGroup(rows, ast, groupCols, aggSpecs, sourceColNames) {
  const numericData = rows.map(row => sourceColNames.map(name => row[name] ?? null));
  const numCols = sourceColNames.length;
  const scan = new VecScanOperator(numericData, numCols);
  
  const groupByColIdx = sourceColNames.indexOf(groupCols[0]);
  const vecAggs = aggSpecs.map(spec => {
    const fn = spec.func.toLowerCase();
    if (spec.arg === '*') return { colIdx: 0, fn: fn === 'count' ? 'count' : fn };
    return { colIdx: sourceColNames.indexOf(spec.arg), fn };
  });
  
  const agg = new VecHashAggOperator(scan, groupByColIdx, vecAggs);
  const resultBatches = collectRows(agg);
  
  return _buildResultRows(resultBatches, ast, groupCols, aggSpecs, /* multiGroup */ false);
}

/**
 * Multiple group columns — process in column batches with composite key.
 */
function _vectorizedMultiGroup(rows, ast, groupCols, aggSpecs, sourceColNames) {
  const groupColIdxs = groupCols.map(g => sourceColNames.indexOf(g));
  const aggColIdxs = aggSpecs.map(spec => {
    if (spec.arg === '*') return -1; // COUNT(*) doesn't need a column
    return sourceColNames.indexOf(spec.arg);
  });
  
  // Process in batches (column-oriented within each batch)
  const groups = new Map(); // compositeKey → { groupValues: [], aggs: [] }
  
  for (let start = 0; start < rows.length; start += BATCH_SIZE) {
    const end = Math.min(start + BATCH_SIZE, rows.length);
    const batchLen = end - start;
    
    // Build columnar arrays for this batch
    const columns = sourceColNames.map(name => {
      const col = new Array(batchLen);
      for (let i = 0; i < batchLen; i++) col[i] = rows[start + i][name] ?? null;
      return col;
    });
    
    // Process batch
    for (let i = 0; i < batchLen; i++) {
      // Composite key from all group columns
      const keyParts = groupColIdxs.map(idx => columns[idx][i]);
      const key = keyParts.join('\x00'); // null-byte separator
      
      if (!groups.has(key)) {
        groups.set(key, {
          groupValues: keyParts,
          aggs: aggSpecs.map(a => {
            switch (a.func.toLowerCase()) {
              case 'sum': return 0;
              case 'count': return 0;
              case 'min': return Infinity;
              case 'max': return -Infinity;
              case 'avg': return { sum: 0, count: 0 };
              default: return 0;
            }
          })
        });
      }
      
      const g = groups.get(key);
      for (let a = 0; a < aggSpecs.length; a++) {
        const val = aggColIdxs[a] === -1 ? 1 : columns[aggColIdxs[a]][i];
        switch (aggSpecs[a].func.toLowerCase()) {
          case 'sum': g.aggs[a] += val; break;
          case 'count': g.aggs[a]++; break;
          case 'min': if (val < g.aggs[a]) g.aggs[a] = val; break;
          case 'max': if (val > g.aggs[a]) g.aggs[a] = val; break;
          case 'avg': g.aggs[a].sum += val; g.aggs[a].count++; break;
        }
      }
    }
  }
  
  // Build result rows matching standard path format
  const resultRows = [];
  for (const [, g] of groups) {
    const obj = _buildRowFromSelectList(ast, groupCols, g.groupValues, aggSpecs, g.aggs);
    resultRows.push(obj);
  }
  return resultRows;
}

/**
 * Build result rows from VecHashAggOperator output, matching SELECT list order.
 */
function _buildResultRows(rawRows, ast, groupCols, aggSpecs, multiGroup) {
  const resultRows = [];
  for (const row of rawRows) {
    const groupValues = multiGroup ? row.slice(0, groupCols.length) : [row[0]];
    const aggValues = multiGroup
      ? aggSpecs.map((_, i) => row[groupCols.length + i])
      : aggSpecs.map((_, i) => row[1 + i]);
    
    const obj = _buildRowFromSelectList(ast, groupCols, groupValues, aggSpecs, aggValues);
    resultRows.push(obj);
  }
  return resultRows;
}

/**
 * Build a row object matching the SELECT list column order and naming.
 * This ensures vectorized output matches the standard path exactly.
 */
function _buildRowFromSelectList(ast, groupCols, groupValues, aggSpecs, aggValues) {
  const obj = {};
  let aggIdx = 0;
  
  for (const col of ast.columns) {
    if (col.type === 'aggregate') {
      const spec = aggSpecs[aggIdx];
      let val = aggValues[aggIdx];
      if (spec.func.toLowerCase() === 'avg' && typeof val === 'object') {
        val = val.count > 0 ? val.sum / val.count : 0;
      }
      // Use alias if provided, otherwise standard naming: FUNC(arg)
      const name = col.alias || `${spec.func}(${spec.arg})`;
      obj[name] = val;
      aggIdx++;
    } else if (col.type === 'column') {
      const groupIdx = groupCols.indexOf(col.name);
      if (groupIdx >= 0) {
        const name = col.alias || col.name;
        obj[name] = groupValues[groupIdx];
      }
    }
  }
  
  return obj;
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
