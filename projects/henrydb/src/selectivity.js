/**
 * selectivity.js — Shared selectivity estimator for query planners
 * 
 * Used by both the classic planner (query-plan.js) and Volcano planner (volcano-planner.js)
 * to estimate what fraction of rows will pass a WHERE predicate.
 * 
 * When ANALYZE statistics (ndistinct, histograms) are available, uses them.
 * Falls back to hardcoded heuristics when no statistics exist.
 */

// Default heuristics (used when no table stats available)
const DEFAULTS = {
  equality: 0.1,        // col = val (no stats)
  range: 0.33,          // col < val, col > val, etc.
  inequality: 0.9,      // col != val, col <> val
  like: 0.25,           // col LIKE 'pattern'
  between: 0.25,        // col BETWEEN a AND b
  inList: 0.15,         // col IN (...)
  isNull: 0.05,         // col IS NULL
  regexp: 0.25,         // col REGEXP 'pattern'
  unknown: 0.33,        // unrecognized predicate
  noStats: 0.33,        // no stats available at all
};

/**
 * Estimate selectivity of a WHERE predicate.
 * @param {object} where - AST node for the predicate
 * @param {string} tableName - table being queried
 * @param {Map|null} tableStats - Map<tableName, {columns: {colName: {distinct, histogram}}}>
 * @returns {number} fraction 0..1
 */
export function estimateSelectivity(where, tableName, tableStats) {
  if (!where) return 1.0;
  if (!tableStats) return _estimateWithoutStats(where);
  
  const stats = tableStats.get?.(tableName);
  if (!stats || !stats.columns) return _estimateWithoutStats(where);
  
  return _estimateWithStats(where, stats, tableName, tableStats);
}

/**
 * Estimate selectivity using table statistics.
 */
function _estimateWithStats(where, stats, tableName, tableStats) {
  if (where.type === 'COMPARE' || where.type === 'binary') {
    const op = where.operator || where.op;
    const colName = _extractColName(where.left);
    const colStats = colName ? stats.columns[colName] : null;
    
    if ((op === '=' || op === 'EQ') && colStats?.distinct > 0) {
      return 1 / colStats.distinct;
    }
    
    if (['<', '>', '<=', '>=', 'LT', 'LE', 'GT', 'GE'].includes(op)) {
      if (colStats?.histogram?.length > 0) {
        const val = where.right?.value ?? where.left?.value;
        if (val != null) {
          return _histogramSelectivity(colStats.histogram, val, op);
        }
      }
      return DEFAULTS.range;
    }
    
    if (op === '!=' || op === '<>' || op === 'NE') {
      if (colStats?.distinct > 0) return 1 - (1 / colStats.distinct);
      return DEFAULTS.inequality;
    }
    
    if (op === 'LIKE' || op === 'ILIKE') return DEFAULTS.like;
    
    return DEFAULTS.unknown;
  }
  
  if (where.type === 'AND') {
    const left = _estimateWithStats(where.left, stats, tableName, tableStats);
    const right = _estimateWithStats(where.right, stats, tableName, tableStats);
    return left * right; // Independence assumption
  }
  
  if (where.type === 'OR') {
    const left = _estimateWithStats(where.left, stats, tableName, tableStats);
    const right = _estimateWithStats(where.right, stats, tableName, tableStats);
    return Math.min(1, left + right - left * right);
  }
  
  if (where.type === 'BETWEEN') return DEFAULTS.between;
  if (where.type === 'IN' || where.type === 'IN_LIST') return DEFAULTS.inList;
  if (where.type === 'IS_NULL') return DEFAULTS.isNull;
  if (where.type === 'LIKE' || where.type === 'ILIKE') return DEFAULTS.like;
  if (where.type === 'REGEXP') return DEFAULTS.regexp;
  if (where.type === 'NOT') return 1 - estimateSelectivity(where.expr, tableName, tableStats);
  
  return DEFAULTS.unknown;
}

/**
 * Estimate selectivity without table statistics (hardcoded heuristics).
 */
function _estimateWithoutStats(where) {
  if (where.type === 'binary' || where.type === 'COMPARE') {
    const op = where.operator || where.op;
    switch (op) {
      case '=': case 'EQ': return DEFAULTS.equality;
      case '<': case '>': case '<=': case '>=': case 'LT': case 'GT': case 'LE': case 'GE': return DEFAULTS.range;
      case '!=': case '<>': case 'NE': return DEFAULTS.inequality;
      case 'LIKE': case 'ILIKE': return DEFAULTS.like;
      default: return DEFAULTS.unknown;
    }
  }
  if (where.type === 'AND') return _estimateWithoutStats(where.left) * _estimateWithoutStats(where.right);
  if (where.type === 'OR') {
    const sl = _estimateWithoutStats(where.left);
    const sr = _estimateWithoutStats(where.right);
    return sl + sr - sl * sr;
  }
  if (where.type === 'BETWEEN') return DEFAULTS.between;
  if (where.type === 'IN' || where.type === 'IN_LIST') return DEFAULTS.inList;
  if (where.type === 'IS_NULL') return DEFAULTS.isNull;
  if (where.type === 'LIKE' || where.type === 'ILIKE') return DEFAULTS.like;
  if (where.type === 'REGEXP') return DEFAULTS.regexp;
  if (where.type === 'NOT') return 1 - _estimateWithoutStats(where.expr);
  return DEFAULTS.unknown;
}

/**
 * Extract column name from an AST node (handles table.column qualified names).
 */
function _extractColName(node) {
  if (!node) return null;
  const name = node.name || node.column;
  if (!name) return null;
  return typeof name === 'string' && name.includes('.') ? name.split('.').pop() : name;
}

/**
 * Estimate selectivity using histogram buckets.
 */
function _histogramSelectivity(histogram, val, op) {
  const totalCount = histogram.reduce((s, b) => s + b.count, 0);
  if (totalCount === 0) return DEFAULTS.range;
  
  let matchingCount = 0;
  for (const bucket of histogram) {
    if (op === 'GT' || op === 'GE' || op === '>' || op === '>=') {
      if (bucket.lo >= val) matchingCount += bucket.count;
      else if (bucket.hi >= val) {
        const frac = (bucket.hi - val) / Math.max(1, bucket.hi - bucket.lo);
        matchingCount += Math.round(bucket.count * frac);
      }
    } else { // LT, LE, <, <=
      if (bucket.hi <= val) matchingCount += bucket.count;
      else if (bucket.lo <= val) {
        const frac = (val - bucket.lo) / Math.max(1, bucket.hi - bucket.lo);
        matchingCount += Math.round(bucket.count * frac);
      }
    }
  }
  
  return Math.max(0.01, matchingCount / totalCount);
}

/**
 * Get ndistinct for a column from ANALYZE stats.
 */
export function getColumnNdv(tableName, columnName, tableStats) {
  if (!tableStats || !tableName || !columnName) return null;
  const col = columnName.includes('.') ? columnName.split('.').pop() : columnName;
  const stats = tableStats?.get?.(tableName);
  if (!stats || !stats.columns) return null;
  return stats.columns[col]?.distinct || null;
}

export { DEFAULTS as SELECTIVITY_DEFAULTS };
