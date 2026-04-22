// volcano-cost.js — Cost model for Volcano planner
// Estimates cardinality and execution cost for plan nodes.

/**
 * Cost constants (PostgreSQL-inspired, adjusted for in-memory operation)
 */
const COST = {
  seqScanPerRow: 0.01,          // CPU cost per row scanned
  indexScanPerRow: 0.005,        // CPU cost per index lookup
  filterPerRow: 0.0025,          // CPU cost per predicate evaluation
  hashBuildPerRow: 0.02,         // CPU cost per row to build hash table
  hashProbePerRow: 0.01,         // CPU cost per row to probe hash table
  nestedLoopPerRow: 0.01,        // CPU cost per outer row in NL join
  sortPerRowLog: 0.05,           // CPU cost per n*log(n) sort operation
  mergeJoinPerRow: 0.01,         // CPU cost per row in merge join
  projectPerRow: 0.001,          // CPU cost per projection
  distinctPerRow: 0.015,         // CPU cost per row for dedup (hash-based)
};

/**
 * Default selectivity estimates when no statistics are available.
 */
const SELECTIVITY = {
  equality: 0.1,      // col = value → 10% of rows
  range: 0.33,        // col > value → 33% of rows
  like: 0.2,          // col LIKE 'pattern%' → 20% of rows
  in_list: 0.05,      // col IN (v1, v2, ...) → 5% per value, capped
  between: 0.25,      // col BETWEEN a AND b → 25%
  is_null: 0.02,      // col IS NULL → 2%
  not: 0.9,           // NOT(pred) → 1 - selectivity(pred) (default 90%)
  and: null,           // product of children
  or: null,            // sum - product of children
  default: 0.5,        // unknown predicate → 50%
};

/**
 * Estimate the selectivity of a predicate (WHERE clause).
 * Returns a number between 0 and 1.
 */
export function estimateSelectivity(predicate) {
  if (!predicate) return 1.0;
  
  switch (predicate.type) {
    case 'COMPARE':
      if (predicate.op === 'EQ') return SELECTIVITY.equality;
      if (['LT', 'GT', 'LE', 'GE', 'NE'].includes(predicate.op)) return SELECTIVITY.range;
      return SELECTIVITY.default;
      
    case 'LIKE':
    case 'ILIKE':
      return SELECTIVITY.like;
      
    case 'IN_LIST':
      return Math.min(1.0, (predicate.values?.length || 1) * SELECTIVITY.in_list);
      
    case 'BETWEEN':
      return SELECTIVITY.between;
      
    case 'IS_NULL':
      return SELECTIVITY.is_null;
      
    case 'IS_NOT_NULL':
      return 1.0 - SELECTIVITY.is_null;
      
    case 'NOT':
      return 1.0 - estimateSelectivity(predicate.expr);
      
    case 'AND':
      return estimateSelectivity(predicate.left) * estimateSelectivity(predicate.right);
      
    case 'OR':
      const sl = estimateSelectivity(predicate.left);
      const sr = estimateSelectivity(predicate.right);
      return sl + sr - sl * sr;
      
    default:
      return SELECTIVITY.default;
  }
}

/**
 * Estimate the cardinality (output rows) of a table with optional filter.
 * @param {Map} tables — table catalog
 * @param {string} tableName — table name
 * @param {object|null} predicate — WHERE clause AST
 * @returns {number} estimated rows
 */
export function estimateCardinality(tables, tableName, predicate) {
  const table = tables.get(tableName);
  if (!table) return 100; // default fallback
  
  const rowCount = table.heap?.rowCount || (table._cteMaterialized ? table.heap?.length : 100);
  const selectivity = estimateSelectivity(predicate);
  return Math.max(1, Math.round(rowCount * selectivity));
}

/**
 * Estimate the cost of different join strategies.
 * Returns costs for hash join, nested loop join, and merge join.
 */
export function estimateJoinCosts(leftCard, rightCard) {
  // Hash join: build hash on smaller side, probe with larger
  const buildSide = Math.min(leftCard, rightCard);
  const probeSide = Math.max(leftCard, rightCard);
  const hashJoinCost = buildSide * COST.hashBuildPerRow + probeSide * COST.hashProbePerRow;
  
  // Nested loop join: O(n*m)
  const nestedLoopCost = leftCard * rightCard * COST.nestedLoopPerRow;
  
  // Merge join: sort both sides + merge
  const leftSortCost = leftCard > 1 ? leftCard * Math.log2(leftCard) * COST.sortPerRowLog : 0;
  const rightSortCost = rightCard > 1 ? rightCard * Math.log2(rightCard) * COST.sortPerRowLog : 0;
  const mergeCost = (leftCard + rightCard) * COST.mergeJoinPerRow;
  const mergeJoinCost = leftSortCost + rightSortCost + mergeCost;
  
  return { hashJoinCost, nestedLoopCost, mergeJoinCost };
}

/**
 * Choose the best join strategy for an equi-join.
 * @param {number} leftCard — left input cardinality
 * @param {number} rightCard — right input cardinality
 * @param {boolean} leftSorted — is left already sorted on join key?
 * @param {boolean} rightSorted — is right already sorted on join key?
 * @returns {'hash'|'merge'|'nested_loop'} recommended join strategy
 */
export function chooseBestJoin(leftCard, rightCard, leftSorted = false, rightSorted = false) {
  const costs = estimateJoinCosts(leftCard, rightCard);
  
  // If both sides are sorted, merge join sort cost is 0
  let effectiveMergeCost = costs.mergeJoinCost;
  if (leftSorted && rightSorted) {
    effectiveMergeCost = (leftCard + rightCard) * COST.mergeJoinPerRow;
  }
  
  // For very small tables (< 10 rows), nested loop is fine
  if (leftCard * rightCard < 100) return 'nested_loop';
  
  // Choose cheapest
  const options = [
    { strategy: 'hash', cost: costs.hashJoinCost },
    { strategy: 'merge', cost: effectiveMergeCost },
    { strategy: 'nested_loop', cost: costs.nestedLoopCost },
  ];
  
  options.sort((a, b) => a.cost - b.cost);
  return options[0].strategy;
}

/**
 * Decide whether to use IndexScan vs SeqScan based on selectivity.
 * Index scan is only better when selectivity is low (few matching rows).
 * @param {number} tableCard — total rows in table
 * @param {number} selectivity — estimated selectivity (0-1)
 * @returns {boolean} true if index scan is recommended
 */
export function shouldUseIndexScan(tableCard, selectivity) {
  // For very small tables, always seq scan
  if (tableCard < 50) return false;
  
  // Index scan wins when we read < ~20% of rows
  // (random I/O overhead vs sequential scan)
  return selectivity < 0.2;
}

export { COST, SELECTIVITY };
