// sql-analyzer.js — SQL complexity analyzer for HenryDB
// Analyzes parsed AST to measure query complexity and characteristics.

/**
 * Analyze a SQL AST for complexity metrics.
 * @param {Object} ast - Parsed SQL AST
 * @returns {Object} Complexity metrics
 */
export function analyzeSQL(ast) {
  const metrics = {
    type: ast.type || 'UNKNOWN',
    tables: 0,
    joins: 0,
    subqueries: 0,
    conditions: 0,
    aggregates: 0,
    windowFunctions: 0,
    ctes: 0,
    unions: 0,
    orderBy: 0,
    groupBy: 0,
    distinct: false,
    hasLimit: false,
    hasHaving: false,
    depth: 0,
    complexity: 0, // Computed score
  };

  if (!ast) return metrics;

  walkAST(ast, metrics, 0);
  
  // Compute complexity score
  metrics.complexity = (
    metrics.tables * 1 +
    metrics.joins * 3 +
    metrics.subqueries * 5 +
    metrics.conditions * 1 +
    metrics.aggregates * 2 +
    metrics.windowFunctions * 4 +
    metrics.ctes * 3 +
    metrics.unions * 2 +
    (metrics.hasHaving ? 2 : 0) +
    (metrics.distinct ? 1 : 0) +
    metrics.depth * 2
  );
  
  // Classification
  if (metrics.complexity <= 3) metrics.class = 'simple';
  else if (metrics.complexity <= 10) metrics.class = 'moderate';
  else if (metrics.complexity <= 25) metrics.class = 'complex';
  else metrics.class = 'very complex';
  
  return metrics;
}

function walkAST(node, metrics, depth) {
  if (!node || typeof node !== 'object') return;
  
  metrics.depth = Math.max(metrics.depth, depth);
  
  // Count tables
  if (node.from) metrics.tables++;
  
  // Count joins
  if (node.joins) metrics.joins += node.joins.length;
  
  // Count CTEs
  if (node.ctes || node.cte || node.with) {
    const ctes = node.ctes || node.cte || node.with;
    if (Array.isArray(ctes)) metrics.ctes += ctes.length;
  }
  
  // Count conditions
  if (node.where) countConditions(node.where, metrics);
  
  // Check aggregates and window functions in columns
  if (node.columns && Array.isArray(node.columns)) {
    for (const col of node.columns) {
      countFunctions(col, metrics);
    }
  }
  
  // GROUP BY
  if (node.groupBy) metrics.groupBy = Array.isArray(node.groupBy) ? node.groupBy.length : 1;
  
  // ORDER BY
  if (node.orderBy) metrics.orderBy = Array.isArray(node.orderBy) ? node.orderBy.length : 1;
  
  // DISTINCT
  if (node.distinct) metrics.distinct = true;
  
  // LIMIT
  if (node.limit !== null && node.limit !== undefined) metrics.hasLimit = true;
  
  // HAVING
  if (node.having) metrics.hasHaving = true;
  
  // Recurse into subqueries
  for (const key of Object.keys(node)) {
    const val = node[key];
    if (val && typeof val === 'object') {
      if (val.type === 'SELECT') {
        metrics.subqueries++;
        walkAST(val, metrics, depth + 1);
      } else if (Array.isArray(val)) {
        for (const item of val) {
          if (item && typeof item === 'object') {
            if (item.type === 'SELECT') {
              metrics.subqueries++;
              walkAST(item, metrics, depth + 1);
            }
          }
        }
      }
    }
  }
}

function countConditions(node, metrics) {
  if (!node) return;
  metrics.conditions++;
  if (node.type === 'AND' || node.type === 'OR') {
    countConditions(node.left, metrics);
    countConditions(node.right, metrics);
  }
}

function countFunctions(node, metrics) {
  if (!node || typeof node !== 'object') return;
  
  if (node.type === 'function_call' || node.type === 'FUNCTION') {
    const name = (node.name || node.function || '').toUpperCase();
    if (['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'TOTAL'].includes(name)) {
      metrics.aggregates++;
    }
    if (node.over) metrics.windowFunctions++;
  }
  
  if (node.type === 'window_function') {
    metrics.windowFunctions++;
  }
  
  // Recurse
  for (const key of Object.keys(node)) {
    const val = node[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => countFunctions(item, metrics));
      } else {
        countFunctions(val, metrics);
      }
    }
  }
}

/**
 * Analyze a raw SQL string.
 */
export function analyze(sql, parseFn) {
  try {
    const ast = parseFn(sql);
    return analyzeSQL(ast);
  } catch(e) {
    return { type: 'ERROR', error: e.message, complexity: 0, class: 'error' };
  }
}
