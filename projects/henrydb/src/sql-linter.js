// sql-linter.js — SQL anti-pattern detector for HenryDB
// Analyzes parsed AST for common SQL quality issues and provides suggestions.

const SEVERITY = { ERROR: 'error', WARNING: 'warning', INFO: 'info' };

/**
 * Lint a parsed SQL AST for common anti-patterns.
 * @param {Object} ast - Parsed AST from sql.parse()
 * @returns {Array<{rule: string, severity: string, message: string, suggestion: string}>}
 */
export function lintSQL(ast) {
  if (!ast) return [];
  const issues = [];
  
  const rules = [
    checkSelectStar,
    checkMissingWhere,
    checkSelectWithoutLimit,
    checkCartesianProduct,
    checkNestedSubqueries,
    checkFunctionInWhere,
    checkSelectDistinctStar,
    checkOrderByOrdinal,
    checkImplicitJoin,
    checkNullComparison,
    checkCountStar,
    checkUnusedAlias,
    checkLeadingWildcard,
    checkRedundantDistinct,
  ];
  
  for (const rule of rules) {
    const result = rule(ast);
    if (result) {
      if (Array.isArray(result)) issues.push(...result);
      else issues.push(result);
    }
  }
  
  return issues;
}

// Rule: SELECT * — use explicit column list
function checkSelectStar(ast) {
  if (ast.type !== 'SELECT') return null;
  const hasStar = (ast.columns || []).some(c => 
    c.type === 'star' || c === '*' || (c.type === 'column' && c.name === '*')
  );
  if (hasStar) {
    return {
      rule: 'no-select-star',
      severity: SEVERITY.WARNING,
      message: 'SELECT * returns all columns — consider listing specific columns.',
      suggestion: 'Replace * with explicit column names for clarity and performance.',
    };
  }
  return null;
}

// Rule: UPDATE/DELETE without WHERE
function checkMissingWhere(ast) {
  if (ast.type === 'UPDATE' && !ast.where) {
    return {
      rule: 'update-without-where',
      severity: SEVERITY.ERROR,
      message: 'UPDATE without WHERE clause will modify ALL rows.',
      suggestion: 'Add a WHERE clause to limit which rows are updated.',
    };
  }
  if (ast.type === 'DELETE' && !ast.where) {
    return {
      rule: 'delete-without-where',
      severity: SEVERITY.ERROR,
      message: 'DELETE without WHERE clause will remove ALL rows.',
      suggestion: 'Add a WHERE clause, or use TRUNCATE if intentional.',
    };
  }
  return null;
}

// Rule: Large result sets without LIMIT
function checkSelectWithoutLimit(ast) {
  if (ast.type !== 'SELECT') return null;
  if (ast.limit !== null && ast.limit !== undefined) return null;
  // Only warn if there's a FROM (not for aggregate-only queries)
  if (!ast.from) return null;
  // Don't warn if there's a GROUP BY (result set likely small)
  if (ast.groupBy) return null;
  return {
    rule: 'select-without-limit',
    severity: SEVERITY.INFO,
    message: 'SELECT without LIMIT may return a large result set.',
    suggestion: 'Consider adding LIMIT to prevent fetching too many rows.',
  };
}

// Rule: Cartesian product (multiple FROM tables without JOIN condition)
function checkCartesianProduct(ast) {
  if (ast.type !== 'SELECT') return null;
  // Check for implicit cross join via comma-separated tables
  // In our AST, this would show as FROM with no join condition
  if (ast.from && ast.joins && ast.joins.length > 0) {
    const hasNoCondition = ast.joins.some(j => j.type === 'CROSS' || (!j.on && j.type !== 'NATURAL'));
    if (hasNoCondition) {
      return {
        rule: 'cartesian-product',
        severity: SEVERITY.WARNING,
        message: 'CROSS JOIN or missing JOIN condition creates a Cartesian product.',
        suggestion: 'Add an ON clause or use explicit CROSS JOIN if intentional.',
      };
    }
  }
  return null;
}

// Rule: Deeply nested subqueries (>2 levels)
function checkNestedSubqueries(ast) {
  if (ast.type !== 'SELECT') return null;
  
  function depth(node, level) {
    if (!node) return level;
    let maxD = level;
    if (node.type === 'SELECT') maxD = Math.max(maxD, level + 1);
    for (const key of Object.keys(node)) {
      const val = node[key];
      if (val && typeof val === 'object') {
        if (Array.isArray(val)) {
          for (const item of val) {
            if (item && typeof item === 'object') maxD = Math.max(maxD, depth(item, level));
          }
        } else {
          maxD = Math.max(maxD, depth(val, level));
        }
      }
    }
    return maxD;
  }
  
  const d = depth(ast, 0);
  if (d > 3) {
    return {
      rule: 'deep-nesting',
      severity: SEVERITY.WARNING,
      message: `Query has ${d} levels of nesting — may be hard to understand and slow.`,
      suggestion: 'Consider refactoring with CTEs or temporary tables.',
    };
  }
  return null;
}

// Rule: Function call on column in WHERE (prevents index use)
function checkFunctionInWhere(ast) {
  if (ast.type !== 'SELECT' || !ast.where) return null;
  
  function hasFunctionOnColumn(node) {
    if (!node) return false;
    if ((node.type === 'function_call' || node.type === 'FUNCTION') && 
        node.args && node.args.some(a => a.type === 'column_ref')) {
      return true;
    }
    for (const key of Object.keys(node)) {
      const val = node[key];
      if (val && typeof val === 'object') {
        if (Array.isArray(val)) {
          if (val.some(item => item && typeof item === 'object' && hasFunctionOnColumn(item))) return true;
        } else {
          if (hasFunctionOnColumn(val)) return true;
        }
      }
    }
    return false;
  }
  
  if (hasFunctionOnColumn(ast.where)) {
    return {
      rule: 'function-in-where',
      severity: SEVERITY.WARNING,
      message: 'Function call on column in WHERE prevents index usage.',
      suggestion: 'Use computed/expression indexes, or restructure the condition.',
    };
  }
  return null;
}

// Rule: SELECT DISTINCT * — usually wrong
function checkSelectDistinctStar(ast) {
  if (ast.type !== 'SELECT' || !ast.distinct) return null;
  const hasStar = (ast.columns || []).some(c => 
    c.type === 'star' || c === '*' || (c.type === 'column' && c.name === '*')
  );
  if (hasStar) {
    return {
      rule: 'distinct-star',
      severity: SEVERITY.WARNING,
      message: 'SELECT DISTINCT * is almost always wrong — the entire row becomes the key.',
      suggestion: 'Select only the columns you need to deduplicate.',
    };
  }
  return null;
}

// Rule: ORDER BY ordinal position
function checkOrderByOrdinal(ast) {
  if (ast.type !== 'SELECT' || !ast.orderBy) return null;
  const hasOrdinal = ast.orderBy.some(o => {
    const col = o.column || '';
    return /^\d+$/.test(String(col));
  });
  if (hasOrdinal) {
    return {
      rule: 'order-by-ordinal',
      severity: SEVERITY.INFO,
      message: 'ORDER BY ordinal position (e.g., ORDER BY 1) is fragile.',
      suggestion: 'Use column names or aliases instead of position numbers.',
    };
  }
  return null;
}

// Rule: Implicit JOIN (comma syntax)
function checkImplicitJoin(ast) {
  // Detected by comma-separated tables in FROM
  // Our parser handles this differently, but check anyway
  return null;
}

// Rule: Comparing with NULL using = instead of IS NULL
function checkNullComparison(ast) {
  if (ast.type !== 'SELECT' || !ast.where) return null;
  
  function hasNullCompare(node) {
    if (!node) return false;
    if (node.type === 'COMPARE' && (node.op === 'EQ' || node.op === 'NEQ')) {
      if ((node.left?.type === 'literal' && node.left?.value === null) ||
          (node.right?.type === 'literal' && node.right?.value === null)) {
        return true;
      }
    }
    for (const key of Object.keys(node)) {
      const val = node[key];
      if (val && typeof val === 'object') {
        if (Array.isArray(val)) {
          if (val.some(item => item && typeof item === 'object' && hasNullCompare(item))) return true;
        } else {
          if (hasNullCompare(val)) return true;
        }
      }
    }
    return false;
  }
  
  if (hasNullCompare(ast.where)) {
    return {
      rule: 'null-comparison',
      severity: SEVERITY.ERROR,
      message: 'Comparing with NULL using = or != always returns NULL.',
      suggestion: 'Use IS NULL or IS NOT NULL instead.',
    };
  }
  return null;
}

// Rule: COUNT(*) vs COUNT(column) distinction
function checkCountStar(ast) {
  // This is informational — COUNT(*) counts rows, COUNT(col) counts non-null values
  return null;
}

// Rule: Unused table alias
function checkUnusedAlias(ast) {
  // Would need deeper AST analysis — skip for now
  return null;
}

// Rule: Leading wildcard in LIKE
function checkLeadingWildcard(ast) {
  if (ast.type !== 'SELECT' || !ast.where) return null;
  
  function hasLeadingWildcard(node) {
    if (!node) return false;
    if (node.type === 'LIKE' && (node.right?.type === 'literal' || node.pattern?.type === 'literal')) {
      const pattern = String((node.right || node.pattern).value);
      if (pattern.startsWith('%') || pattern.startsWith('_')) return true;
    }
    for (const key of Object.keys(node)) {
      const val = node[key];
      if (val && typeof val === 'object') {
        if (Array.isArray(val)) {
          if (val.some(item => item && typeof item === 'object' && hasLeadingWildcard(item))) return true;
        } else {
          if (hasLeadingWildcard(val)) return true;
        }
      }
    }
    return false;
  }
  
  if (hasLeadingWildcard(ast.where)) {
    return {
      rule: 'leading-wildcard',
      severity: SEVERITY.WARNING,
      message: "LIKE with leading wildcard (e.g., '%foo') prevents index usage.",
      suggestion: 'Consider full-text search or restructuring the query.',
    };
  }
  return null;
}

// Rule: Redundant DISTINCT with GROUP BY
function checkRedundantDistinct(ast) {
  if (ast.type !== 'SELECT') return null;
  if (ast.distinct && ast.groupBy) {
    return {
      rule: 'redundant-distinct',
      severity: SEVERITY.INFO,
      message: 'DISTINCT with GROUP BY is redundant — GROUP BY already produces unique groups.',
      suggestion: 'Remove DISTINCT for clarity.',
    };
  }
  return null;
}

/**
 * Lint a raw SQL string.
 * @param {string} sql - Raw SQL
 * @param {Function} parseFn - Parser function
 * @returns {Array} Array of lint issues
 */
export function lint(sql, parseFn) {
  try {
    const ast = parseFn(sql);
    return lintSQL(ast);
  } catch(e) {
    return [{ rule: 'parse-error', severity: 'error', message: e.message, suggestion: 'Fix the SQL syntax.' }];
  }
}
