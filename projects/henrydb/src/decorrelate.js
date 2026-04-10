// decorrelate.js — Subquery decorrelation optimizer for HenryDB
// Transforms: WHERE x IN (SELECT ...) → semi-join or hash lookup
// Transforms: WHERE EXISTS (SELECT ... WHERE outer.col = inner.col) → semi-join

/**
 * Analyze whether an expression references any columns from outside tables.
 * Returns the set of table aliases referenced.
 */
function collectColumnRefs(expr, refs = new Set()) {
  if (!expr) return refs;
  if (expr.type === 'column_ref') {
    const name = expr.name;
    if (name.includes('.')) refs.add(name.split('.')[0]);
    else refs.add(name);
    return refs;
  }
  // Recurse through all properties
  for (const key of Object.keys(expr)) {
    const val = expr[key];
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      collectColumnRefs(val, refs);
    } else if (Array.isArray(val)) {
      for (const item of val) {
        if (item && typeof item === 'object') collectColumnRefs(item, refs);
      }
    }
  }
  return refs;
}

/**
 * Check if a subquery references any outer table columns
 * (i.e., is it correlated?)
 */
function isCorrelated(subqueryAst, outerTables) {
  const refs = collectColumnRefs(subqueryAst.where);
  const innerTables = new Set();
  if (subqueryAst.from) {
    innerTables.add(subqueryAst.from.table);
    if (subqueryAst.from.alias) innerTables.add(subqueryAst.from.alias);
  }
  for (const join of subqueryAst.joins || []) {
    innerTables.add(join.table);
    if (join.alias) innerTables.add(join.alias);
  }
  
  // Check if any referenced table is NOT in the inner query
  for (const ref of refs) {
    if (!innerTables.has(ref) && outerTables.has(ref)) return true;
  }
  return false;
}

/**
 * Optimize IN_SUBQUERY expressions.
 * For uncorrelated subqueries: execute once, build a Set, return optimized node.
 * For correlated: leave as-is (future: convert to semi-join).
 */
export function decorrelateExpr(expr, outerTables, db) {
  if (!expr) return expr;

  // Recurse into AND/OR
  if (expr.type === 'AND') {
    return {
      ...expr,
      left: decorrelateExpr(expr.left, outerTables, db),
      right: decorrelateExpr(expr.right, outerTables, db),
    };
  }
  if (expr.type === 'OR') {
    return {
      ...expr,
      left: decorrelateExpr(expr.left, outerTables, db),
      right: decorrelateExpr(expr.right, outerTables, db),
    };
  }
  if (expr.type === 'NOT') {
    return { ...expr, expr: decorrelateExpr(expr.expr, outerTables, db) };
  }

  // Optimize IN_SUBQUERY
  if (expr.type === 'IN_SUBQUERY') {
    if (!isCorrelated(expr.subquery, outerTables)) {
      // Uncorrelated: execute once, build hash set
      const result = db._select(expr.subquery);
      const values = new Set();
      for (const row of result.rows) {
        const keys = Object.keys(row);
        // Use the first column from the SELECT list (the projected column)
        // For "SELECT MAX(val) FROM g GROUP BY cat", this should be MAX(val),
        // but Object.keys may put GROUP BY column first.
        // Find the first column that matches the subquery's SELECT columns.
        const selectCols = expr.subquery.columns;
        let targetCol = keys[0]; // default: first column
        if (selectCols && selectCols.length > 0) {
          const firstSelect = selectCols[0];
          const selectName = firstSelect.alias || 
            (firstSelect.type === 'aggregate' ? `${firstSelect.func}(${firstSelect.arg})` : firstSelect.name);
          // Look for a matching key
          for (const k of keys) {
            if (k === selectName || k.toUpperCase() === selectName.toUpperCase()) {
              targetCol = k;
              break;
            }
          }
        }
        values.add(row[targetCol]);
      }
      return {
        type: 'IN_HASHSET',
        left: expr.left,
        hashSet: values,
        negated: expr.negated || false,
      };
    }
    // TODO: Correlated IN subquery → semi-join
    return expr;
  }

  // Optimize EXISTS
  if (expr.type === 'EXISTS') {
    if (!isCorrelated(expr.subquery, outerTables)) {
      // Uncorrelated EXISTS: evaluate once
      const result = db._select(expr.subquery);
      return {
        type: 'LITERAL_BOOL',
        value: result.rows.length > 0,
      };
    }
    // Correlated EXISTS stays as-is (already using outerRow)
    return expr;
  }

  // Optimize NOT EXISTS
  if (expr.type === 'NOT_EXISTS') {
    if (!isCorrelated(expr.subquery, outerTables)) {
      const result = db._select(expr.subquery);
      return {
        type: 'LITERAL_BOOL',
        value: result.rows.length === 0,
      };
    }
    return expr;
  }

  return expr;
}

/**
 * Optimize a SELECT AST: decorrelate subqueries in WHERE clause
 */
export function optimizeSelect(ast, db) {
  if (!ast.where) return ast;

  const outerTables = new Set();
  if (ast.from) {
    outerTables.add(ast.from.table);
    if (ast.from.alias) outerTables.add(ast.from.alias);
  }
  for (const join of ast.joins || []) {
    outerTables.add(join.table);
    if (join.alias) outerTables.add(join.alias);
  }

  const optimizedWhere = decorrelateExpr(ast.where, outerTables, db);
  
  if (optimizedWhere !== ast.where) {
    return { ...ast, where: optimizedWhere };
  }
  return ast;
}
