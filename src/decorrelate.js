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
function isCorrelated(subqueryAst, outerTables, db) {
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
  
  // Collect inner table column names
  const innerColumns = new Set();
  if (db && db.tables) {
    for (const tbl of innerTables) {
      const table = db.tables.get(tbl) || db.tables.get(tbl.toLowerCase());
      if (table && table.schema) {
        for (const col of table.schema) {
          innerColumns.add(col.name);
          innerColumns.add(col.name.toUpperCase());
          innerColumns.add(col.name.toLowerCase());
        }
      }
    }
  }
  
  for (const ref of refs) {
    // Qualified reference: check if table prefix is an outer table
    if (!innerTables.has(ref) && outerTables.has(ref)) return true;
    
    // Unqualified reference: check if it's NOT an inner table column
    if (!innerTables.has(ref) && !outerTables.has(ref) && !innerColumns.has(ref)) {
      // This column isn't from any inner table — must be an outer reference
      return true;
    }
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
    if (!isCorrelated(expr.subquery, outerTables, db)) {
      // Uncorrelated: execute once, build hash set
      const result = db._select(expr.subquery);
      const values = new Set();
      for (const row of result.rows) {
        const vals = Object.values(row);
        if (vals.length > 0) values.add(vals[0]);
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
    if (!isCorrelated(expr.subquery, outerTables, db)) {
      // Uncorrelated EXISTS: evaluate once
      const result = db._select(expr.subquery);
      return {
        type: 'LITERAL_BOOL',
        value: result.rows.length > 0,
      };
    }
    // Correlated EXISTS → batch hash semi-join
    return decorrelateExists(expr, outerTables, db, false);
  }

  // Optimize NOT EXISTS
  if (expr.type === 'NOT_EXISTS') {
    if (!isCorrelated(expr.subquery, outerTables, db)) {
      const result = db._select(expr.subquery);
      return {
        type: 'LITERAL_BOOL',
        value: result.rows.length === 0,
      };
    }
    // Correlated NOT EXISTS → batch hash anti-join
    return decorrelateExists(expr, outerTables, db, true);
  }

  return expr;
}

/**
 * Decorrelate a correlated EXISTS/NOT EXISTS into a batch hash semi-join.
 * 
 * Pattern: WHERE EXISTS (SELECT 1 FROM inner WHERE inner.col = outer.col AND local_pred)
 * → Execute subquery without correlation pred, build hash set, filter outer rows.
 * 
 * Returns a SEMI_HASH_JOIN or ANTI_HASH_JOIN node.
 */
function decorrelateExists(expr, outerTables, db, negated) {
  const subquery = expr.subquery;
  if (!subquery.where) return expr; // Can't decorrelate without WHERE

  // Split WHERE into correlation predicates and local predicates
  const { correlationPreds, localPreds } = splitCorrelationPredicates(
    subquery.where, outerTables, getInnerTables(subquery)
  );

  if (correlationPreds.length === 0) return expr; // No simple correlation found

  // For each correlation predicate (outer.col = inner.col),
  // extract the outer column ref and inner column ref
  const outerCols = [];
  const innerCols = [];
  for (const pred of correlationPreds) {
    if (pred.type !== '=') continue;
    const { outerRef, innerRef } = classifyEqualityRefs(pred, outerTables);
    if (outerRef && innerRef) {
      outerCols.push(outerRef);
      innerCols.push(innerRef);
    }
  }

  if (outerCols.length === 0) return expr; // No equality correlations

  // Build the decorrelated subquery: SELECT DISTINCT inner_cols FROM inner WHERE local_preds
  const decorrelatedSubquery = {
    ...subquery,
    columns: innerCols.map(col => ({ type: 'column_ref', name: col })),
    where: localPreds.length > 0 ? combineAND(localPreds) : undefined,
    distinct: true,
  };

  // Execute the decorrelated subquery once
  try {
    const result = db._select(decorrelatedSubquery);
    
    // Build hash set from results
    if (innerCols.length === 1) {
      // Single-column: simple Set
      const hashSet = new Set();
      for (const row of result.rows) {
        const vals = Object.values(row);
        if (vals.length > 0 && vals[0] != null) hashSet.add(vals[0]);
      }
      
      return {
        type: negated ? 'NOT_IN_HASHSET' : 'IN_HASHSET',
        left: { type: 'column_ref', name: outerCols[0] },
        hashSet,
        negated,
      };
    } else {
      // Multi-column: composite key hash set
      const hashSet = new Set();
      for (const row of result.rows) {
        const vals = Object.values(row);
        hashSet.add(JSON.stringify(vals));
      }

      return {
        type: negated ? 'NOT_IN_COMPOSITE_HASHSET' : 'IN_COMPOSITE_HASHSET',
        outerCols,
        hashSet,
        negated,
      };
    }
  } catch {
    // Fallback: if decorrelated query fails, leave as-is
    return expr;
  }
}

/**
 * Get inner table names from a subquery AST.
 */
function getInnerTables(ast) {
  const tables = new Set();
  if (ast.from) {
    tables.add(ast.from.table);
    if (ast.from.alias) tables.add(ast.from.alias);
  }
  for (const join of ast.joins || []) {
    tables.add(join.table);
    if (join.alias) tables.add(join.alias);
  }
  return tables;
}

/**
 * Split a WHERE expression into correlation predicates (referencing outer tables)
 * and local predicates (only referencing inner tables).
 */
function splitCorrelationPredicates(where, outerTables, innerTables) {
  const correlationPreds = [];
  const localPreds = [];
  
  const preds = flattenAND(where);
  for (const pred of preds) {
    const refs = collectColumnRefs(pred);
    let hasOuter = false;
    for (const ref of refs) {
      if (outerTables.has(ref) && !innerTables.has(ref)) {
        hasOuter = true;
        break;
      }
    }
    if (hasOuter) {
      correlationPreds.push(pred);
    } else {
      localPreds.push(pred);
    }
  }
  
  return { correlationPreds, localPreds };
}

/**
 * Flatten an AND tree into a list of conjuncts.
 */
function flattenAND(expr) {
  if (!expr) return [];
  if (expr.type === 'AND') {
    return [...flattenAND(expr.left), ...flattenAND(expr.right)];
  }
  return [expr];
}

/**
 * Combine a list of predicates into an AND tree.
 */
function combineAND(preds) {
  if (preds.length === 0) return undefined;
  if (preds.length === 1) return preds[0];
  return preds.reduce((acc, p) => ({ type: 'AND', left: acc, right: p }));
}

/**
 * For an equality predicate (a = b), classify which side is outer and which is inner.
 * Returns { outerRef: string, innerRef: string } or nulls if not a simple equality.
 */
function classifyEqualityRefs(pred, outerTables) {
  if (pred.type !== '=') return { outerRef: null, innerRef: null };
  
  const leftRefs = collectColumnRefs(pred.left);
  const rightRefs = collectColumnRefs(pred.right);
  
  let leftIsOuter = false, rightIsOuter = false;
  for (const ref of leftRefs) {
    if (outerTables.has(ref)) { leftIsOuter = true; break; }
  }
  for (const ref of rightRefs) {
    if (outerTables.has(ref)) { rightIsOuter = true; break; }
  }
  
  if (leftIsOuter && !rightIsOuter) {
    return { 
      outerRef: pred.left.name || `${pred.left.table}.${pred.left.column}`,
      innerRef: pred.right.name || `${pred.right.table}.${pred.right.column}`,
    };
  }
  if (rightIsOuter && !leftIsOuter) {
    return {
      outerRef: pred.right.name || `${pred.right.table}.${pred.right.column}`,
      innerRef: pred.left.name || `${pred.left.table}.${pred.left.column}`,
    };
  }
  
  return { outerRef: null, innerRef: null };
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
