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
    // Use alias as the inner scope's identifier when available;
    // the raw table name should NOT be added when aliased, as it
    // could conflict with outer table references (e.g., FROM t t2
    // should have inner scope "t2", not "t" which is the outer table)
    const alias = subqueryAst.from.alias || subqueryAst.from.table;
    if (alias) innerTables.add(alias);
  }
  for (const join of subqueryAst.joins || []) {
    const alias = join.alias || join.table;
    if (alias) innerTables.add(alias);
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
/**
 * If expr is an uncorrelated scalar subquery, evaluate it once and return a literal.
 * Otherwise return the expr unchanged.
 */
function hoistScalarSubquery(expr, outerTables, db) {
  if (!expr || expr.type !== 'SUBQUERY') return expr;
  
  // Check if the subquery references any outer tables
  if (isCorrelated(expr.subquery, outerTables)) return expr;
  
  // Uncorrelated: evaluate once and replace with literal
  try {
    const result = db._select(expr.subquery);
    if (result.rows.length === 0) {
      return { type: 'literal', value: null };
    }
    const firstRow = result.rows[0];
    const value = Object.values(firstRow)[0];
    return { type: 'literal', value: value ?? null };
  } catch (e) {
    // If evaluation fails, leave the subquery in place
    return expr;
  }
}

/**
 * Try to batch-decorrelate a correlated IN subquery.
 * 
 * Pattern: WHERE outer.x IN (SELECT inner.y FROM t2 WHERE inner.key = outer.key)
 * 
 * Strategy:
 * 1. Extract correlation predicate: inner.key = outer.key
 * 2. Execute inner query without correlation: SELECT key, y FROM t2
 * 3. Build Map<outerKeyVal, Set<innerVal>>
 * 4. Return a CORRELATED_IN_HASHMAP node that looks up per row
 * 
 * Returns null if the pattern doesn't match.
 */
function tryBatchDecorrelate(expr, outerTables, db) {
  const subquery = expr.subquery;
  if (!subquery || !subquery.where) return null;
  
  // Collect inner tables
  const innerTables = new Set();
  if (subquery.from) {
    innerTables.add(subquery.from.alias || subquery.from.table);
  }
  for (const join of subquery.joins || []) {
    innerTables.add(join.alias || join.table);
  }
  
  // Split WHERE into correlation predicates and inner-only predicates
  const allPreds = flattenAnd(subquery.where);
  const correlationPreds = [];
  const innerPreds = [];
  
  for (const pred of allPreds) {
    if (isCorrelationPred(pred, innerTables, outerTables)) {
      correlationPreds.push(pred);
    } else {
      innerPreds.push(pred);
    }
  }
  
  if (correlationPreds.length === 0) return null; // Not correlated
  
  // Extract correlation mappings: outerCol → innerCol
  const mappings = [];
  for (const pred of correlationPreds) {
    const mapping = extractEqMapping(pred, innerTables, outerTables);
    if (!mapping) return null; // Non-equality correlation — can't decorrelate
    mappings.push(mapping);
  }
  
  // Build the decorrelated inner query: remove correlation predicates, add
  // the inner correlation columns to the SELECT list
  try {
    const innerSelectCols = [...(subquery.columns || [])];
    // Add correlation inner columns if not already selected
    const innerColNames = mappings.map(m => m.innerCol);
    
    // Build a modified subquery AST without correlation predicates
    const modifiedSubquery = {
      ...subquery,
      where: innerPreds.length > 0 ? buildAnd(innerPreds) : null,
      columns: [
        ...innerSelectCols,
        ...innerColNames.map(col => ({
          type: 'column',
          name: col,
          alias: `__corr_${col.replace(/\./g, '_')}`,
        })),
      ],
    };
    
    // Execute the inner query once (no correlation)
    const result = db._select(modifiedSubquery);
    
    // Build multi-valued hash map: compositeKey → Set<innerVal>
    const hashMap = new Map();
    const selectCol = subquery.columns?.[0];
    const targetColName = selectCol?.alias || selectCol?.name || 
      (selectCol?.type === 'aggregate' ? `${selectCol.func}(${selectCol.arg})` : null);
    
    for (const row of result.rows) {
      // Build composite key from correlation columns
      const keyParts = mappings.map(m => {
        const alias = `__corr_${m.innerCol.replace(/\./g, '_')}`;
        return row[alias] ?? row[m.innerCol] ?? row[m.innerCol.split('.').pop()];
      });
      const key = keyParts.length === 1 ? String(keyParts[0]) : keyParts.map(String).join('\0');
      
      // Get the value column (first column from original SELECT)
      const keys = Object.keys(row);
      let val;
      if (targetColName) {
        val = row[targetColName] ?? row[keys[0]];
      } else {
        val = row[keys[0]];
      }
      
      if (!hashMap.has(key)) hashMap.set(key, new Set());
      hashMap.get(key).add(val);
    }
    
    return {
      type: 'CORRELATED_IN_HASHMAP',
      left: expr.left,
      outerCols: mappings.map(m => m.outerCol),
      hashMap,
      negated: expr.negated || false,
    };
  } catch (e) {
    // If anything fails, fall back to correlated execution
    return null;
  }
}

function flattenAnd(expr) {
  if (!expr) return [];
  if (expr.type === 'AND') return [...flattenAnd(expr.left), ...flattenAnd(expr.right)];
  return [expr];
}

function buildAnd(preds) {
  if (preds.length === 0) return null;
  return preds.length === 1 ? preds[0] : preds.reduce((a, b) => ({ type: 'AND', left: a, right: b }));
}

/**
 * Check if a predicate references both inner and outer tables.
 */
function isCorrelationPred(pred, innerTables, outerTables) {
  const refs = collectColumnRefs(pred);
  let hasInner = false, hasOuter = false;
  for (const ref of refs) {
    if (innerTables.has(ref)) hasInner = true;
    else if (outerTables.has(ref)) hasOuter = true;
  }
  return hasInner && hasOuter;
}

/**
 * Extract an equality mapping from a correlation predicate.
 * E.g., t2.id = t1.id → { innerCol: 't2.id', outerCol: 't1.id' }
 * Returns null for non-equality predicates.
 */
function extractEqMapping(pred, innerTables, outerTables) {
  if (pred.type !== 'COMPARE' || pred.op !== 'EQ') return null;
  
  const leftCol = getColumnRef(pred.left);
  const rightCol = getColumnRef(pred.right);
  if (!leftCol || !rightCol) return null;
  
  const leftTable = leftCol.includes('.') ? leftCol.split('.')[0] : null;
  const rightTable = rightCol.includes('.') ? rightCol.split('.')[0] : null;
  
  if (leftTable && innerTables.has(leftTable) && rightTable && outerTables.has(rightTable)) {
    return { innerCol: leftCol, outerCol: rightCol };
  }
  if (rightTable && innerTables.has(rightTable) && leftTable && outerTables.has(leftTable)) {
    return { innerCol: rightCol, outerCol: leftCol };
  }
  return null;
}

function getColumnRef(expr) {
  if (!expr) return null;
  if (expr.type === 'column_ref') return expr.name;
  if (expr.type === 'column') return expr.name;
  return null;
}

export function decorrelateExpr(expr, outerTables, db) {
  if (!expr) return expr;

  // Hoist uncorrelated scalar subqueries to literal values
  // This covers: WHERE val > (SELECT AVG(x) FROM t)
  if (expr.type === 'COMPARE') {
    const left = hoistScalarSubquery(expr.left, outerTables, db);
    const right = hoistScalarSubquery(expr.right, outerTables, db);
    if (left !== expr.left || right !== expr.right) {
      expr = { ...expr, left, right };
    }
  }

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
    // Try batch decorrelation: extract correlation predicate, run inner query
    // once without it, build a hash map for fast lookup.
    const decorrelated = tryBatchDecorrelate(expr, outerTables, db);
    if (decorrelated) return decorrelated;
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
