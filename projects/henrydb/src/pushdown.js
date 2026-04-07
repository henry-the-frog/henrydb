// pushdown.js — Predicate pushdown optimizer for HenryDB
// Pushes WHERE conditions through JOINs to filter rows earlier

/**
 * Extract all table references from an expression
 */
function getTableRefs(expr) {
  if (!expr) return new Set();
  const refs = new Set();
  
  if (expr.type === 'column_ref') {
    const name = expr.name;
    if (name.includes('.')) refs.add(name.split('.')[0]);
    return refs;
  }
  
  for (const key of Object.keys(expr)) {
    const val = expr[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        for (const item of val) {
          if (typeof item === 'object') {
            for (const r of getTableRefs(item)) refs.add(r);
          }
        }
      } else {
        for (const r of getTableRefs(val)) refs.add(r);
      }
    }
  }
  
  return refs;
}

/**
 * Split an AND expression into individual conjuncts
 */
function splitConjuncts(expr) {
  if (!expr) return [];
  if (expr.type === 'AND') {
    return [...splitConjuncts(expr.left), ...splitConjuncts(expr.right)];
  }
  return [expr];
}

/**
 * Combine conjuncts back into AND expression
 */
function combineConjuncts(conjuncts) {
  if (conjuncts.length === 0) return null;
  if (conjuncts.length === 1) return conjuncts[0];
  return conjuncts.reduce((acc, c) => ({ type: 'AND', left: acc, right: c }));
}

/**
 * Push predicates from WHERE clause down to individual table scans
 * 
 * Input AST: SELECT ... FROM A JOIN B ON ... WHERE A.x = 1 AND B.y = 2
 * Output: Modified AST where A's scan filter includes A.x = 1 
 *         and B's scan filter includes B.y = 2
 * 
 * Returns: { ast (modified), pushed: number of predicates pushed }
 */
export function pushdownPredicates(ast) {
  if (!ast.where || !ast.joins || ast.joins.length === 0) {
    return { ast, pushed: 0 };
  }

  // Collect all table names/aliases
  const tables = new Map(); // alias → tableName
  const fromName = ast.from.alias || ast.from.table;
  tables.set(fromName, ast.from.table);
  
  for (const join of ast.joins) {
    const alias = join.alias || join.table;
    tables.set(alias, join.table);
  }

  // Split WHERE into conjuncts
  const conjuncts = splitConjuncts(ast.where);
  
  const remaining = [];     // Conjuncts that stay in WHERE
  const tablePushdowns = new Map(); // tableAlias → [conjuncts to push]
  let pushed = 0;

  for (const conj of conjuncts) {
    const refs = getTableRefs(conj);
    
    // Can push down if it references exactly one table
    if (refs.size === 1) {
      const tableRef = [...refs][0];
      if (tables.has(tableRef)) {
        if (!tablePushdowns.has(tableRef)) tablePushdowns.set(tableRef, []);
        tablePushdowns.get(tableRef).push(conj);
        pushed++;
        continue;
      }
    }
    
    // Also push down if no table prefix but all columns belong to one table
    if (refs.size === 0) {
      // Unqualified columns — try to determine which table they belong to
      // For now, leave these in WHERE (conservative)
      remaining.push(conj);
      continue;
    }
    
    remaining.push(conj);
  }

  if (pushed === 0) return { ast, pushed: 0 };

  // Build new AST with pushed predicates
  const newAst = { ...ast };
  
  // Push to FROM table filter
  if (tablePushdowns.has(fromName)) {
    newAst.from = {
      ...newAst.from,
      filter: combineConjuncts(tablePushdowns.get(fromName)),
    };
  }

  // Push to JOIN table filters
  newAst.joins = ast.joins.map(join => {
    const alias = join.alias || join.table;
    if (tablePushdowns.has(alias)) {
      return {
        ...join,
        filter: combineConjuncts(tablePushdowns.get(alias)),
      };
    }
    return join;
  });

  // Update WHERE with remaining predicates
  newAst.where = combineConjuncts(remaining);

  return { ast: newAst, pushed };
}
