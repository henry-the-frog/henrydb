// compiler.js — Query compiler for HenryDB
// Generates JavaScript functions from WHERE predicates for fast execution.

/**
 * Compile a WHERE clause AST into a JavaScript filter function.
 * Returns a function (row) => boolean that can be called on each row.
 */
export function compileWhere(expr) {
  if (!expr) return () => true;
  const code = compileExpr(expr);
  try {
    return new Function('row', `return ${code};`);
  } catch (e) {
    // Fall back to interpretation if compilation fails
    return null;
  }
}

/**
 * Compile a SELECT column list into a projection function.
 * Returns a function (row) => projected_row.
 */
export function compileProjection(columns, schema) {
  if (columns.length === 1 && (columns[0].name === '*' || columns[0].type === 'star')) {
    return null; // Use default star projection
  }
  
  const assignments = columns.map(col => {
    const alias = col.alias || col.name;
    if (col.type === 'column') {
      return `  ${JSON.stringify(alias)}: row[${JSON.stringify(col.name)}]`;
    }
    if (col.type === 'expression' && col.expr) {
      try {
        const code = compileExpr(col.expr);
        return `  ${JSON.stringify(alias)}: ${code}`;
      } catch { return null; }
    }
    return null;
  }).filter(Boolean);
  
  if (assignments.length !== columns.length) return null; // Some columns couldn't be compiled
  
  const code = `return {\n${assignments.join(',\n')}\n};`;
  try {
    return new Function('row', code);
  } catch { return null; }
}

/**
 * Compile an ORDER BY into a comparator function.
 */
export function compileOrderBy(orderBy) {
  if (!orderBy || orderBy.length === 0) return null;
  
  const comparisons = orderBy.map(({ column, direction }) => {
    const col = JSON.stringify(column);
    if (direction === 'DESC') {
      return `if (a[${col}] < b[${col}]) return 1; if (a[${col}] > b[${col}]) return -1;`;
    }
    return `if (a[${col}] < b[${col}]) return -1; if (a[${col}] > b[${col}]) return 1;`;
  });
  
  const code = comparisons.join('\n') + '\nreturn 0;';
  try {
    return new Function('a', 'b', code);
  } catch { return null; }
}

/**
 * Compile an expression AST node into a JavaScript expression string.
 */
function compileExpr(node) {
  if (!node) return 'true';
  
  switch (node.type) {
    case 'literal':
      if (node.value === null) return 'null';
      if (typeof node.value === 'string') return JSON.stringify(node.value);
      return String(node.value);
    
    case 'column_ref':
      return `row[${JSON.stringify(node.name)}]`;
    
    case 'COMPARE': {
      const left = compileExpr(node.left);
      const right = compileExpr(node.right);
      const ops = { 'EQ': '===', 'NEQ': '!==', 'LT': '<', 'GT': '>', 'LTE': '<=', 'GTE': '>=' };
      const jsOp = ops[node.op];
      if (!jsOp) throw new Error(`Unsupported operator: ${node.op}`);
      return `(${left} ${jsOp} ${right})`;
    }
    
    case 'AND':
      return `(${compileExpr(node.left)} && ${compileExpr(node.right)})`;
    
    case 'OR':
      return `(${compileExpr(node.left)} || ${compileExpr(node.right)})`;
    
    case 'NOT':
      return `!(${compileExpr(node.expr)})`;
    
    case 'IS_NULL':
      return `(${compileExpr(node.expr)} == null)`;
    
    case 'IS_NOT_NULL':
      return `(${compileExpr(node.expr)} != null)`;
    
    case 'BETWEEN': {
      const val = compileExpr(node.expr);
      const low = compileExpr(node.low);
      const high = compileExpr(node.high);
      return `(${val} >= ${low} && ${val} <= ${high})`;
    }
    
    case 'arith': {
      const left = compileExpr(node.left);
      const right = compileExpr(node.right);
      const ops = { '+': '+', '-': '-', '*': '*', '/': '/' };
      return `(${left} ${ops[node.op] || node.op} ${right})`;
    }
    
    case 'LIKE': {
      const val = compileExpr(node.left);
      const pattern = node.right.value;
      // Convert SQL LIKE pattern to regex
      const regex = pattern
        .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex special chars
        .replace(/%/g, '.*')    // % → .*
        .replace(/_/g, '.');    // _ → .
      return `(/^${regex}$/i.test(${val}))`;
    }
    
    default:
      throw new Error(`Cannot compile expression type: ${node.type}`);
  }
}

export { compileExpr };
