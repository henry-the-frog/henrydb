// where-compiler.js — Compile WHERE clause ASTs into JavaScript filter functions
// For row-object mode (row is {colName: value}).
// Falls back to null if expression contains unsupported features (subqueries, etc.)

/**
 * Compile a WHERE expression AST into a filter function.
 * @param {object} expr - The WHERE clause AST
 * @param {object} [db] - Database instance (for fallback _evalExpr)
 * @returns {Function|null} (row) => boolean, or null if compilation not possible
 */
export function compileWhereFilter(expr, db) {
  if (!expr) return () => true;
  // Safety: don't compile expressions containing subqueries or correlated refs
  if (containsUnsafe(expr)) return null;
  try {
    const { code, params } = compileExprToCode(expr);
    if (!code) return null;
    // Build the compiled function with captured parameter values
    const paramNames = Object.keys(params);
    const paramValues = Object.values(params);
    const fn = new Function(...paramNames, 'row', `"use strict"; return (${code});`);
    return fn.bind(null, ...paramValues);
  } catch (e) {
    return null; // Fallback to interpreted
  }
}

/**
 * Check if an expression is safe to compile.
 * Unsafe if it contains subqueries, table-qualified column references,
 * or other constructs that need runtime context (outer rows, lateral scope, etc.)
 */
function containsUnsafe(expr) {
  if (!expr) return false;
  if (expr.type === 'EXISTS' || expr.type === 'IN_SUBQUERY' || expr.subquery) return true;
  // Table-qualified column refs need _resolveColumn which compiled filters don't support
  if (expr.type === 'column_ref' && expr.table) return true;
  if (expr.type === 'column_ref' && expr.name && expr.name.includes('.')) return true;
  for (const key of ['left', 'right', 'expr', 'condition', 'low', 'high']) {
    if (expr[key] && containsUnsafe(expr[key])) return true;
  }
  if (expr.values && Array.isArray(expr.values)) {
    for (const v of expr.values) {
      if (v && containsUnsafe(v)) return true;
    }
  }
  return false;
}

let paramCounter = 0;

/**
 * Compile an expression AST node into a JS code string.
 * Returns { code: string, params: {name: value} } or { code: null } if unsupported.
 */
function compileExprToCode(expr) {
  paramCounter = 0;
  const params = {};
  const code = _compile(expr, params);
  return { code, params };
}

function _compile(expr, params) {
  if (!expr) return 'true';
  
  switch (expr.type) {
    case 'AND': {
      const left = _compile(expr.left, params);
      const right = _compile(expr.right, params);
      if (!left || !right) return null;
      return `(${left}) && (${right})`;
    }
    case 'OR': {
      const left = _compile(expr.left, params);
      const right = _compile(expr.right, params);
      if (!left || !right) return null;
      return `(${left}) || (${right})`;
    }
    case 'NOT': {
      const inner = _compile(expr.expr, params);
      if (!inner) return null;
      return `!(${inner})`;
    }
    case 'COMPARE': {
      const left = _compileValue(expr.left, params);
      const right = _compileValue(expr.right, params);
      if (!left || !right) return null;
      switch (expr.op) {
        case 'EQ': return `${left} === ${right}`;
        case 'NE': return `${left} !== ${right}`;
        case 'LT': return `${left} < ${right}`;
        case 'GT': return `${left} > ${right}`;
        case 'LE': return `${left} <= ${right}`;
        case 'GE': return `${left} >= ${right}`;
        default: return null;
      }
    }
    case 'IS_NULL': {
      const val = _compileValue(expr.expr || expr.left, params);
      if (!val) return null;
      return `(${val} === null || ${val} === undefined)`;
    }
    case 'IS_NOT_NULL': {
      const val = _compileValue(expr.expr || expr.left, params);
      if (!val) return null;
      return `(${val} !== null && ${val} !== undefined)`;
    }
    case 'BETWEEN': {
      const val = _compileValue(expr.expr, params);
      const low = _compileValue(expr.low, params);
      const high = _compileValue(expr.high, params);
      if (!val || !low || !high) return null;
      return `(${val} >= ${low} && ${val} <= ${high})`;
    }
    case 'LIKE':
    case 'ILIKE': {
      const val = _compileValue(expr.left, params);
      if (!val) return null;
      // Convert LIKE pattern to regex
      const pattern = expr.right?.value;
      if (typeof pattern !== 'string') return null;
      const regexStr = likeToRegex(pattern, expr.type === 'ILIKE');
      const pName = `_p${paramCounter++}`;
      params[pName] = new RegExp(regexStr);
      return `${pName}.test(String(${val}))`;
    }
    case 'IN_LIST': {
      const val = _compileValue(expr.expr || expr.left, params);
      if (!val) return null;
      const items = expr.values || expr.list;
      if (!items || !Array.isArray(items)) return null;
      const compiled = items.map(item => {
        if (item.type === 'literal' || item.value !== undefined) {
          return JSON.stringify(item.value);
        }
        return null;
      });
      if (compiled.some(c => c === null)) return null;
      const pName = `_p${paramCounter++}`;
      params[pName] = new Set(items.map(i => i.value));
      return `${pName}.has(${val})`;
    }
    case 'IN_HASHSET': {
      const val = _compileValue(expr.expr || expr.left, params);
      if (!val) return null;
      const pName = `_p${paramCounter++}`;
      params[pName] = expr.hashSet || expr.hashset || expr.set;
      if (!params[pName]) return null;
      return `${pName}.has(${val})`;
    }
    case 'NOT_IN_HASHSET': {
      const val = _compileValue(expr.expr || expr.left, params);
      if (!val) return null;
      const pName = `_p${paramCounter++}`;
      params[pName] = expr.hashSet || expr.hashset || expr.set;
      if (!params[pName]) return null;
      return `!${pName}.has(${val})`;
    }
    case 'LITERAL_BOOL': {
      return expr.value ? 'true' : 'false';
    }
    case 'binary_expr': {
      const left = _compileValue(expr.left || expr, params);
      const right = _compileValue(expr.right, params);
      if (!left || !right) return null;
      switch (expr.op) {
        case '=': return `${left} === ${right}`;
        case '!=': case '<>': return `${left} !== ${right}`;
        case '<': return `${left} < ${right}`;
        case '>': return `${left} > ${right}`;
        case '<=': return `${left} <= ${right}`;
        case '>=': return `${left} >= ${right}`;
        default: return null;
      }
    }
    default:
      return null; // Unsupported — fallback to interpreted
  }
}

function _compileValue(expr, params) {
  if (!expr) return null;
  
  if (expr.type === 'column_ref') {
    const name = expr.table ? `${expr.table}.${expr.name}` : expr.name;
    // Generate property access — row[name]
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
      return `row.${name}`;
    }
    return `row[${JSON.stringify(name)}]`;
  }
  
  if (expr.type === 'literal' || expr.value !== undefined) {
    const val = expr.value;
    if (val === null) return 'null';
    if (typeof val === 'string') return JSON.stringify(val);
    if (typeof val === 'number') return String(val);
    if (typeof val === 'boolean') return String(val);
    return null;
  }
  
  // Arithmetic expressions
  if (expr.type === 'binary_expr' || (expr.op && expr.left && expr.right)) {
    const left = _compileValue(expr.left, params);
    const right = _compileValue(expr.right, params);
    if (!left || !right) return null;
    switch (expr.op) {
      case '+': return `(${left} + ${right})`;
      case '-': return `(${left} - ${right})`;
      case '*': return `(${left} * ${right})`;
      case '/': return `(${left} / ${right})`;
      case '%': return `(${left} % ${right})`;
      default: return null;
    }
  }
  
  // Unary minus
  if (expr.type === 'unary_expr' && expr.op === '-') {
    const operand = _compileValue(expr.expr, params);
    if (!operand) return null;
    return `(-${operand})`;
  }
  
  // CASE expressions — compile to nested ternaries
  if (expr.type === 'CASE' || expr.type === 'case') {
    const whens = expr.whens || [];
    if (whens.length === 0) return null;
    // Build nested ternary: cond1 ? val1 : cond2 ? val2 : ... : elseVal
    let result = expr.else ? _compileValue(expr.else, params) : 'null';
    if (result === null) result = 'null';
    // Build from end to start
    for (let i = whens.length - 1; i >= 0; i--) {
      const w = whens[i];
      const cond = _compile(w.when, params);
      const val = _compileValue(w.then, params);
      if (!cond || !val) return null; // Bail if any branch unsupported
      result = `(${cond} ? ${val} : ${result})`;
    }
    return result;
  }
  
  // Function calls
  if (expr.type === 'function_call' || expr.type === 'FUNCTION') {
    return null; // Would need function registry
  }
  
  return null;
}

function likeToRegex(pattern, caseInsensitive) {
  let regex = '^';
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i];
    if (c === '%') {
      regex += '.*';
    } else if (c === '_') {
      regex += '.';
    } else if ('.+*?^${}()|[]\\'.includes(c)) {
      regex += '\\' + c;
    } else {
      regex += c;
    }
  }
  regex += '$';
  return caseInsensitive ? regex : regex;
}

/**
 * Compile a SET value expression (e.g., `column + 1`, `'hello'`, `price * 1.1`)
 * into an optimized function (row) => value.
 * Returns null if compilation not possible (fallback to _evalValue).
 * 
 * Unlike compileWhereFilter (returns boolean), this returns any value type.
 */
export function compileSetExpr(expr) {
  if (!expr) return null;
  if (containsUnsafe(expr)) return null;
  try {
    paramCounter = 0;
    const params = {};
    const code = _compileValue(expr, params);
    if (!code) return null;
    const paramNames = Object.keys(params);
    const paramValues = Object.values(params);
    const fn = new Function(...paramNames, 'row', `"use strict"; return (${code});`);
    return fn.bind(null, ...paramValues);
  } catch (e) {
    return null;
  }
}

/**
 * Compile a batch of SET assignments [{column, value: expr}] into an optimized
 * function that applies all SET operations at once.
 * Returns: { compiledAssignments: [{column, colIdx, fn}] } or null
 * 
 * @param {Array} assignments - [{column: string, value: AST}]
 * @param {Array} schema - table schema [{name: string, ...}]
 * @returns {Array|null} array of {column, colIdx, fn} or null if any can't compile
 */
export function compileSetBatch(assignments, schema) {
  if (!assignments || !assignments.length) return null;
  const compiled = [];
  for (const { column, value } of assignments) {
    const colIdx = schema.findIndex(c => c.name === column);
    if (colIdx === -1) return null;
    const fn = compileSetExpr(value);
    if (!fn) return null; // One failed = bail to interpreted for all
    compiled.push({ column, colIdx, fn });
  }
  return compiled;
}
