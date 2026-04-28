// compiled-expr.js — Compile SQL WHERE expressions to JS functions
// For simple expressions (=, <, >, <=, >=, <>, AND, OR, NOT, IS NULL, literals, column refs)
// Falls back to _evalExpr for complex cases

/**
 * Compile a WHERE AST node into a JS function: (row) => boolean
 * Returns null if the expression is too complex to compile.
 */
export function compileExpr(expr) {
  if (!expr) return () => true;
  
  try {
    // Don't compile if AST contains mutable param nodes (prepared stmts use in-place mutation)
    if (_containsParam(expr)) return null;
    const code = _emitExpr(expr);
    if (code === null) return null;
    // Use Function constructor for fast execution
    return new Function('row', `return ${code};`);
  } catch {
    return null; // Fallback to interpreter
  }
}

/**
 * Check if an AST contains any PARAM or mutable nodes.
 */
function _containsParam(node) {
  if (!node || typeof node !== 'object') return false;
  if (node.type === 'PARAM') return true;
  // Check if any value previously held a PARAM (after fastBind mutation)
  for (const key of Object.keys(node)) {
    const val = node[key];
    if (Array.isArray(val)) {
      for (const item of val) {
        if (_containsParam(item)) return true;
      }
    } else if (val && typeof val === 'object' && val.type) {
      if (_containsParam(val)) return true;
    }
  }
  return false;
}

/**
 * Emit JS code string for an expression node.
 * Returns null if not compilable.
 */
function _emitExpr(node) {
  if (!node) return 'true';
  
  switch (node.type) {
    case 'literal':
    case 'number':
    case 'string': {
      const val = node.value;
      if (val === null || val === undefined) return 'null';
      if (typeof val === 'string') return JSON.stringify(val);
      if (typeof val === 'number') return String(val);
      if (typeof val === 'boolean') return String(val);
      return null;
    }
    
    case 'column_ref': {
      const name = node.name;
      // Don't compile qualified column refs (table.col) — they may reference outer scope
      // in correlated subqueries
      if (name.includes('.')) return null;
      return `row[${JSON.stringify(name)}]`;
    }
    
    case 'COMPARE': {
      const left = _emitExpr(node.left);
      const right = _emitExpr(node.right);
      if (left === null || right === null) return null;
      
      switch (node.op) {
        case 'EQ': return `(${left} === ${right})`;
        case 'NE': case '<>': case '!=': return `(${left} !== ${right})`;
        case 'LT': return `(${left} < ${right})`;
        case 'GT': return `(${left} > ${right})`;
        case 'LE': case 'LTE': return `(${left} <= ${right})`;
        case 'GE': case 'GTE': return `(${left} >= ${right})`;
        default: return null;
      }
    }
    
    case 'AND': case 'LOGICAL_AND': {
      const left = _emitExpr(node.left);
      const right = _emitExpr(node.right);
      if (left === null || right === null) return null;
      return `(${left} && ${right})`;
    }
    
    case 'OR': case 'LOGICAL_OR': {
      const left = _emitExpr(node.left);
      const right = _emitExpr(node.right);
      if (left === null || right === null) return null;
      return `(${left} || ${right})`;
    }
    
    case 'NOT': case 'LOGICAL_NOT': {
      const operand = _emitExpr(node.operand || node.expr || node.right);
      if (operand === null) return null;
      return `(!${operand})`;
    }
    
    case 'IS_NULL': {
      const operand = _emitExpr(node.operand || node.expr || node.left);
      if (operand === null) return null;
      return `(${operand} == null)`;
    }
    
    case 'IS_NOT_NULL': {
      const operand = _emitExpr(node.operand || node.expr || node.left);
      if (operand === null) return null;
      return `(${operand} != null)`;
    }
    
    case 'BETWEEN': {
      const val = _emitExpr(node.expr || node.left);
      const low = _emitExpr(node.low || node.start);
      const high = _emitExpr(node.high || node.end);
      if (val === null || low === null || high === null) return null;
      return `(${val} >= ${low} && ${val} <= ${high})`;
    }
    
    case 'IN': {
      const val = _emitExpr(node.expr || node.left);
      if (val === null) return null;
      if (!node.values || !Array.isArray(node.values)) return null;
      const vals = node.values.map(v => _emitExpr(v));
      if (vals.some(v => v === null)) return null;
      return `([${vals.join(',')}].includes(${val}))`;
    }
    
    case 'binary': case 'BINARY': {
      const left = _emitExpr(node.left);
      const right = _emitExpr(node.right);
      if (left === null || right === null) return null;
      const op = node.op || node.operator;
      switch (op) {
        case '+': return `(${left} + ${right})`;
        case '-': return `(${left} - ${right})`;
        case '*': return `(${left} * ${right})`;
        case '/': return `(${left} / ${right})`;
        case '%': return `(${left} % ${right})`;
        default: return null;
      }
    }
    
    case 'unary': case 'UNARY': {
      const operand = _emitExpr(node.operand || node.expr || node.right);
      if (operand === null) return null;
      const op = node.op || node.operator;
      if (op === '-') return `(-${operand})`;
      if (op === '+') return `(+${operand})`;
      return null;
    }
    
    case 'PARAM': {
      // Parameter placeholder — can't compile statically
      return null;
    }
    
    default:
      return null; // Not compilable
  }
}

/**
 * Get or create a compiled expression function for a WHERE clause.
 * Does NOT cache (AST may be mutated by prepared statement fast-bind).
 * Compilation is fast enough (~1μs) that caching is unnecessary.
 */
export function getCompiledExpr(expr) {
  if (!expr) return () => true;
  return compileExpr(expr);
}
