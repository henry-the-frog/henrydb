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
    const code = _emitExpr(expr);
    if (code === null) return null;
    // Use Function constructor for fast execution
    return new Function('row', `return ${code};`);
  } catch {
    return null; // Fallback to interpreter
  }
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
      // Handle qualified names: table.column
      const col = name.includes('.') ? name.split('.').pop() : name;
      // Use bracket notation for safety
      return `row[${JSON.stringify(col)}]`;
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
 * Expression compiler cache: AST hash → compiled function
 */
const _exprCache = new WeakMap();

/**
 * Get or create a compiled expression function for a WHERE clause.
 * Uses WeakMap keyed on AST node for caching.
 */
export function getCompiledExpr(expr) {
  if (!expr) return () => true;
  
  let fn = _exprCache.get(expr);
  if (fn !== undefined) return fn; // null means "not compilable"
  
  fn = compileExpr(expr);
  _exprCache.set(expr, fn);
  return fn;
}
