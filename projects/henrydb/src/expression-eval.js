// expression-eval.js — Runtime evaluation of SQL expressions
// Handles CASE, COALESCE, CAST, NULLIF, arithmetic, comparisons.

export class ExpressionEvaluator {
  evaluate(expr, row = {}) {
    if (expr == null) return null;
    
    switch (expr.type) {
      case 'literal': return expr.value;
      case 'column': return row[expr.name];
      
      case 'ARITHMETIC': return this._arithmetic(expr, row);
      case 'COMPARE': return this._compare(expr, row);
      case 'AND': return this.evaluate(expr.left, row) && this.evaluate(expr.right, row);
      case 'OR': return this.evaluate(expr.left, row) || this.evaluate(expr.right, row);
      case 'NOT': return !this.evaluate(expr.expr, row);
      
      case 'CASE': return this._case(expr, row);
      case 'COALESCE': return this._coalesce(expr, row);
      case 'NULLIF': return this._nullif(expr, row);
      case 'CAST': return this._cast(expr, row);
      case 'BETWEEN': {
        const val = this.evaluate(expr.expr, row);
        return (val >= this.evaluate(expr.low, row) && val <= this.evaluate(expr.high, row)) ? 1 : 0;
      }
      case 'IN': {
        const val = this.evaluate(expr.expr, row);
        return expr.values.some(v => this.evaluate(v, row) === val);
      }
      case 'IS_NULL': return this.evaluate(expr.expr, row) == null;
      case 'LIKE': return this._like(expr, row);
      case 'UNARY_MINUS': return -this.evaluate(expr.expr, row);
      case 'FUNCTION': return this._function(expr, row);
      
      default: throw new Error(`Unknown expression type: ${expr.type}`);
    }
  }

  _arithmetic(expr, row) {
    const left = this.evaluate(expr.left, row);
    const right = this.evaluate(expr.right, row);
    if (left == null || right == null) return null;
    if (expr.op === '||') return String(left) + String(right);  // SQL concat
    // SQL arithmetic: coerce string operands to numbers (SQLite compat)
    const l = typeof left === 'string' ? (isNaN(Number(left)) ? 0 : Number(left)) : left;
    const r = typeof right === 'string' ? (isNaN(Number(right)) ? 0 : Number(right)) : right;
    switch (expr.op) {
      case '+': return l + r;
      case '-': return l - r;
      case '*': return l * r;
      case '/': return r !== 0 ? l / r : null;
      case '%': return r !== 0 ? l % r : null;
      default: return null;
    }
  }

  _compare(expr, row) {
    const l = this.evaluate(expr.left, row);
    const r = this.evaluate(expr.right, row);
    if (l == null || r == null) return null;
    switch (expr.op) {
      case '=': case 'EQ': return l === r;
      case '!=': case '<>': case 'NE': return l !== r;
      case '<': case 'LT': return l < r;
      case '>': case 'GT': return l > r;
      case '<=': case 'LE': return l <= r;
      case '>=': case 'GE': return l >= r;
      default: return null;
    }
  }

  _case(expr, row) {
    for (const { when, then } of (expr.cases || [])) {
      if (this.evaluate(when, row)) return this.evaluate(then, row);
    }
    return expr.else ? this.evaluate(expr.else, row) : null;
  }

  _coalesce(expr, row) {
    for (const arg of expr.args) {
      const val = this.evaluate(arg, row);
      if (val != null) return val;
    }
    return null;
  }

  _nullif(expr, row) {
    const a = this.evaluate(expr.left, row);
    const b = this.evaluate(expr.right, row);
    return a === b ? null : a;
  }

  _cast(expr, row) {
    const val = this.evaluate(expr.expr, row);
    if (val == null) return null;
    switch (expr.targetType?.toUpperCase()) {
      case 'INT': case 'INTEGER': return Math.round(Number(val));
      case 'FLOAT': case 'DOUBLE': return Number(val);
      case 'VARCHAR': case 'TEXT': return String(val);
      case 'BOOLEAN': return Boolean(val);
      default: return val;
    }
  }

  _like(expr, row) {
    const val = this.evaluate(expr.expr, row);
    if (val == null) return null;
    const pattern = expr.pattern.replace(/%/g, '.*').replace(/_/g, '.');
    return new RegExp(`^${pattern}$`, 'i').test(String(val));
  }

  _function(expr, row) {
    const args = expr.args.map(a => this.evaluate(a, row));
    switch (expr.name?.toUpperCase()) {
      case 'ABS': return Math.abs(args[0]);
      case 'UPPER': return String(args[0]).toUpperCase();
      case 'LOWER': return String(args[0]).toLowerCase();
      case 'LENGTH': return String(args[0]).length;
      case 'CONCAT': return args.join('');
      case 'SUBSTR': case 'SUBSTRING': return String(args[0]).substring(args[1] - 1, args[1] - 1 + (args[2] || Infinity));
      case 'ROUND': return Math.round(args[0] * (10 ** (args[1] || 0))) / (10 ** (args[1] || 0));
      case 'CEIL': case 'CEILING': return Math.ceil(args[0]);
      case 'FLOOR': return Math.floor(args[0]);
      case 'TRIM': return String(args[0]).trim();
      default: throw new Error(`Unknown function: ${expr.name}`);
    }
  }
}
