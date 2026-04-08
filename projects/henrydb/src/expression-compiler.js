// expression-compiler.js — JIT compile WHERE clauses to native JS functions
// Converts AST expressions into `new Function()` for near-native filter speed.
// Eliminates interpretation overhead in hot loops (row-by-row evaluation).

/**
 * ExpressionCompiler — compile WHERE expressions to native JS functions.
 */
export class ExpressionCompiler {
  constructor() {
    this._cache = new Map(); // expression key → compiled function
    this.stats = { compilations: 0, cacheHits: 0 };
  }

  /**
   * Compile an expression AST into a function: (row) => boolean
   * 
   * @param {Object} expr - Expression AST
   * @returns {{ fn: Function, code: string }} Compiled function and generated code
   */
  compile(expr) {
    const key = JSON.stringify(expr);
    if (this._cache.has(key)) {
      this.stats.cacheHits++;
      return this._cache.get(key);
    }

    const code = this._emit(expr);
    const fn = new Function('row', `return ${code};`);
    const result = { fn, code };
    this._cache.set(key, result);
    this.stats.compilations++;
    return result;
  }

  /**
   * Compile and immediately evaluate against rows.
   */
  filter(rows, expr) {
    const { fn } = this.compile(expr);
    return rows.filter(fn);
  }

  /**
   * Emit JS code for an expression.
   */
  _emit(expr) {
    if (!expr) return 'true';

    switch (expr.type) {
      case 'literal':
        return JSON.stringify(expr.value);

      case 'column':
        return `row[${JSON.stringify(expr.name)}]`;

      case 'COMPARE': {
        const left = this._emit(expr.left);
        const right = this._emit(expr.right);
        const op = { EQ: '===', NE: '!==', LT: '<', GT: '>', LE: '<=', GE: '>=' }[expr.op];
        return `(${left} ${op} ${right})`;
      }

      case 'AND':
        return `(${this._emit(expr.left)} && ${this._emit(expr.right)})`;

      case 'OR':
        return `(${this._emit(expr.left)} || ${this._emit(expr.right)})`;

      case 'NOT':
        return `(!(${this._emit(expr.expr)}))`;

      case 'BETWEEN': {
        const val = this._emit(expr.value);
        const lo = this._emit(expr.low);
        const hi = this._emit(expr.high);
        return `(${val} >= ${lo} && ${val} <= ${hi})`;
      }

      case 'IN': {
        const val = this._emit(expr.value);
        const set = expr.list.map(e => this._emit(e)).join(', ');
        return `([${set}].includes(${val}))`;
      }

      case 'LIKE': {
        const col = this._emit(expr.column);
        const pattern = expr.pattern
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.');
        return `(/^${pattern}$/.test(${col}))`;
      }

      case 'IS_NULL':
        return `(${this._emit(expr.expr)} == null)`;

      case 'IS_NOT_NULL':
        return `(${this._emit(expr.expr)} != null)`;

      case 'ARITHMETIC': {
        const left = this._emit(expr.left);
        const right = this._emit(expr.right);
        return `(${left} ${expr.op} ${right})`;
      }

      case 'FUNC': {
        const args = expr.args.map(a => this._emit(a)).join(', ');
        switch (expr.name.toUpperCase()) {
          case 'ABS': return `Math.abs(${args})`;
          case 'UPPER': return `String(${args}).toUpperCase()`;
          case 'LOWER': return `String(${args}).toLowerCase()`;
          case 'LENGTH': return `String(${args}).length`;
          case 'COALESCE': return `(${expr.args.map(a => this._emit(a)).join(' ?? ')})`;
          default: return `null`;
        }
      }

      default:
        return 'true';
    }
  }

  getStats() {
    return { ...this.stats, cacheSize: this._cache.size };
  }
}
