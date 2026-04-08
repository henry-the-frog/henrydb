// constant-folding.js — Compile-time constant folding for SQL expressions
// Simplifies expressions before execution:
// - Arithmetic: 2 + 3 → 5, x * 1 → x, x + 0 → x
// - Comparison: 1 = 1 → true, 1 > 2 → false
// - Boolean: true AND x → x, false OR x → x, NOT true → false
// - String: 'hello' || ' ' || 'world' → 'hello world'

/**
 * ConstantFolder — AST-level constant folding.
 */
export class ConstantFolder {
  constructor() {
    this.stats = { folds: 0, eliminations: 0 };
  }

  /**
   * Fold constants in an AST expression.
   * Returns a simplified expression or a literal.
   */
  fold(expr) {
    if (!expr) return expr;

    switch (expr.type) {
      case 'literal':
      case 'number':
      case 'string':
        return expr;

      case 'column_ref':
        return expr;

      case 'COMPARE':
        return this._foldCompare(expr);

      case 'AND':
        return this._foldAnd(expr);

      case 'OR':
        return this._foldOr(expr);

      case 'NOT':
        return this._foldNot(expr);

      case 'ARITHMETIC':
      case 'binary_expr':
        return this._foldArithmetic(expr);

      case 'unary':
        return this._foldUnary(expr);

      case 'BETWEEN':
        return this._foldBetween(expr);

      case 'IN':
        return this._foldIn(expr);

      case 'CASE':
        return this._foldCase(expr);

      default:
        return expr;
    }
  }

  _foldCompare(expr) {
    const left = this.fold(expr.left);
    const right = this.fold(expr.right);

    if (this._isLiteral(left) && this._isLiteral(right)) {
      const lv = this._getValue(left);
      const rv = this._getValue(right);
      let result;
      switch (expr.op) {
        case 'EQ': result = lv === rv; break;
        case 'NE': result = lv !== rv; break;
        case 'LT': result = lv < rv; break;
        case 'GT': result = lv > rv; break;
        case 'LE': result = lv <= rv; break;
        case 'GE': result = lv >= rv; break;
        default: return { ...expr, left, right };
      }
      this.stats.folds++;
      return { type: 'literal', value: result };
    }

    return { ...expr, left, right };
  }

  _foldAnd(expr) {
    const left = this.fold(expr.left);
    const right = this.fold(expr.right);

    // true AND x → x
    if (this._isLiteral(left) && this._getValue(left) === true) {
      this.stats.eliminations++;
      return right;
    }
    // x AND true → x
    if (this._isLiteral(right) && this._getValue(right) === true) {
      this.stats.eliminations++;
      return left;
    }
    // false AND x → false
    if (this._isLiteral(left) && this._getValue(left) === false) {
      this.stats.eliminations++;
      return { type: 'literal', value: false };
    }
    // x AND false → false
    if (this._isLiteral(right) && this._getValue(right) === false) {
      this.stats.eliminations++;
      return { type: 'literal', value: false };
    }

    return { ...expr, left, right };
  }

  _foldOr(expr) {
    const left = this.fold(expr.left);
    const right = this.fold(expr.right);

    // true OR x → true
    if (this._isLiteral(left) && this._getValue(left) === true) {
      this.stats.eliminations++;
      return { type: 'literal', value: true };
    }
    // false OR x → x
    if (this._isLiteral(left) && this._getValue(left) === false) {
      this.stats.eliminations++;
      return right;
    }
    // x OR true → true
    if (this._isLiteral(right) && this._getValue(right) === true) {
      this.stats.eliminations++;
      return { type: 'literal', value: true };
    }
    // x OR false → x
    if (this._isLiteral(right) && this._getValue(right) === false) {
      this.stats.eliminations++;
      return left;
    }

    return { ...expr, left, right };
  }

  _foldNot(expr) {
    const inner = this.fold(expr.expr || expr.operand);

    if (this._isLiteral(inner)) {
      this.stats.folds++;
      return { type: 'literal', value: !this._getValue(inner) };
    }

    // NOT NOT x → x
    if (inner.type === 'NOT') {
      this.stats.eliminations++;
      return inner.expr || inner.operand;
    }

    return { ...expr, expr: inner };
  }

  _foldArithmetic(expr) {
    const left = this.fold(expr.left);
    const right = this.fold(expr.right);
    const op = expr.op || expr.operator;

    if (this._isLiteral(left) && this._isLiteral(right)) {
      const lv = this._getValue(left);
      const rv = this._getValue(right);
      let result;
      switch (op) {
        case '+': result = lv + rv; break;
        case '-': result = lv - rv; break;
        case '*': result = lv * rv; break;
        case '/': result = rv !== 0 ? lv / rv : null; break;
        case '%': result = rv !== 0 ? lv % rv : null; break;
        case '||': result = String(lv) + String(rv); break; // Concat
        default: return { ...expr, left, right };
      }
      this.stats.folds++;
      return { type: 'literal', value: result };
    }

    // Identity optimizations
    if (this._isLiteral(right)) {
      const rv = this._getValue(right);
      if (op === '+' && rv === 0) { this.stats.eliminations++; return left; } // x + 0 → x
      if (op === '-' && rv === 0) { this.stats.eliminations++; return left; } // x - 0 → x
      if (op === '*' && rv === 1) { this.stats.eliminations++; return left; } // x * 1 → x
      if (op === '*' && rv === 0) { this.stats.eliminations++; return { type: 'literal', value: 0 }; } // x * 0 → 0
      if (op === '/' && rv === 1) { this.stats.eliminations++; return left; } // x / 1 → x
    }

    if (this._isLiteral(left)) {
      const lv = this._getValue(left);
      if (op === '+' && lv === 0) { this.stats.eliminations++; return right; } // 0 + x → x
      if (op === '*' && lv === 1) { this.stats.eliminations++; return right; } // 1 * x → x
      if (op === '*' && lv === 0) { this.stats.eliminations++; return { type: 'literal', value: 0 }; } // 0 * x → 0
    }

    return { ...expr, left, right };
  }

  _foldUnary(expr) {
    const operand = this.fold(expr.operand || expr.expr);
    if (this._isLiteral(operand)) {
      const v = this._getValue(operand);
      if (expr.op === '-') { this.stats.folds++; return { type: 'literal', value: -v }; }
      if (expr.op === '+') { this.stats.folds++; return operand; }
    }
    return { ...expr, operand };
  }

  _foldBetween(expr) {
    const val = this.fold(expr.value || expr.expr);
    const lo = this.fold(expr.low || expr.left);
    const hi = this.fold(expr.high || expr.right);

    if (this._isLiteral(val) && this._isLiteral(lo) && this._isLiteral(hi)) {
      const v = this._getValue(val);
      const l = this._getValue(lo);
      const h = this._getValue(hi);
      this.stats.folds++;
      return { type: 'literal', value: v >= l && v <= h };
    }

    return { ...expr, value: val, low: lo, high: hi };
  }

  _foldIn(expr) {
    const val = this.fold(expr.value || expr.expr);
    if (this._isLiteral(val) && expr.list?.every(e => this._isLiteral(e))) {
      const v = this._getValue(val);
      const inList = expr.list.map(e => this._getValue(e));
      this.stats.folds++;
      return { type: 'literal', value: inList.includes(v) };
    }
    return expr;
  }

  _foldCase(expr) {
    // CASE WHEN ... THEN ... ELSE ... END
    if (expr.conditions) {
      for (const cond of expr.conditions) {
        const when = this.fold(cond.when);
        if (this._isLiteral(when)) {
          if (this._getValue(when)) {
            this.stats.eliminations++;
            return this.fold(cond.then);
          }
          // false: skip this branch
          continue;
        }
        return expr; // Non-constant condition, can't fold further
      }
      // All conditions were false
      if (expr.else) {
        this.stats.eliminations++;
        return this.fold(expr.else);
      }
    }
    return expr;
  }

  _isLiteral(expr) {
    return expr && (expr.type === 'literal' || expr.type === 'number' || expr.type === 'string') && expr.value !== undefined;
  }

  _getValue(expr) {
    return expr.value;
  }

  getStats() { return { ...this.stats }; }
}
