// query-rewriter.js — Query rewriter for predicate simplification and join ordering
// Rewrites query ASTs before execution for better performance:
// 1. Predicate pushdown hints
// 2. Constant propagation
// 3. Join order suggestions based on estimated sizes
// 4. Subquery decorrelation hints

export class QueryRewriter {
  constructor() {
    this.stats = { rewrites: 0 };
  }

  /**
   * Rewrite a WHERE clause: simplify predicates.
   */
  simplifyPredicate(pred) {
    if (!pred) return pred;
    return this._simplify(pred);
  }

  _simplify(expr) {
    if (!expr) return expr;

    // Recursively simplify children first
    if (expr.type === 'AND') {
      const left = this._simplify(expr.left);
      const right = this._simplify(expr.right);
      // Remove tautologies
      if (this._isTrueLiteral(left)) { this.stats.rewrites++; return right; }
      if (this._isTrueLiteral(right)) { this.stats.rewrites++; return left; }
      if (this._isFalseLiteral(left) || this._isFalseLiteral(right)) { this.stats.rewrites++; return { type: 'literal', value: false }; }
      // Combine range predicates: x > 5 AND x > 3 → x > 5
      const merged = this._mergeRangePredicates(left, right);
      if (merged) { this.stats.rewrites++; return merged; }
      return { ...expr, left, right };
    }

    if (expr.type === 'OR') {
      const left = this._simplify(expr.left);
      const right = this._simplify(expr.right);
      if (this._isTrueLiteral(left) || this._isTrueLiteral(right)) { this.stats.rewrites++; return { type: 'literal', value: true }; }
      if (this._isFalseLiteral(left)) { this.stats.rewrites++; return right; }
      if (this._isFalseLiteral(right)) { this.stats.rewrites++; return left; }
      // Convert OR of equalities to IN: x=1 OR x=2 → x IN (1,2)
      const inExpr = this._orToIn(left, right);
      if (inExpr) { this.stats.rewrites++; return inExpr; }
      return { ...expr, left, right };
    }

    if (expr.type === 'NOT') {
      const inner = this._simplify(expr.expr);
      if (this._isTrueLiteral(inner)) { this.stats.rewrites++; return { type: 'literal', value: false }; }
      if (this._isFalseLiteral(inner)) { this.stats.rewrites++; return { type: 'literal', value: true }; }
      if (inner.type === 'NOT') { this.stats.rewrites++; return inner.expr; } // NOT NOT x → x
      return { ...expr, expr: inner };
    }

    // Self-comparison: x = x → true (for non-nullable)
    if (expr.type === 'COMPARE' && expr.op === 'EQ' && 
        expr.left?.type === 'column' && expr.right?.type === 'column' &&
        expr.left.name === expr.right.name) {
      this.stats.rewrites++;
      return { type: 'literal', value: true };
    }

    return expr;
  }

  /**
   * Suggest join order based on table sizes.
   * Smaller tables should be on the build side (inner) of hash joins.
   */
  suggestJoinOrder(tables) {
    // Sort by estimated size ascending — build hash table on smallest
    return [...tables].sort((a, b) => (a.estimatedRows || 0) - (b.estimatedRows || 0));
  }

  /**
   * Push predicates down to individual tables.
   * Returns { pushed: [{table, predicate}], remaining: predicate }
   */
  pushdownPredicates(predicate, tables) {
    const pushed = [];
    const remaining = [];

    const preds = this._flattenAnd(predicate);
    for (const pred of preds) {
      const cols = this._extractColumns(pred);
      // Can push if all columns belong to one table
      const table = this._findSingleTable(cols, tables);
      if (table) {
        pushed.push({ table, predicate: pred });
      } else {
        remaining.push(pred);
      }
    }

    return {
      pushed,
      remaining: remaining.length > 0 ? this._buildAnd(remaining) : null,
    };
  }

  _mergeRangePredicates(left, right) {
    if (left.type === 'COMPARE' && right.type === 'COMPARE' &&
        left.left?.type === 'column' && right.left?.type === 'column' &&
        left.left.name === right.left.name) {
      // x > 5 AND x > 3 → x > 5
      if (left.op === 'GT' && right.op === 'GT') {
        return left.right.value >= right.right.value ? left : right;
      }
      // x < 5 AND x < 3 → x < 3
      if (left.op === 'LT' && right.op === 'LT') {
        return left.right.value <= right.right.value ? left : right;
      }
    }
    return null;
  }

  _orToIn(left, right) {
    if (left.type === 'COMPARE' && left.op === 'EQ' &&
        right.type === 'COMPARE' && right.op === 'EQ' &&
        left.left?.type === 'column' && right.left?.type === 'column' &&
        left.left.name === right.left.name &&
        left.right?.type === 'literal' && right.right?.type === 'literal') {
      return {
        type: 'IN',
        column: left.left,
        values: [left.right, right.right],
      };
    }
    return null;
  }

  _isTrueLiteral(e) { return e?.type === 'literal' && e.value === true; }
  _isFalseLiteral(e) { return e?.type === 'literal' && e.value === false; }

  _flattenAnd(expr) {
    if (!expr) return [];
    if (expr.type === 'AND') return [...this._flattenAnd(expr.left), ...this._flattenAnd(expr.right)];
    return [expr];
  }

  _buildAnd(preds) {
    if (preds.length === 0) return null;
    if (preds.length === 1) return preds[0];
    return preds.reduce((a, b) => ({ type: 'AND', left: a, right: b }));
  }

  _extractColumns(expr) {
    const cols = new Set();
    const walk = (e) => {
      if (!e) return;
      if (e.type === 'column') cols.add(e.name);
      for (const key of ['left', 'right', 'expr', 'value', 'column']) {
        if (e[key]) walk(e[key]);
      }
    };
    walk(expr);
    return cols;
  }

  _findSingleTable(cols, tables) {
    for (const table of tables) {
      if ([...cols].every(c => table.columns?.includes(c))) return table.name;
    }
    return null;
  }

  getStats() { return { ...this.stats }; }
}
