// query-rewriter.js — AST-to-AST query rewriting engine
// Performs logical transformations on the query AST before execution:
// 1. View expansion — inline view definitions
// 2. Subquery flattening — convert correlated subqueries to joins where possible
// 3. Predicate pushdown — move WHERE conditions closer to their source tables
// 4. Constant folding — evaluate constant expressions at compile time
// 5. Redundant predicate elimination — remove tautologies (1=1, x=x)

/**
 * QueryRewriter — transforms query ASTs for optimization.
 */
export class QueryRewriter {
  constructor(options = {}) {
    this.views = options.views || new Map(); // view name → view definition AST
    this.rules = [
      this._expandViews.bind(this),
      this._pushdownPredicates.bind(this),
      this._flattenSubqueries.bind(this),
      this._foldConstants.bind(this),
      this._eliminateRedundantPredicates.bind(this),
    ];
    this._stats = {
      viewExpansions: 0,
      predicatePushdowns: 0,
      subqueryFlattenings: 0,
      constantFolds: 0,
      redundantEliminations: 0,
    };
  }

  /**
   * Rewrite a query AST by applying all rules.
   * Returns the transformed AST (new object, original unchanged).
   */
  rewrite(ast) {
    let current = this._deepClone(ast);
    for (const rule of this.rules) {
      current = rule(current);
    }
    return current;
  }

  getStats() {
    return { ...this._stats };
  }

  // --- Rule 1: View Expansion ---
  _expandViews(ast) {
    if (ast.type !== 'SELECT') return ast;

    // Check FROM clause
    if (ast.from && ast.from.table && this.views.has(ast.from.table)) {
      const viewDef = this._deepClone(this.views.get(ast.from.table));
      const alias = ast.from.alias || ast.from.table;

      // Replace FROM with the view's subquery
      ast.from = { type: 'subquery', query: viewDef, alias };
      this._stats.viewExpansions++;
    }

    // Check JOIN tables
    if (ast.joins) {
      for (const join of ast.joins) {
        const joinTable = join.table?.table || join.table;
        if (typeof joinTable === 'string' && this.views.has(joinTable)) {
          const viewDef = this._deepClone(this.views.get(joinTable));
          const alias = join.table?.alias || joinTable;
          join.table = { type: 'subquery', query: viewDef, alias };
          this._stats.viewExpansions++;
        }
      }
    }

    return ast;
  }

  // --- Rule 2: Predicate Pushdown ---
  _pushdownPredicates(ast) {
    if (ast.type !== 'SELECT' || !ast.where || !ast.joins || ast.joins.length === 0) {
      return ast;
    }

    // Analyze WHERE conditions to determine which table they reference
    const conditions = this._splitConjunction(ast.where);
    const pushed = [];
    const remaining = [];

    for (const cond of conditions) {
      const tables = this._extractTableRefs(cond);

      // If condition references only one table, push it down
      if (tables.size === 1) {
        const tableName = [...tables][0];
        
        // Try to push to FROM table
        if (ast.from && (ast.from.table === tableName || ast.from.alias === tableName)) {
          // Add as a filter on the FROM clause
          if (!ast.from.filter) ast.from.filter = [];
          ast.from.filter.push(cond);
          pushed.push(cond);
          this._stats.predicatePushdowns++;
          continue;
        }

        // Try to push to a JOIN table
        let pushedToJoin = false;
        for (const join of ast.joins) {
          const joinTableName = join.table?.table || join.table?.alias || join.table;
          if (joinTableName === tableName || join.table?.alias === tableName) {
            if (!join.additionalConditions) join.additionalConditions = [];
            join.additionalConditions.push(cond);
            pushed.push(cond);
            this._stats.predicatePushdowns++;
            pushedToJoin = true;
            break;
          }
        }
        if (pushedToJoin) continue;
      }

      remaining.push(cond);
    }

    // Reconstruct WHERE from remaining conditions
    if (remaining.length === 0) {
      ast.where = null;
    } else if (remaining.length === 1) {
      ast.where = remaining[0];
    } else {
      ast.where = { type: 'AND', left: remaining[0], right: this._buildConjunction(remaining.slice(1)) };
    }

    return ast;
  }

  // --- Rule 3: Subquery Flattening ---
  _flattenSubqueries(ast) {
    if (ast.type !== 'SELECT' || !ast.where) return ast;

    // Look for IN (SELECT ...) patterns and convert to JOIN
    ast.where = this._flattenInSubquery(ast, ast.where);
    return ast;
  }

  _flattenInSubquery(ast, where) {
    if (!where || typeof where !== 'object') return where;

    // Pattern: column IN (SELECT col FROM table WHERE ...)
    if (where.type === 'IN' && where.right?.type === 'SELECT') {
      const subquery = where.right;
      // Can flatten if subquery is a simple single-table SELECT with one column
      if (subquery.from && !subquery.joins && subquery.columns?.length === 1) {
        const subTable = subquery.from.table;
        const subCol = subquery.columns[0].name || subquery.columns[0];
        const alias = `_sq_${subTable}`;

        // Convert to a semi-join
        const joinCondition = {
          type: 'EQUALS',
          left: where.left,
          right: { type: 'column_ref', table: alias, column: subCol },
        };

        if (!ast.joins) ast.joins = [];
        ast.joins.push({
          type: 'INNER',
          table: { table: subTable, alias },
          condition: subquery.where
            ? { type: 'AND', left: joinCondition, right: subquery.where }
            : joinCondition,
        });

        // Add DISTINCT to prevent duplicate rows from the join
        ast.distinct = true;

        this._stats.subqueryFlattenings++;
        return null; // Remove the IN condition from WHERE
      }
    }

    // Recurse into AND/OR
    if (where.type === 'AND') {
      where.left = this._flattenInSubquery(ast, where.left);
      where.right = this._flattenInSubquery(ast, where.right);
      if (!where.left) return where.right;
      if (!where.right) return where.left;
    }

    return where;
  }

  // --- Rule 4: Constant Folding ---
  _foldConstants(ast) {
    if (ast.type !== 'SELECT') return ast;

    // Fold constants in WHERE clause
    if (ast.where) {
      ast.where = this._foldExpr(ast.where);
    }

    // Fold in columns
    if (ast.columns) {
      ast.columns = ast.columns.map(col => {
        if (col.expression) {
          return { ...col, expression: this._foldExpr(col.expression) };
        }
        return col;
      });
    }

    return ast;
  }

  _foldExpr(expr) {
    if (!expr || typeof expr !== 'object') return expr;

    // Arithmetic on two constants
    if (expr.type === 'BINARY_OP' && expr.left?.type === 'literal' && expr.right?.type === 'literal') {
      const l = expr.left.value;
      const r = expr.right.value;
      if (typeof l === 'number' && typeof r === 'number') {
        let result;
        switch (expr.op) {
          case '+': result = l + r; break;
          case '-': result = l - r; break;
          case '*': result = l * r; break;
          case '/': result = r !== 0 ? l / r : undefined; break;
          case '%': result = r !== 0 ? l % r : undefined; break;
        }
        if (result !== undefined) {
          this._stats.constantFolds++;
          return { type: 'literal', value: result };
        }
      }
    }

    // String concatenation of two constants
    if (expr.type === 'CONCAT' && expr.left?.type === 'literal' && expr.right?.type === 'literal') {
      this._stats.constantFolds++;
      return { type: 'literal', value: String(expr.left.value) + String(expr.right.value) };
    }

    // Recurse
    if (expr.left) expr.left = this._foldExpr(expr.left);
    if (expr.right) expr.right = this._foldExpr(expr.right);
    if (expr.conditions) {
      expr.conditions = expr.conditions.map(c => this._foldExpr(c));
    }

    return expr;
  }

  // --- Rule 5: Redundant Predicate Elimination ---
  _eliminateRedundantPredicates(ast) {
    if (ast.type !== 'SELECT' || !ast.where) return ast;
    ast.where = this._eliminateRedundant(ast.where);
    return ast;
  }

  _eliminateRedundant(expr) {
    if (!expr || typeof expr !== 'object') return expr;

    // 1 = 1, TRUE = TRUE, x = x etc.
    if ((expr.type === 'EQUALS' || expr.type === 'comparison') && expr.left && expr.right) {
      const eq = this._isEqual(expr.left, expr.right);
      if (eq) {
        this._stats.redundantEliminations++;
        return { type: 'literal', value: true };
      }
    }

    // AND simplification
    if (expr.type === 'AND') {
      expr.left = this._eliminateRedundant(expr.left);
      expr.right = this._eliminateRedundant(expr.right);
      if (expr.left?.type === 'literal' && expr.left.value === true) return expr.right;
      if (expr.right?.type === 'literal' && expr.right.value === true) return expr.left;
      if (expr.left?.type === 'literal' && expr.left.value === false) return expr.left;
      if (expr.right?.type === 'literal' && expr.right.value === false) return expr.right;
    }

    // OR simplification
    if (expr.type === 'OR') {
      expr.left = this._eliminateRedundant(expr.left);
      expr.right = this._eliminateRedundant(expr.right);
      if (expr.left?.type === 'literal' && expr.left.value === true) return expr.left;
      if (expr.right?.type === 'literal' && expr.right.value === true) return expr.right;
      if (expr.left?.type === 'literal' && expr.left.value === false) return expr.right;
      if (expr.right?.type === 'literal' && expr.right.value === false) return expr.left;
    }

    return expr;
  }

  // --- Utility Methods ---

  _splitConjunction(where) {
    if (!where) return [];
    if (where.type === 'AND') {
      return [...this._splitConjunction(where.left), ...this._splitConjunction(where.right)];
    }
    return [where];
  }

  _buildConjunction(conditions) {
    if (conditions.length === 1) return conditions[0];
    return { type: 'AND', left: conditions[0], right: this._buildConjunction(conditions.slice(1)) };
  }

  _extractTableRefs(expr) {
    const tables = new Set();
    this._walkExpr(expr, node => {
      if (node.type === 'column_ref' && node.table) {
        tables.add(node.table);
      }
    });
    return tables;
  }

  _walkExpr(expr, fn) {
    if (!expr || typeof expr !== 'object') return;
    fn(expr);
    if (expr.left) this._walkExpr(expr.left, fn);
    if (expr.right) this._walkExpr(expr.right, fn);
    if (expr.conditions) expr.conditions.forEach(c => this._walkExpr(c, fn));
    if (expr.args) expr.args.forEach(a => this._walkExpr(a, fn));
  }

  _isEqual(a, b) {
    if (!a || !b) return false;
    return JSON.stringify(a) === JSON.stringify(b);
  }

  /**
   * Simplify a predicate expression.
   * Returns null for null input, folds constants and eliminates redundancies.
   */
  simplifyPredicate(pred) {
    if (pred === null || pred === undefined) return null;
    // Use constant folding and redundant elimination
    let result = this._foldConstantsExpr(pred);
    result = this._eliminateRedundant(result);
    return result;
  }

  _foldConstantsExpr(expr) {
    if (!expr || typeof expr !== 'object') return expr;
    // Recursively fold
    if (expr.left) expr.left = this._foldConstantsExpr(expr.left);
    if (expr.right) expr.right = this._foldConstantsExpr(expr.right);
    // Fold literal comparisons
    if (expr.type === 'COMPARE' && expr.left?.type === 'literal' && expr.right?.type === 'literal') {
      const l = expr.left.value, r = expr.right.value;
      const ops = { '=': l === r, '!=': l !== r, '<': l < r, '>': l > r, '<=': l <= r, '>=': l >= r };
      if (expr.op in ops) return { type: 'literal', value: ops[expr.op] };
    }
    return expr;
  }

  // (simplified version merged into main _eliminateRedundant above)

  _deepClone(obj) {
    return JSON.parse(JSON.stringify(obj));
  }
}
