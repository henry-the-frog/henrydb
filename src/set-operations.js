// set-operations.js — UNION, INTERSECT, EXCEPT operations
// Extracted from db.js. Mixin pattern: installSetOperations(Database) adds methods to prototype.

/**
 * Install UNION, INTERSECT, and EXCEPT methods on Database.
 * @param {Function} DatabaseClass — the Database constructor
 */
export function installSetOperations(DatabaseClass) {

  DatabaseClass.prototype._union = function _union(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    let rows = [...leftResult.rows, ...rightResult.rows];

    if (!ast.all) {
      // UNION (not ALL) — remove duplicates
      const seen = new Set();
      rows = rows.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    // ORDER BY on the combined result
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'number') {
            av = this._resolveOrderByValue(column, a, ast);
            bv = this._resolveOrderByValue(column, b, ast);
          } else if (typeof column === 'object') {
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else {
            av = this._resolveColumn(column, a);
            bv = this._resolveColumn(column, b);
          }
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // LIMIT/OFFSET
    if (ast.limit != null || ast.offset != null) {
      const offset = ast.offset || 0;
      const limit = ast.limit != null ? ast.limit : rows.length;
      rows = rows.slice(offset, offset + limit);
    }

    return { type: 'ROWS', rows };
  };

  DatabaseClass.prototype._intersect = function _intersect(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    
    if (ast.all) {
      // INTERSECT ALL: multiset intersection (count-based)
      const rightCounts = new Map();
      for (const row of rightResult.rows) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          rows.push(row);
          rightCounts.set(key, count - 1);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    // INTERSECT (set): deduplicate
    const rightKeys = new Set(rightResult.rows.map(r => JSON.stringify(r)));
    const seen = new Set();
    const rows = leftResult.rows.filter(row => {
      const key = JSON.stringify(row);
      if (rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        return true;
      }
      return false;
    });
    
    return { type: 'ROWS', rows };
  };

  DatabaseClass.prototype._except = function _except(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    
    if (ast.all) {
      // EXCEPT ALL: multiset difference (count-based)
      const rightCounts = new Map();
      for (const row of rightResult.rows) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          rightCounts.set(key, count - 1);
        } else {
          rows.push(row);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    // EXCEPT (set): deduplicate
    const rightKeys = new Set(rightResult.rows.map(r => JSON.stringify(r)));
    const seen = new Set();
    const rows = leftResult.rows.filter(row => {
      const key = JSON.stringify(row);
      if (!rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        return true;
      }
      return false;
    });
    
    return { type: 'ROWS', rows };
  };
}
