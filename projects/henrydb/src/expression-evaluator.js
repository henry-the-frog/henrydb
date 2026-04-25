// expression-evaluator.js — Expression evaluation engine for HenryDB
// Extracted from db.js to reduce monolith size (~1050 LOC)
//
// Installs expression evaluation methods on Database.prototype as a mixin.
// All methods use `this` referencing the Database instance.

import { exprContains, exprCollect } from './expr-walker.js';
import { evalFunction as _evalFunctionImpl, dateArith as _dateArithImpl, likeToRegex as _likeToRegexImpl } from './sql-functions.js';
import { tokenize } from './fulltext.js';
import { typeClass, sqliteCompare } from './type-affinity.js';

/**
 * Install expression evaluation methods on Database.prototype.
 * @param {Function} Database - The Database class
 */
export function installExpressionEvaluator(Database) {
  const P = Database.prototype;

P._collectAggregateExprs = function(expr) {
  return exprCollect(expr, n => n.type === 'aggregate_expr');
}

/**
 * Resolve an ORDER BY column value from a row.
 * Handles string column names, numeric references, and expression nodes.
 */
P._orderByValue = function(column, row, selectCols) {
  if (typeof column === 'number') {
    if (selectCols && selectCols[column - 1]) {
      const selCol = selectCols[column - 1];
      const colName = selCol.alias || selCol.name;
      return row[colName] !== undefined ? row[colName] : this._resolveColumn(colName, row);
    }
    // Fallback: use unqualified keys
    const keys = Object.keys(row).filter(k => !k.includes('.'));
    const key = keys[column - 1];
    return key !== undefined ? row[key] : undefined;
  }
  if (typeof column === 'object' && column !== null) {
    return this._evalValue(column, row);
  }
  // String column name — try direct lookup first, then _resolveColumn
  if (row[column] !== undefined) return row[column];
  return this._resolveColumn(column, row);
}

/**
 * Pre-compute SELECT alias expressions into rows so ORDER BY can access them.
 * This resolves the SQL standard issue where ORDER BY runs before SELECT projection,
 * but should be able to reference SELECT aliases (e.g., COALESCE(...) as product).
 */
P._preComputeOrderByAliases = function(ast, rows) {
  if (!ast.orderBy || !ast.columns) return;
  const orderByNames = new Set(
    ast.orderBy.map(o => typeof o.column === 'string' ? o.column : null).filter(Boolean)
  );
  if (orderByNames.size === 0) return;
  
  // Find SELECT columns that are expressions with aliases matching ORDER BY columns
  const aliasedCols = ast.columns.filter(c => {
    const alias = c.alias;
    if (!alias || !orderByNames.has(alias)) return false;
    // Only for computed expressions, not simple column refs
    return c.type !== 'column_ref' && c.type !== 'star' && c.type !== 'qualified_star';
  });
  
  if (aliasedCols.length === 0) return;
  
  for (const row of rows) {
    for (const col of aliasedCols) {
      try {
        const val = col.expr ? this._evalValue(col.expr, row) : this._evalValue(col, row);
        row[col.alias] = val;
      } catch (e) {
        // If evaluation fails, don't overwrite existing value
        if (row[col.alias] === undefined) row[col.alias] = null;
      }
    }
  }
}

/**
 * Project a row for SELECT *: handle column name collisions from joins.
 * When multiple qualified keys (a.id, b.id) share the same unqualified name,
 * use qualified names for those columns to prevent data loss.
 */
P._projectStarRow = function(row) {
  const clean = {};
  const qualifiedKeys = [];
  const unqualifiedCount = {};
  
  for (const key of Object.keys(row)) {
    if (key.startsWith('__')) continue;
    if (key.includes('.')) {
      qualifiedKeys.push(key);
      const unqual = key.split('.').pop();
      unqualifiedCount[unqual] = (unqualifiedCount[unqual] || 0) + 1;
    }
  }
  
  const hasCollisions = Object.values(unqualifiedCount).some(c => c > 1);
  
  if (hasCollisions && qualifiedKeys.length > 0) {
    const colliding = new Set(Object.entries(unqualifiedCount).filter(([, c]) => c > 1).map(([k]) => k));
    const seen = new Set();
    
    // For each colliding name, check if all qualified values are identical (equi-join/USING/NATURAL)
    // If so, emit once with unqualified name. Otherwise emit qualified names.
    const collidingValues = {};
    for (const key of qualifiedKeys) {
      const unqual = key.split('.').pop();
      if (colliding.has(unqual)) {
        if (!collidingValues[unqual]) collidingValues[unqual] = [];
        collidingValues[unqual].push(row[key]);
      }
    }
    
    const canMerge = {};
    for (const [name, vals] of Object.entries(collidingValues)) {
      // Merge if all non-null values are identical (equi-join result)
      const nonNull = vals.filter(v => v !== null && v !== undefined);
      canMerge[name] = nonNull.length <= 1 || nonNull.every(v => v === nonNull[0]);
    }
    
    for (const key of Object.keys(row)) {
      if (key.startsWith('__')) continue;
      if (key.includes('.')) {
        const unqual = key.split('.').pop();
        if (colliding.has(unqual)) {
          if (canMerge[unqual]) {
            // Merge: use unqualified name with the non-null value
            if (!seen.has(unqual)) {
              const nonNull = collidingValues[unqual].find(v => v !== null && v !== undefined);
              clean[unqual] = nonNull !== undefined ? nonNull : null;
              seen.add(unqual);
            }
          } else {
            // Can't merge: different values — use qualified names
            clean[key] = row[key];
          }
        } else if (!seen.has(unqual)) {
          clean[unqual] = row[key];
          seen.add(unqual);
        }
      } else {
        if (!colliding.has(key)) {
          clean[key] = row[key];
        }
      }
    }
  } else {
    for (const [key, val] of Object.entries(row)) {
      if (!key.includes('.') && !key.startsWith('__')) {
        clean[key] = val;
      }
    }
  }
  return clean;
}

/**
 * Evaluate an expression in GROUP BY context, resolving aggregate sub-expressions.
 */
P._evalGroupExpr = function(expr, groupRows, result, computeAgg) {
  if (!expr) return null;
  if (expr.type === 'literal') return expr.value;
  if (expr.type === 'column_ref') {
    // Try result first (already computed group-by / aggregate columns)
    if (result[expr.name] !== undefined) return result[expr.name];
    return this._resolveColumn(expr.name, groupRows[0]);
  }
  if (expr.type === 'aggregate_expr') {
    return computeAgg(expr.func, expr.arg, expr.distinct);
  }
  if (expr.type === 'case_expr') {
    for (const { condition, result: condResult } of expr.whens) {
      if (this._evalGroupCond(condition, groupRows, result, computeAgg)) {
        return this._evalGroupExpr(condResult, groupRows, result, computeAgg);
      }
    }
    return expr.elseResult ? this._evalGroupExpr(expr.elseResult, groupRows, result, computeAgg) : null;
  }
  if (expr.type === 'arith') {
    const left = this._evalGroupExpr(expr.left, groupRows, result, computeAgg);
    const right = this._evalGroupExpr(expr.right, groupRows, result, computeAgg);
    if (left == null || right == null) return null;
    switch (expr.op) {
      case '+': return left + right;
      case '-': return left - right;
      case '*': return left * right;
      case '/': return right === 0 ? null : left / right;
      case '%': return left % right;
    }
  }
  if (expr.type === 'unary_minus') {
    const val = this._evalGroupExpr(expr.operand, groupRows, result, computeAgg);
    if (val == null) return null;
    return val === 0 ? 0 : -val;
  }
  if (expr.type === 'function_call' || expr.type === 'function') {
    const args = (expr.args || []).map(a => this._evalGroupExpr(a, groupRows, result, computeAgg));
    return this._evalFunction(expr.func, args.map(v => ({ type: 'literal', value: v })), groupRows[0]);
  }
  if (expr.type === 'cast') {
    const val = this._evalGroupExpr(expr.expr, groupRows, result, computeAgg);
    if (val == null) return null;
    switch (expr.targetType?.toUpperCase()) {
      case 'INT': case 'INTEGER': case 'BIGINT': case 'SMALLINT': return Math.trunc(Number(val));
      case 'FLOAT': case 'DOUBLE': case 'REAL': case 'DECIMAL': case 'NUMERIC': return Number(val);
      case 'TEXT': case 'VARCHAR': case 'CHAR': return String(val);
      case 'BOOLEAN': case 'BOOL': return !!val;
      default: return val;
    }
  }
  if (expr.type === 'IS_NULL') {
    const val = this._evalGroupExpr(expr.left, groupRows, result, computeAgg);
    return val === null || val === undefined;
  }
  if (expr.type === 'IS_NOT_NULL') {
    const val = this._evalGroupExpr(expr.left, groupRows, result, computeAgg);
    return val !== null && val !== undefined;
  }
  // Fallback: try regular eval on first row
  return this._evalValue(expr, groupRows[0]);
}

P._evalGroupCond = function(cond, groupRows, result, computeAgg) {
  if (!cond) return true;
  if (cond.type === 'COMPARE') {
    const left = this._evalGroupExpr(cond.left, groupRows, result, computeAgg);
    const right = this._evalGroupExpr(cond.right, groupRows, result, computeAgg);
    switch (cond.op) {
      case 'EQ': case '=': return left === right;
      case 'NE': case '!=': case '<>': return left !== right;
      case 'LT': case '<': return sqliteCompare(left, right) < 0;
      case 'GT': case '>': return sqliteCompare(left, right) > 0;
      case 'LE': case '<=': return sqliteCompare(left, right) <= 0;
      case 'GE': case '>=': return sqliteCompare(left, right) >= 0;
    }
  }
  if (cond.type === 'AND') return this._evalGroupCond(cond.left, groupRows, result, computeAgg) && this._evalGroupCond(cond.right, groupRows, result, computeAgg);
  if (cond.type === 'OR') return this._evalGroupCond(cond.left, groupRows, result, computeAgg) || this._evalGroupCond(cond.right, groupRows, result, computeAgg);
  if (cond.type === 'NOT') return !this._evalGroupCond(cond.expr, groupRows, result, computeAgg);
  // Fallback: eval as expression
  return !!this._evalGroupExpr(cond, groupRows, result, computeAgg);
}

/**
 * Extract a value from a JSON object using a path expression.
 * Supports: $.key, $.nested.key, $.array[0], $[0], $.key.array[1].nested
 */
P._jsonExtract = function(obj, path) {
  return _jsonExtractImpl(obj, path);
}

/**
 * Evaluate an expression that contains aggregate functions against a set of rows.
 * Recursively evaluates aggregate_expr nodes against all rows, then computes arithmetic.
 */
P._evalAggregateExpr = function(expr, rows) {
  if (!expr) return null;
  if (expr.type === 'aggregate_expr') {
    // Compute the aggregate
    const func = expr.func;
    const isStarArg = expr.arg === '*' || (typeof expr.arg === 'object' && expr.arg?.name === '*');
    let values;
    if (isStarArg) {
      values = rows;
    } else if (typeof expr.arg === 'object') {
      values = rows.map(r => this._evalValue(expr.arg, r)).filter(v => v != null);
    } else {
      values = rows.map(r => this._resolveColumn(expr.arg, r)).filter(v => v != null);
    }
    if (expr.distinct) values = [...new Set(values)];
    
    switch (func) {
      case 'COUNT': return isStarArg ? rows.length : values.length;
      case 'SUM': return values.length ? values.reduce((s, v) => s + (typeof v === 'string' ? Number(v) || 0 : v), 0) : null;
      case 'AVG': return values.length ? values.reduce((s, v) => s + (typeof v === 'string' ? Number(v) || 0 : v), 0) / values.length : null;
      case 'MIN': return values.length ? values.reduce((a, b) => a < b ? a : b) : null;
      case 'MAX': return values.length ? values.reduce((a, b) => a > b ? a : b) : null;
      default: return null;
    }
  }
  if (expr.type === 'arith') {
    const left = this._evalAggregateExpr(expr.left, rows);
    const right = this._evalAggregateExpr(expr.right, rows);
    if (left == null || right == null) return null;
    switch (expr.op) {
      case '+': return left + right;
      case '-': return left - right;
      case '*': return left * right;
      case '/': return right !== 0 ? left / right : null;
      case '%': return right !== 0 ? left % right : null;
      default: return null;
    }
  }
  if (expr.type === 'literal') return expr.value;
  if (expr.type === 'number') return expr.value;
  if (expr.type === 'cast') {
    const val = this._evalAggregateExpr(expr.expr, rows);
    if (val == null) return null;
    switch (expr.targetType?.toUpperCase()) {
      case 'INT': case 'INTEGER': case 'BIGINT': case 'SMALLINT': return Math.trunc(Number(val));
      case 'FLOAT': case 'DOUBLE': case 'REAL': case 'DECIMAL': case 'NUMERIC': return Number(val);
      case 'TEXT': case 'VARCHAR': case 'CHAR': return String(val);
      case 'BOOLEAN': case 'BOOL': return !!val;
      default: return val;
    }
  }
  if (expr.type === 'function_call' || expr.type === 'function') {
    const args = (expr.args || []).map(a => this._evalAggregateExpr(a, rows));
    if (expr.func?.toUpperCase() === 'COALESCE') return args.find(v => v !== null && v !== undefined) ?? null;
    if (expr.func?.toUpperCase() === 'NULLIF') return args[0] === args[1] ? null : args[0];
    return this._evalFunction(expr.func, args.map(v => ({ type: 'literal', value: v })), rows[0] || {});
  }
  // For non-aggregate expressions, evaluate against first row
  return rows.length ? this._evalValue(expr, rows[0]) : null;
}

/**
 * Check if an expression tree contains any aggregate function calls.
 */
P._exprContainsAggregate = function(expr) {
  return exprContains(expr, n => {
    if (n.type === 'aggregate_expr') return true;
    if ((n.type === 'function_call' || n.type === 'function') && 
        ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'BOOL_AND', 'BOOL_OR', 'EVERY', 'GROUP_CONCAT', 'STRING_AGG', 'JSON_AGG', 'JSONB_AGG', 'ARRAY_AGG'].includes(n.func?.toUpperCase())) return true;
    return false;
  });
}

P._validateColumnRefs = function(expr, schemaColNames, tableName, tableAlias) {
  if (!expr) return;
  if (expr.type === 'column_ref') {
    let colName = expr.name;
    // Strip table alias prefix
    if (colName.includes('.')) {
      const parts = colName.split('.');
      const prefix = parts[0].toLowerCase();
      if (prefix === tableName.toLowerCase() || prefix === tableAlias?.toLowerCase()) {
        colName = parts.slice(1).join('.');
      } else {
        return; // Different table alias — skip (could be outer query ref)
      }
    }
    if (!schemaColNames.has(colName.toLowerCase())) {
      throw new Error(`Column "${colName}" does not exist in table "${tableName}"`);
    }
  }
  // Recurse into sub-expressions
  if (expr.left) this._validateColumnRefs(expr.left, schemaColNames, tableName, tableAlias);
  if (expr.right) this._validateColumnRefs(expr.right, schemaColNames, tableName, tableAlias);
  if (expr.expr) this._validateColumnRefs(expr.expr, schemaColNames, tableName, tableAlias);
  if (expr.operand) this._validateColumnRefs(expr.operand, schemaColNames, tableName, tableAlias);
  if (expr.args) expr.args.forEach(a => this._validateColumnRefs(a, schemaColNames, tableName, tableAlias));
  if (expr.values) expr.values.forEach(v => this._validateColumnRefs(v, schemaColNames, tableName, tableAlias));
  // Skip subqueries — they have their own scope
  if (expr.type === 'SUBQUERY' || expr.type === 'EXISTS' || expr.type === 'IN_SUBQUERY') return;
}

P._resolveColumn = function(name, row) {
  // Handle numeric column references (ORDER BY 1, 2, etc.)
  if (typeof name === 'number') {
    const keys = Object.keys(row);
    const idx = name - 1; // 1-based to 0-based
    if (idx >= 0 && idx < keys.length) return row[keys[idx]];
    return undefined;
  }
  if (name in row) return row[name];
  // Case-insensitive lookup
  const lowerName = name.toLowerCase();
  for (const key of Object.keys(row)) {
    if (key.toLowerCase() === lowerName) return row[key];
  }
  // Try without table prefix (e.g., t.a → a)
  for (const key of Object.keys(row)) {
    if (key.endsWith(`.${name}`)) return row[key];
    if (key.toLowerCase().endsWith(`.${lowerName}`)) return row[key];
  }
  // If name is qualified (contains '.'), try stripping the table alias
  if (name.includes('.')) {
    const colName = name.split('.').pop();
    const tablePrefix = name.substring(0, name.lastIndexOf('.'));
    const lowerColName = colName.toLowerCase();
    const lowerPrefix = tablePrefix.toLowerCase();
    
    // Check if this table prefix belongs to the current (inner) query scope
    const isInnerAlias = this._innerTableAliases && this._innerTableAliases.has(lowerPrefix);
    // Check if this table prefix belongs to the outer query scope
    const isOuterAlias = !isInnerAlias && this._outerRow;
    
    if (isOuterAlias) {
      // Resolve from outer row
      if (colName in this._outerRow) return this._outerRow[colName];
      for (const key of Object.keys(this._outerRow)) {
        if (key.toLowerCase() === lowerColName) return this._outerRow[key];
      }
    }
    
    // Resolve from inner row (strip alias)
    if (colName in row) return row[colName];
    for (const key of Object.keys(row)) {
      if (key.toLowerCase() === lowerColName) return row[key];
    }
  }
  // For correlated subqueries: check outer row
  if (this._outerRow) {
    if (name in this._outerRow) return this._outerRow[name];
    for (const key of Object.keys(this._outerRow)) {
      if (key.endsWith(`.${name}`)) return this._outerRow[key];
    }
    // If name is qualified, try stripping alias in outer row too
    if (name.includes('.')) {
      const colName = name.split('.').pop();
      if (colName in this._outerRow) return this._outerRow[colName];
      for (const key of Object.keys(this._outerRow)) {
        if (key.toLowerCase() === colName.toLowerCase()) return this._outerRow[key];
      }
    }
  }
  return undefined;
}

/**
 * Detect correlation pattern in a WHERE clause: inner_col = outer_ref
 * Returns { innerCol, outerCol } or null if not a simple correlation.
 */
P._detectCorrelation = function(where, sampleRow) {
  if (!where || !sampleRow) return null;
  if (where.type === 'COMPARE' && where.op === 'EQ') {
    const left = where.left;
    const right = where.right;
    // Check: left is inner column, right is outer column (or vice versa)
    if (left?.type === 'column_ref' && right?.type === 'column_ref') {
      const leftName = left.name;
      const rightName = right.name;
      // Determine which is the outer reference by checking if it exists in the sample row
      const leftInRow = this._resolveColumn(leftName, sampleRow) !== undefined ||
        Object.keys(sampleRow).some(k => k === leftName || k.endsWith('.' + leftName));
      const rightInRow = this._resolveColumn(rightName, sampleRow) !== undefined ||
        Object.keys(sampleRow).some(k => k === rightName || k.endsWith('.' + rightName));
      
      if (leftInRow && !rightInRow) {
        return { outerCol: leftName, innerCol: rightName };
      }
      if (rightInRow && !leftInRow) {
        return { outerCol: rightName, innerCol: leftName };
      }
    }
  }
  // Check AND: look for correlation in either side
  if (where.type === 'AND') {
    return this._detectCorrelation(where.left, sampleRow) ||
           this._detectCorrelation(where.right, sampleRow);
  }
  return null;
}

/**
 * Remove the correlation predicate from a WHERE clause.
 * Returns the remaining predicates (or null if the entire WHERE was the correlation).
 */
P._removeCorrelationPredicate = function(where, corr) {
  if (!where) return null;
  if (where.type === 'COMPARE' && where.op === 'EQ') {
    const leftName = where.left?.name;
    const rightName = where.right?.name;
    if ((leftName === corr.innerCol && rightName === corr.outerCol) ||
        (leftName === corr.outerCol && rightName === corr.innerCol)) {
      return null; // This IS the correlation predicate — remove it
    }
  }
  if (where.type === 'AND') {
    const left = this._removeCorrelationPredicate(where.left, corr);
    const right = this._removeCorrelationPredicate(where.right, corr);
    if (!left && !right) return null;
    if (!left) return right;
    if (!right) return left;
    return { type: 'AND', left, right };
  }
  return where;
}

P._isFloatColumnRef = function(node, row) {
  if (!node || node.type !== 'column_ref') return false;
  const colName = node.name?.includes('.') ? node.name.split('.').pop() : node.name;
  if (!colName) return false;
  // Look up the column type in known tables
  for (const [, table] of this.tables) {
    const col = table.schema?.find(c => c.name === colName);
    if (col) {
      const type = (col.type || '').toUpperCase();
      if (type.startsWith('DECIMAL') || type.startsWith('NUMERIC') || 
          type === 'REAL' || type === 'FLOAT' || type === 'DOUBLE' || 
          type === 'DOUBLE PRECISION' || type.startsWith('FLOAT')) {
        return true;
      }
      return false;
    }
  }
  return false;
}

P._evalExpr = function(expr, row) {
  if (!expr) return true;
  switch (expr.type) {
    case 'literal': {
      if (expr.value == null) return null; // Preserve NULL for three-valued logic
      return !!expr.value; // 0/false → false, others → true
    }
    case 'AND': {
      const left = this._evalExpr(expr.left, row);
      const right = this._evalExpr(expr.right, row);
      // Three-valued logic: NULL AND false = false, NULL AND true = NULL
      if (left === false || left === 0) return false;
      if (right === false || right === 0) return false;
      if (left == null || right == null) return null;
      return left && right;
    }
    case 'OR': {
      const left = this._evalExpr(expr.left, row);
      const right = this._evalExpr(expr.right, row);
      // Three-valued logic: NULL OR true = true, NULL OR false = NULL
      if (left === true || left === 1) return true;
      if (right === true || right === 1) return true;
      if (left == null || right == null) return null;
      return left || right;
    }
    case 'NOT': {
      const val = this._evalExpr(expr.expr, row);
      if (val == null) return null;
      return !val;
    }
    case 'MATCH_AGAINST': {
      // Find the fulltext index for this column
      const searchText = this._evalValue(expr.search, row);
      const column = expr.column;
      
      // Find a fulltext index that covers this column
      let ftIdx = null;
      for (const [, idx] of this.fulltextIndexes) {
        if (idx.column === column) { ftIdx = idx; break; }
      }
      if (!ftIdx) throw new Error(`No fulltext index found for column ${column}`);
      
      // Get the text from the current row
      const rowText = String(row[column] || '');
      const rowTokens = tokenize(rowText);
      const searchTokens = tokenize(String(searchText));
      
      // Check if all search terms appear in the row
      return searchTokens.every(st => rowTokens.includes(st));
    }
    case 'EXISTS': {
      const result = this._evalSubquery(expr.subquery, row);
      return result.length > 0;
    }
    case 'IN_SUBQUERY': {
      const leftVal = this._evalValue(expr.left, row);
      const result = this._evalSubquery(expr.subquery, row);
      return result.some(r => {
        const vals = Object.values(r);
        return vals.includes(leftVal);
      });
    }
    case 'IN_HASHSET': {
      const leftVal = this._evalValue(expr.left, row);
      const found = expr.hashSet.has(leftVal);
      return expr.negated ? !found : found;
    }
    case 'CORRELATED_IN_HASHMAP': {
      // Batch-decorrelated correlated IN subquery
      const leftVal = this._evalValue(expr.left, row);
      // Build composite key from outer columns
      const keyParts = expr.outerCols.map(col => {
        const name = col.includes('.') ? col : col;
        return this._evalValue({ type: 'column_ref', name }, row);
      });
      const key = keyParts.length === 1 ? String(keyParts[0]) : keyParts.map(String).join('\0');
      const valSet = expr.hashMap.get(key);
      const found = valSet ? valSet.has(leftVal) : false;
      return expr.negated ? !found : found;
    }
    case 'LITERAL_BOOL': {
      return expr.value;
    }
    case 'IN_LIST': {
      const leftVal = this._evalValue(expr.left, row);
      if (leftVal == null) return null; // NULL IN (...) is NULL
      let hasNull = false;
      for (const v of expr.values) {
        const rightVal = this._evalValue(v, row);
        if (rightVal == null) { hasNull = true; continue; }
        if (rightVal === leftVal) return true;
      }
      return hasNull ? null : false; // If no match but NULLs present, result is NULL
    }
    case 'IS_NULL': {
      const val = this._evalValue(expr.left, row);
      return val === null || val === undefined;
    }
    case 'IS_NOT_NULL': {
      const val = this._evalValue(expr.left, row);
      return val !== null && val !== undefined;
    }
    case 'LIKE': {
      const val = this._evalValue(expr.left, row);
      const pattern = this._evalValue(expr.pattern, row);
      if (val == null || pattern == null) return null;
      const escapeChar = expr.escape ? this._evalValue(expr.escape, row) : null;
      const regex = this._likeToRegex(String(pattern), escapeChar);
      return new RegExp(regex).test(String(val));
    }
    case 'ILIKE': {
      const val = this._evalValue(expr.left, row);
      const pattern = this._evalValue(expr.pattern, row);
      if (val == null || pattern == null) return null;
      const escapeChar = expr.escape ? this._evalValue(expr.escape, row) : null;
      const regex = this._likeToRegex(String(pattern), escapeChar);
      return new RegExp(regex, 'i').test(String(val));
    }
    case 'SIMILAR_TO': {
      const val = this._evalValue(expr.left, row);
      const pattern = this._evalValue(expr.pattern, row);
      if (val == null || pattern == null) return false;
      // SIMILAR TO: SQL standard regex with %, _, |, (), [], *, +
      // Convert to JS regex by only escaping non-SQL-regex chars
      let regex = '^';
      const p = String(pattern);
      for (let i = 0; i < p.length; i++) {
        const ch = p[i];
        if (ch === '%') regex += '.*';
        else if (ch === '_') regex += '.';
        else if (ch === '(' || ch === ')' || ch === '|' || ch === '[' || ch === ']' || ch === '+' || ch === '*') regex += ch;
        else if (ch === '\\' && i + 1 < p.length) { regex += '\\' + p[++i]; }
        else if ('.^${}?'.includes(ch)) regex += '\\' + ch;
        else regex += ch;
      }
      regex += '$';
      return new RegExp(regex).test(String(val));
    }
    case 'REGEXP': {
      const val = this._evalValue(expr.left, row);
      const pattern = this._evalValue(expr.pattern, row);
      if (val == null || pattern == null) return null;
      try {
        return new RegExp(String(pattern)).test(String(val));
      } catch {
        return false; // Invalid regex pattern
      }
    }
    case 'BETWEEN': {
      const val = this._evalValue(expr.left, row);
      let low = this._evalValue(expr.low, row);
      let high = this._evalValue(expr.high, row);
      if (val === null || val === undefined || low === null || low === undefined || high === null || high === undefined) return null;
      if (expr.symmetric && low > high) { const tmp = low; low = high; high = tmp; }
      return val >= low && val <= high;
    }
    case 'NOT_BETWEEN': {
      const val = this._evalValue(expr.left, row);
      let low = this._evalValue(expr.low, row);
      let high = this._evalValue(expr.high, row);
      if (val === null || val === undefined || low === null || low === undefined || high === null || high === undefined) return null;
      if (expr.symmetric && low > high) { const tmp = low; low = high; high = tmp; }
      return val < low || val > high;
    }
    case 'TS_MATCH': {
      // Full-text search: to_tsvector(text) @@ to_tsquery(query)
      const leftVal = this._evalValue(expr.left, row);
      const rightVal = this._evalValue(expr.right, row);
      if (leftVal === null || leftVal === undefined || rightVal === null || rightVal === undefined) return false;
      // Both sides should be evaluated as function calls (to_tsvector, to_tsquery)
      // The function evaluator will return TSVector/TSQuery objects or strings
      // If they're strings, we need to do the matching ourselves
      if (typeof leftVal === 'string' && typeof rightVal === 'string') {
        // leftVal is the text (from to_tsvector), rightVal is the query (from to_tsquery)
        // Simple word-level matching
        const words = leftVal.toLowerCase().replace(/[^\w\s]/g, ' ').split(/\s+/).filter(Boolean);
        const queryTerms = rightVal.toLowerCase().replace(/[^\w\s&|!]/g, ' ').split(/\s+/).filter(Boolean);
        return queryTerms.every(term => words.some(w => w.includes(term) || term.includes(w)));
      }
      // If FTS module returns proper objects, use their match method
      if (leftVal && typeof leftVal.matches === 'function') {
        return leftVal.matches(rightVal);
      }
      if (rightVal && typeof rightVal.matches === 'function') {
        return rightVal.matches(leftVal);
      }
      return false;
    }
    case 'NATURAL_EQ': {
      // Compare column from left and right table in merged row
      // Use the RIGHT alias (qualified name preserved) and LEFT's original value
      // Left value: stored as __natural_left_<col> before merge
      const lVal = row[`__natural_left_${expr.column}`] ?? row[expr.column];
      const rVal = row[`${expr.rightAlias}.${expr.column}`] ?? row[expr.column];
      return lVal === rVal;
    }
    case 'QUANTIFIED_COMPARE': {
      // val op ANY/ALL (subquery)
      const leftVal = this._evalValue(expr.left, row);
      if (leftVal === null || leftVal === undefined) return false;
      const subRows = this._evalSubquery(expr.subquery, row);
      if (subRows.length === 0) {
        return expr.quantifier === 'ALL'; // ALL with empty set is true, ANY with empty set is false
      }
      const compare = (left, right) => {
        if (right === null || right === undefined) return null;
        switch (expr.op) {
          case 'EQ': return left === right;
          case 'NE': return left !== right;
          case 'LT': return sqliteCompare(left, right) < 0;
          case 'GT': return sqliteCompare(left, right) > 0;
          case 'LE': return sqliteCompare(left, right) <= 0;
          case 'GE': return sqliteCompare(left, right) >= 0;
        }
      };
      if (expr.quantifier === 'ANY') {
        return subRows.some(r => compare(leftVal, Object.values(r)[0]) === true);
      } else {
        return subRows.every(r => compare(leftVal, Object.values(r)[0]) === true);
      }
    }
    case 'COMPARE': {
      let left = this._evalValue(expr.left, row);
      let right = this._evalValue(expr.right, row);
      // SQL NULL semantics: any comparison with NULL returns NULL (unknown)
      // This is correct for three-valued logic in WHERE, HAVING, CHECK, etc.
      if (left === null || left === undefined || right === null || right === undefined) {
        // Special case: IS / IS NOT operators handle NULL directly
        if (expr.op === 'IS') return left === right;
        if (expr.op === 'IS_NOT') return left !== right;
        return null;
      }
      // Implicit type coercion: if one is number and other is string, try numeric comparison
      if (typeof left === 'number' && typeof right === 'string') {
        const n = Number(right);
        if (!isNaN(n)) right = n;
      } else if (typeof left === 'string' && typeof right === 'number') {
        const n = Number(left);
        if (!isNaN(n)) left = n;
      }
      switch (expr.op) {
        case 'EQ': return left === right;
        case 'NE': return left !== right;
        case 'LT': return sqliteCompare(left, right) < 0;
        case 'GT': return sqliteCompare(left, right) > 0;
        case 'LE': return sqliteCompare(left, right) <= 0;
        case 'GE': return sqliteCompare(left, right) >= 0;
      }
    }
    default: {
      // For expression types not explicitly handled as boolean conditions
      // (arith, function_call, case_expr, etc.), evaluate as a value and
      // check truthiness. This handles WHERE 1+1, WHERE LENGTH('x'), etc.
      try {
        const val = this._evalValue(expr, row);
        return !!val; // NULL/0/false/'' → false, others → true
      } catch {
        return true; // If evaluation fails, default to true for safety
      }
    }
  }
}

P._evalValue = function(node, row) {
  if (node.type === 'literal') return node.value;
  if (node.type === 'column_ref') return this._resolveColumn(node.name, row);
  // Window function node — resolve pre-computed value
  if (node.type === 'window') {
    const key = node._windowKey || node.alias || `${node.func}(${node.arg || ''})`;
    return row[`__window_${key}`];
  }
  // Boolean expression types — delegate to _evalExpr and return true/false as values
  if (node.type === 'IS_NULL' || node.type === 'IS_NOT_NULL' ||
      node.type === 'COMPARE' || node.type === 'BETWEEN' || node.type === 'NOT_BETWEEN' ||
      node.type === 'LIKE' || node.type === 'ILIKE' || node.type === 'REGEXP' ||
      node.type === 'TS_MATCH' ||
      node.type === 'IN_LIST' || node.type === 'IN_SUBQUERY' ||
      node.type === 'NOT_IN' || node.type === 'NOT_LIKE' ||
      node.type === 'IS_TRUE' || node.type === 'IS_FALSE' ||
      node.type === 'IS_NOT_TRUE' || node.type === 'IS_NOT_FALSE' ||
      node.type === 'IS_DISTINCT_FROM' || node.type === 'IS_NOT_DISTINCT_FROM' ||
      node.type === 'AND' || node.type === 'OR' || node.type === 'NOT' ||
      node.type === 'EXISTS') {
    const result = this._evalExpr(node, row);
    if (result == null) return null;
    return result ? true : false;
  }
  if (node.type === 'MATCH_AGAINST') {
    // Return relevance score
    return this._evalExpr(node, row) ? 1 : 0;
  }
  if (node.type === 'SUBQUERY') {
    const result = this._evalSubquery(node.subquery, row);
    if (result.length === 0) return null;
    const firstRow = result[0];
    return Object.values(firstRow)[0];
  }
  if (node.type === 'scalar_subquery') {
    const result = this._evalSubquery(node.subquery, row);
    if (result.length === 0) return null;
    const firstRow = result[0];
    return Object.values(firstRow)[0];
  }
  if (node.type === 'function_call' || node.type === 'function') {
    return this._evalFunction(node.func, node.args, row);
  }
  if (node.type === 'cast') {
    const val = this._evalValue(node.expr, row);
    if (val == null) return null;
    switch (node.targetType) {
      case 'INT': case 'INTEGER': return parseInt(val, 10) || 0;
      case 'FLOAT': case 'REAL': case 'DOUBLE': return parseFloat(val) || 0;
      case 'TEXT': case 'VARCHAR': case 'CHAR': return String(val);
      case 'BOOLEAN': return Boolean(val);
      default: return val;
    }
  }
  if (node.type === 'case_expr') {
    for (const { condition, result } of node.whens) {
      if (this._evalExpr(condition, row)) {
        return this._evalValue(result, row);
      }
    }
    return node.elseResult ? this._evalValue(node.elseResult, row) : null;
  }
  if (node.type === 'interval') {
    return { __interval: true, value: node.value };
  }
  if (node.type === 'unary_minus') {
    const val = this._evalValue(node.operand, row);
    if (val == null) return null;
    const neg = -val;
    return neg === 0 ? 0 : neg; // avoid -0
  }
  if (node.type === 'arith') {
    const left = this._evalValue(node.left, row);
    const right = this._evalValue(node.right, row);
    if (left == null || right == null) return null;
    // Date arithmetic with INTERVAL
    if (right && right.__interval && (node.op === '+' || node.op === '-')) {
      return this._dateArith(left, right.value, node.op);
    }
    if (left && left.__interval && node.op === '+') {
      return this._dateArith(right, left.value, '+');
    }
    // SQL arithmetic: coerce string operands to numbers (SQLite compat)
    const l = typeof left === 'string' ? (isNaN(Number(left)) ? 0 : Number(left)) : left;
    const r = typeof right === 'string' ? (isNaN(Number(right)) ? 0 : Number(right)) : right;
    switch (node.op) {
      case '||': return String(left ?? '') + String(right ?? '');  // SQL concat — no coercion
      case '+': return l + r;
      case '-': return l - r;
      case '*': return l * r;
      case '/': {
        if (r === 0) return null;
        const result = l / r;
        // Integer division when both operands are integer-typed (SQL standard).
        const leftIsFloat = node.left?.isFloat || false;
        const rightIsFloat = node.right?.isFloat || false;
        if (leftIsFloat || rightIsFloat) return result;
        if (!Number.isInteger(l) || !Number.isInteger(r)) return result;
        if (this._isFloatColumnRef(node.left, row) || this._isFloatColumnRef(node.right, row)) return result;
        return Math.trunc(result);
      }
      case '%': return r === 0 ? null : l % r;
    }
  }
  if (node.type === 'aggregate_expr') {
    // In HAVING/ORDER BY context, look up the computed aggregate from the row
    const argStr = this._serializeExpr(node.arg);
    const key = `${node.func}(${argStr})`;
    if (key in row) return row[key];
    // Check prefixed aggregate keys (used for HAVING resolution)
    const prefixedKey = `__agg_${key}`;
    if (prefixedKey in row) return row[prefixedKey];
    // Try to find it with any alias pattern
    for (const k of Object.keys(row)) {
      if (k.toUpperCase().includes(node.func) && k.includes(argStr)) return row[k];
    }
    return null;
  }
  return null;
}

P._dateArith = function(dateStr, intervalStr, op) {
  return _dateArithImpl(dateStr, intervalStr, op);
}

P._evalFunction = function(func, args, row) {
  return _evalFunctionImpl(this, func, args, row);
}

P._evalSubquery = function(subqueryAst, outerRow) {
  // Execute the subquery, passing outerRow for correlated references
  const savedOuterRow = this._outerRow;
  const savedInnerAliases = this._innerTableAliases;
  this._outerRow = outerRow;
  
  // Collect inner query's table aliases for qualified column resolution
  const aliases = new Set();
  if (subqueryAst.from) {
    const alias = (subqueryAst.from.alias || subqueryAst.from.table || '').toLowerCase();
    if (alias) aliases.add(alias);
  }
  if (subqueryAst.joins) {
    for (const join of subqueryAst.joins) {
      const alias = (join.alias || join.table || '').toLowerCase();
      if (alias) aliases.add(alias);
    }
  }
  this._innerTableAliases = aliases;
  
  const result = this._select(subqueryAst);
  this._outerRow = savedOuterRow;
  this._innerTableAliases = savedInnerAliases;
  return result.rows;
}

P._computeSingleAggregate = function(func, arg, rows, distinct) {
  if (func === 'COUNT' && (arg === '*' || (arg && arg.type === 'literal' && arg.value === '*'))) return rows.length;
  let vals = rows.map(r => this._evalValue(arg, r)).filter(v => v != null);
  if (distinct) vals = [...new Set(vals)];
  switch (func) {
    case 'COUNT': return arg.type === 'literal' && arg.value === '*' ? rows.length : vals.length;
    case 'SUM': return vals.reduce((a, b) => Number(a) + Number(b), 0);
    case 'AVG': return vals.length > 0 ? vals.reduce((a, b) => Number(a) + Number(b), 0) / vals.length : null;
    case 'MAX': return vals.length > 0 ? vals.reduce((a, b) => a > b ? a : b) : null;
    case 'MIN': return vals.length > 0 ? vals.reduce((a, b) => a < b ? a : b) : null;
    default: return null;
  }
}

P._computeAggregates = function(columns, rows) {
  const result = {};
  for (const col of columns) {
    // Handle expression columns that contain aggregates (e.g., SUM(a) / SUM(b))
    if (col.type === 'expression' && this._exprContainsAggregate(col.expr)) {
      const name = col.alias || 'expr';
      result[name] = this._evalAggregateExpr(col.expr, rows);
      continue;
    }
    // Handle function columns that contain aggregates (e.g., COALESCE(SUM(a), 0))
    if (col.type === 'function' && col.args && col.args.some(a => this._exprContainsAggregate(a))) {
      const name = col.alias || `${col.func}(...)`;
      // Evaluate each argument, replacing aggregate expressions with their computed values
      const evaluatedArgs = col.args.map(arg => {
        if (this._exprContainsAggregate(arg)) {
          return this._evalAggregateExpr(arg, rows);
        }
        return arg.type === 'literal' ? arg.value : this._evalValue(arg, rows[0] || {});
      });
      // Evaluate the function with computed args
      if (col.func.toUpperCase() === 'COALESCE') {
        result[name] = evaluatedArgs.find(v => v !== null && v !== undefined) ?? null;
      } else if (col.func.toUpperCase() === 'NULLIF') {
        result[name] = evaluatedArgs[0] === evaluatedArgs[1] ? null : evaluatedArgs[0];
      } else if (col.func.toUpperCase() === 'IFNULL' || col.func.toUpperCase() === 'NVL') {
        result[name] = evaluatedArgs[0] ?? evaluatedArgs[1];
      } else {
        // Try evaluating as a regular function with a synthetic row
        result[name] = this._evalFunction(col.func, evaluatedArgs.map((v, i) => 
          ({ type: 'literal', value: v })), rows[0] || {});
      }
      continue;
    }
    if (col.type !== 'aggregate') continue;
    const argStr = typeof col.arg === 'object' ? 'expr' : col.arg;
    let name = col.alias || `${col.func}(${argStr})`;
    // Deduplicate: SUM(a), SUM(a) → SUM(a), SUM(a)_1
    if (name in result) { let s = 1; while (`${name}_${s}` in result) s++; name = `${name}_${s}`; }
    
    // Apply FILTER clause: only include rows matching the filter condition
    let filteredRows = rows;
    if (col.filter) {
      filteredRows = rows.filter(r => {
        try { return !!this._evalExpr(col.filter, r); } catch { return false; }
      });
    }
    
    let values;
    if (col.arg === '*') {
      values = filteredRows;
    } else if (typeof col.arg === 'object') {
      values = filteredRows.map(r => this._evalValue(col.arg, r)).filter(v => v != null);
    } else {
      values = filteredRows.map(r => this._resolveColumn(col.arg, r)).filter(v => v != null);
    }

    switch (col.func) {
      case 'COUNT': {
        if (col.distinct && col.arg !== '*') {
          result[name] = new Set(values).size;
        } else {
          result[name] = col.arg === '*' ? filteredRows.length : values.length;
        }
        break;
      }
      case 'SUM': result[name] = values.length ? values.reduce((s, v) => s + (typeof v === 'string' ? Number(v) || 0 : v), 0) : null; break;
      case 'AVG': result[name] = values.length ? values.reduce((s, v) => s + (typeof v === 'string' ? Number(v) || 0 : v), 0) / values.length : null; break;
      case 'MIN': result[name] = values.length ? values.reduce((a, b) => a < b ? a : b) : null; break;
      case 'MAX': result[name] = values.length ? values.reduce((a, b) => a > b ? a : b) : null; break;
      case 'GROUP_CONCAT':
      case 'STRING_AGG': {
        const sep = col.separator || ',';
        let items = col.distinct ? [...new Set(values)] : values;
        // Apply ORDER BY if specified inside the aggregate
        if (col.aggOrderBy && col.aggOrderBy.length > 0) {
          const ordered = filteredRows.slice();
          ordered.sort((a, b) => {
            for (const ob of col.aggOrderBy) {
              const av = this._evalValue(ob.column, a);
              const bv = this._evalValue(ob.column, b);
              if (av < bv) return ob.direction === 'DESC' ? 1 : -1;
              if (av > bv) return ob.direction === 'DESC' ? -1 : 1;
            }
            return 0;
          });
          items = ordered.map(r => {
            const v = typeof col.arg === 'string' ? r[col.arg] : this._evalValue(col.arg, r);
            return v;
          }).filter(v => v != null);
          if (col.distinct) items = [...new Set(items)];
        }
        result[name] = items.length ? items.map(String).join(sep) : null;
        break;
      }
      case 'JSON_AGG':
      case 'JSONB_AGG': {
        const vals = col.distinct ? [...new Set(values)] : values;
        // Try to parse string values as JSON to avoid double-encoding
        const parsed = vals.map(v => {
          if (typeof v === 'string') {
            try { return JSON.parse(v); } catch { return v; }
          }
          return v;
        });
        result[name] = JSON.stringify(parsed);
        break;
      }
      case 'ARRAY_AGG': {
        let items = col.distinct ? [...new Set(values)] : values;
        // Apply ORDER BY if specified inside the aggregate
        if (col.aggOrderBy && col.aggOrderBy.length > 0) {
          const ordered = filteredRows.slice();
          ordered.sort((a, b) => {
            for (const ob of col.aggOrderBy) {
              const av = this._evalValue(ob.column, a);
              const bv = this._evalValue(ob.column, b);
              if (av < bv) return ob.direction === 'DESC' ? 1 : -1;
              if (av > bv) return ob.direction === 'DESC' ? -1 : 1;
            }
            return 0;
          });
          items = ordered.map(r => {
            const v = typeof col.arg === 'string' ? r[col.arg] : this._evalValue(col.arg, r);
            return v;
          }).filter(v => v != null);
          if (col.distinct) items = [...new Set(items)];
        }
        result[name] = items;
        break;
      }
      case 'BOOL_AND':
      case 'EVERY': {
        // Returns TRUE if all values are true/truthy, NULL if all are null
        const boolVals = values.filter(v => v != null);
        result[name] = boolVals.length === 0 ? null : boolVals.every(v => !!v);
        break;
      }
      case 'BOOL_OR': {
        // Returns TRUE if any value is true/truthy, NULL if all are null
        const boolVals2 = values.filter(v => v != null);
        result[name] = boolVals2.length === 0 ? null : boolVals2.some(v => !!v);
        break;
      }
      case 'PERCENTILE_CONT': {
        // Continuous percentile: interpolates between values
        // fraction is stored in col.percentile or as a literal in col.args
        const fraction = col.percentile ?? (col.args?.[0]?.value ?? 0.5);
        const sorted = values.map(Number).sort((a, b) => a - b);
        if (sorted.length === 0) { result[name] = null; break; }
        if (sorted.length === 1) { result[name] = sorted[0]; break; }
        // PostgreSQL formula: position = fraction * (N - 1)
        const pos = fraction * (sorted.length - 1);
        const lower = Math.floor(pos);
        const upper = Math.ceil(pos);
        const weight = pos - lower;
        result[name] = sorted[lower] * (1 - weight) + sorted[upper] * weight;
        break;
      }
      case 'PERCENTILE_DISC': {
        // Discrete percentile: returns an actual value from the set
        const fraction2 = col.percentile ?? (col.args?.[0]?.value ?? 0.5);
        const sorted2 = values.map(Number).sort((a, b) => a - b);
        if (sorted2.length === 0) { result[name] = null; break; }
        // PostgreSQL formula: first value where cumulative distribution >= fraction
        const idx = Math.ceil(fraction2 * sorted2.length) - 1;
        result[name] = sorted2[Math.max(0, Math.min(idx, sorted2.length - 1))];
        break;
      }
      case 'MODE': {
        // Returns the most frequent value
        if (values.length === 0) { result[name] = null; break; }
        const freq = new Map();
        for (const v of values) {
          freq.set(v, (freq.get(v) || 0) + 1);
        }
        let maxFreq = 0, modeVal = null;
        for (const [v, count] of freq) {
          if (count > maxFreq) { maxFreq = count; modeVal = v; }
        }
        result[name] = modeVal;
        break;
      }
      case 'STDDEV':
      case 'STDDEV_SAMP': {
        // Sample standard deviation: sqrt(sum((x-mean)^2) / (N-1))
        const nums = values.map(Number);
        if (nums.length < 2) { result[name] = null; break; }
        const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
        const variance = nums.reduce((sum, x) => sum + (x - mean) ** 2, 0) / (nums.length - 1);
        result[name] = Math.sqrt(variance);
        break;
      }
      case 'STDDEV_POP': {
        // Population standard deviation: sqrt(sum((x-mean)^2) / N)
        const nums2 = values.map(Number);
        if (nums2.length === 0) { result[name] = null; break; }
        const mean2 = nums2.reduce((a, b) => a + b, 0) / nums2.length;
        const variance2 = nums2.reduce((sum, x) => sum + (x - mean2) ** 2, 0) / nums2.length;
        result[name] = Math.sqrt(variance2);
        break;
      }
      case 'VARIANCE':
      case 'VAR_SAMP': {
        // Sample variance: sum((x-mean)^2) / (N-1)
        const nums3 = values.map(Number);
        if (nums3.length < 2) { result[name] = null; break; }
        const mean3 = nums3.reduce((a, b) => a + b, 0) / nums3.length;
        result[name] = nums3.reduce((sum, x) => sum + (x - mean3) ** 2, 0) / (nums3.length - 1);
        break;
      }
      case 'VAR_POP': {
        // Population variance: sum((x-mean)^2) / N
        const nums4 = values.map(Number);
        if (nums4.length === 0) { result[name] = null; break; }
        const mean4 = nums4.reduce((a, b) => a + b, 0) / nums4.length;
        result[name] = nums4.reduce((sum, x) => sum + (x - mean4) ** 2, 0) / nums4.length;
        break;
      }
      case 'CORR':
      case 'COVAR_POP':
      case 'COVAR_SAMP':
      case 'REGR_SLOPE':
      case 'REGR_INTERCEPT':
      case 'REGR_R2':
      case 'REGR_COUNT': {
        // Two-arg aggregate: first arg is Y, second is X
        const arg2Col = col.arg2;
        if (!arg2Col) { result[name] = null; break; }
        // Get paired (y, x) values, excluding pairs with NULL in either
        const pairs = [];
        for (const row of (col.filter ? filteredRows : rows)) {
          const y = typeof col.arg === 'object' ? this._evalValue(col.arg, row) : this._resolveColumn(col.arg, row);
          const x = typeof arg2Col === 'object' ? this._evalValue(arg2Col, row) : this._resolveColumn(arg2Col, row);
          if (y != null && x != null) pairs.push([Number(y), Number(x)]);
        }
        if (col.func === 'REGR_COUNT') { result[name] = pairs.length; break; }
        if (pairs.length < 2) { result[name] = null; break; }
        const n5 = pairs.length;
        const meanY5 = pairs.reduce((s, [y]) => s + y, 0) / n5;
        const meanX5 = pairs.reduce((s, [, x]) => s + x, 0) / n5;
        const covPop = pairs.reduce((s, [y, x]) => s + (y - meanY5) * (x - meanX5), 0) / n5;
        const varXPop = pairs.reduce((s, [, x]) => s + (x - meanX5) ** 2, 0) / n5;
        const varYPop = pairs.reduce((s, [y]) => s + (y - meanY5) ** 2, 0) / n5;
        
        switch (col.func) {
          case 'COVAR_POP': result[name] = covPop; break;
          case 'COVAR_SAMP': result[name] = n5 > 1 ? covPop * n5 / (n5 - 1) : null; break;
          case 'CORR': {
            const denom = Math.sqrt(varXPop * varYPop);
            result[name] = denom > 0 ? covPop / denom : null;
            break;
          }
          case 'REGR_SLOPE': result[name] = varXPop > 0 ? covPop / varXPop : null; break;
          case 'REGR_INTERCEPT': result[name] = varXPop > 0 ? meanY5 - (covPop / varXPop) * meanX5 : null; break;
          case 'REGR_R2': {
            const corr = (varXPop > 0 && varYPop > 0) ? covPop / Math.sqrt(varXPop * varYPop) : null;
            result[name] = corr !== null ? corr ** 2 : null;
            break;
          }
        }
        break;
      }
    }
  }
  return result;
}

}
