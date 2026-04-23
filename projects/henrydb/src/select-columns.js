// select-columns.js — Extracted from db.js (2026-04-23)
// SELECT column projection, ORDER BY, LIMIT/OFFSET application

/**
 * Apply ORDER BY, LIMIT/OFFSET, and SELECT column projection to rows.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed SELECT AST
 * @param {Array} rows - Input rows
 * @returns {object} { type: 'ROWS', rows }
 */
export function applySelectColumns(db, ast, rows) {
  // Pre-compute SELECT alias expressions for ORDER BY access
  if (ast.orderBy && ast.columns) {
    db._preComputeOrderByAliases(ast, rows);
  }
  
  // Apply ORDER BY (with sort elimination for BTree tables)
  if (ast.orderBy && !db._canEliminateSort(ast)) {
    rows.sort((a, b) => {
      for (const { column, direction } of ast.orderBy) {
        const av = db._orderByValue(column, a);
        const bv = db._orderByValue(column, b);
        const aNull = av === null || av === undefined;
        const bNull = bv === null || bv === undefined;
        if (aNull && bNull) continue;
        if (aNull) return direction === 'DESC' ? 1 : -1;
        if (bNull) return direction === 'DESC' ? -1 : 1;
        if (av < bv) return direction === 'DESC' ? 1 : -1;
        if (av > bv) return direction === 'DESC' ? -1 : 1;
      }
      return 0;
    });
  }
  
  // Apply LIMIT/OFFSET
  if (ast.offset) rows = rows.slice(Math.max(0, ast.offset));
  if (ast.limit != null) rows = rows.slice(0, ast.limit);
  
  // Apply SELECT columns
  const isStar = ast.columns.length === 1 && (ast.columns[0].name === '*' || ast.columns[0].type === 'star');
  if (isStar) {
    rows = rows.map(row => db._projectStarRow(row));
  } else {
    rows = rows.map(row => {
      const result = {};
      for (const col of ast.columns) {
        const alias = col.alias || col.name;
        if (col.type === 'column') {
          result[alias] = row[col.name];
        } else if (col.type === 'expression' || col.type === 'aggregate') {
          result[alias] = db._evalValue(col.expr || col, row);
        } else if (col.type === 'function_call') {
          result[alias] = db._evalValue(col, row);
        } else if (col.type === 'window') {
          result[alias] = row[`__window_${alias}`];
        } else {
          result[alias] = db._evalValue(col, row);
        }
      }
      return result;
    });
  }
  return { type: 'ROWS', rows };
}
