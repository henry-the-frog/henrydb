// set-operations.js — UNION/INTERSECT/EXCEPT extracted from db.js
// Functions take "db" as first parameter

export function union_(db, ast) {
  // If the UNION has CTEs (from WITH clause), use _withCTEs to materialize them
  if (ast.ctes && ast.ctes.length > 0) {
    return db._withCTEs(ast, () => unionInner(db, ast));
  }
  return unionInner(db, ast);
}

export function unionInner(db, ast) {
  const leftResult = db.execute_ast(ast.left);
  const rightResult = db.execute_ast(ast.right);
  
  // Remap right result's columns to match left result's column names
  const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
  const rightRows = db._remapUnionColumns(rightResult.rows, leftCols);
  
  let rows = [...leftResult.rows, ...rightRows];

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

  // Apply ORDER BY / OFFSET / LIMIT on combined result
  rows = db._applySetOrderLimit(ast, rows);

  return { type: 'ROWS', rows };
}

export function intersect(db, ast) {
  const leftResult = db.execute_ast(ast.left);
  const rightResult = db.execute_ast(ast.right);
  const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
  const rightRemapped = db._remapUnionColumns(rightResult.rows, leftCols);
  
  if (ast.all) {
    // Bag semantics: count occurrences, take min
    const rightCounts = new Map();
    for (const row of rightRemapped) {
      const key = JSON.stringify(row);
      rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
    }
    const rows = [];
    for (const row of leftResult.rows) {
      const key = JSON.stringify(row);
      if ((rightCounts.get(key) || 0) > 0) {
        rows.push(row);
        rightCounts.set(key, rightCounts.get(key) - 1);
      }
    }
    return { type: 'ROWS', rows: db._applySetOrderLimit(ast, rows) };
  }
  
  const rightKeys = new Set(rightRemapped.map(r => JSON.stringify(r)));
  const seen = new Set();
  const rows = leftResult.rows.filter(row => {
    const key = JSON.stringify(row);
    if (rightKeys.has(key) && !seen.has(key)) {
      seen.add(key);
      return true;
    }
    return false;
  });
  return { type: 'ROWS', rows: db._applySetOrderLimit(ast, rows) };
}

export function except_(db, ast) {
  const leftResult = db.execute_ast(ast.left);
  const rightResult = db.execute_ast(ast.right);
  const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
  const rightRemapped = db._remapUnionColumns(rightResult.rows, leftCols);
  
  if (ast.all) {
    // Bag semantics: remove one copy per right row
    const rightCounts = new Map();
    for (const row of rightRemapped) {
      const key = JSON.stringify(row);
      rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
    }
    const rows = [];
    for (const row of leftResult.rows) {
      const key = JSON.stringify(row);
      if ((rightCounts.get(key) || 0) > 0) {
        rightCounts.set(key, rightCounts.get(key) - 1);
      } else {
        rows.push(row);
      }
    }
    return { type: 'ROWS', rows: db._applySetOrderLimit(ast, rows) };
  }
  
  const rightKeys = new Set(rightRemapped.map(r => JSON.stringify(r)));
  const seen = new Set();
  const rows = leftResult.rows.filter(row => {
    const key = JSON.stringify(row);
    if (!rightKeys.has(key) && !seen.has(key)) {
      seen.add(key);
      return true;
    }
    return false;
  });
  
  return { type: 'ROWS', rows: db._applySetOrderLimit(ast, rows) };
}

/** Apply ORDER BY, OFFSET, LIMIT to set operation results */
