// set-ops-helpers.js — Extracted from db.js (2026-04-23)
// Helper methods for UNION/INTERSECT/EXCEPT: ORDER BY, LIMIT, column remapping

/**
 * Apply ORDER BY, OFFSET, and LIMIT to set operation results.
 * @param {object} db - Database instance (for _orderByValue)
 * @param {object} ast - Parsed AST with optional orderBy, offset, limit
 * @param {Array} rows - Input rows
 * @returns {Array} Processed rows
 */
export function applySetOrderLimit(db, ast, rows) {
  if (ast.orderBy) {
    rows.sort((a, b) => {
      for (const { column, direction } of ast.orderBy) {
        const av = db._orderByValue(column, a);
        const bv = db._orderByValue(column, b);
        const cmp = av < bv ? -1 : av > bv ? 1 : 0;
        if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
      }
      return 0;
    });
  }
  if (ast.offset) rows = rows.slice(Math.max(0, ast.offset));
  if (ast.limit != null) rows = rows.slice(0, ast.limit);
  return rows;
}

/**
 * Remap rows' column names to match target column names (for UNION/INTERSECT/EXCEPT).
 * @param {Array} rows - Input rows
 * @param {Array<string>} targetCols - Target column names
 * @returns {Array} Remapped rows
 */
export function remapUnionColumns(rows, targetCols) {
  if (rows.length === 0 || targetCols.length === 0) return rows;
  const srcCols = Object.keys(rows[0]);
  if (srcCols.join() === targetCols.join()) return rows;
  return rows.map(row => {
    const mapped = {};
    const vals = Object.values(row);
    for (let i = 0; i < targetCols.length && i < vals.length; i++) {
      mapped[targetCols[i]] = vals[i];
    }
    return mapped;
  });
}
