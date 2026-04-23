// paginated-exec.js — Extracted from db.js (2026-04-23)
// Paginated query execution

/**
 * Execute a query and return paginated results.
 * @param {object} db - Database instance
 * @param {string} sql - SQL query
 * @param {number} page - Page number (1-indexed)
 * @param {number} pageSize - Rows per page
 * @returns {object} Paginated result with metadata
 */
export function executePaginated(db, sql, page = 1, pageSize = 100) {
  const result = db.execute(sql);
  if (result.type !== 'ROWS') return result;
  
  const totalRows = result.rows.length;
  const totalPages = Math.ceil(totalRows / pageSize);
  const start = (page - 1) * pageSize;
  const end = start + pageSize;
  
  return {
    type: 'ROWS',
    rows: result.rows.slice(start, end),
    pagination: {
      page,
      pageSize,
      totalRows,
      totalPages,
      hasNext: page < totalPages,
      hasPrev: page > 1,
    },
  };
}
