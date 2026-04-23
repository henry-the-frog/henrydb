// checkpoint-handler.js — Extracted from db.js (2026-04-23)
// CHECKPOINT command handling

/**
 * Handle CHECKPOINT SQL command.
 * In the base Database class (no WAL), this reports table stats.
 * TransactionalDatabase overrides with real fuzzy checkpoint logic.
 * @param {object} db - Database instance
 * @returns {object} Checkpoint result
 */
export function handleCheckpoint(db) {
  const stats = {
    tables: db.tables.size,
    totalRows: 0,
  };
  for (const [, table] of db.tables) {
    if (table.heap && table.heap._pages) {
      for (const page of table.heap._pages) {
        stats.totalRows += page ? page.filter(Boolean).length : 0;
      }
    }
  }
  return {
    type: 'CHECKPOINT',
    message: `CHECKPOINT complete: ${stats.tables} tables, ${stats.totalRows} rows`,
    details: stats,
  };
}
