// index-advisor-impl.js — Extracted from db.js (2026-04-23)
// Index recommendation and auto-apply logic

import { parse } from './sql.js';

/**
 * Recommend indexes based on query workload analysis.
 * @param {object} db - Database instance
 * @returns {object} Result with recommended indexes
 */
export function recommendIndexes(db) {
  // Refresh index list to catch any manually created indexes
  db._indexAdvisor._existingIndexes = db._indexAdvisor._collectExistingIndexes();
  const recs = db._indexAdvisor.recommend();
  if (recs.length === 0) {
    return {
      type: 'ROWS',
      rows: [{ recommendation: 'No index recommendations. Run more queries to build workload profile.', impact: '', sql: '' }],
    };
  }
  return {
    type: 'ROWS',
    rows: recs.map(r => ({
      table: r.table,
      columns: r.columns.join(', '),
      impact: r.level,
      score: r.impact,
      costReduction: r.costReduction != null ? `${r.costReduction}%` : null,
      reason: r.reason,
      sql: r.sql,
    })),
  };
}

/**
 * Apply recommended indexes above a minimum impact level.
 * @param {object} db - Database instance
 * @param {string} minLevel - Minimum impact level ('low', 'medium', 'high')
 * @returns {object} Result with applied indexes
 */
export function applyRecommendedIndexes(db, minLevel = 'medium') {
  // Refresh index list to catch any manually created indexes
  db._indexAdvisor._existingIndexes = db._indexAdvisor._collectExistingIndexes();
  const recs = db._indexAdvisor.recommend();
  const levels = { high: 3, medium: 2, low: 1 };
  const minLevelVal = levels[minLevel] || 2;
  
  const toApply = recs.filter(r => (levels[r.level] || 0) >= minLevelVal);
  
  if (toApply.length === 0) {
    return {
      type: 'OK',
      message: 'No high/medium impact index recommendations to apply.',
      rows: [],
    };
  }
  
  const results = [];
  for (const rec of toApply) {
    try {
      db.execute_ast(parse(rec.sql));
      results.push({
        status: 'created',
        sql: rec.sql,
        impact: rec.level,
        costReduction: rec.costReduction != null ? `${rec.costReduction}%` : null,
      });
    } catch (e) {
      results.push({
        status: 'failed',
        sql: rec.sql,
        error: e.message,
      });
    }
  }
  
  // Refresh the advisor's index list
  db._indexAdvisor._existingIndexes = db._indexAdvisor._collectExistingIndexes();
  
  return {
    type: 'ROWS',
    rows: results,
    message: `Applied ${results.filter(r => r.status === 'created').length}/${toApply.length} recommended indexes`,
  };
}
