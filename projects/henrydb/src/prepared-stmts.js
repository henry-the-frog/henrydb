// prepared-stmts.js — Extracted from db.js (2026-04-23)
// Prepared statement PREPARE/EXECUTE/DEALLOCATE handling

import { parse } from './sql.js';

/**
 * PREPARE name AS sql_with_placeholders
 * @param {object} db - Database instance
 * @param {string} sql - Full PREPARE SQL string
 * @returns {object} Result
 */
export function handlePrepare(db, sql) {
  const match = sql.match(/PREPARE\s+(\w+)\s+AS\s+(.*)/is);
  if (!match) throw new Error('Invalid PREPARE syntax. Use: PREPARE name AS sql');
  const name = match[1];
  const template = match[2].replace(/;$/, '').trim();
  const ast = parse(template);
  db._preparedStatements.set(name, { sql: template, ast });
  return { type: 'OK', message: `Prepared statement "${name}" created` };
}

/**
 * EXECUTE name (val1, val2, ...)
 * @param {object} db - Database instance
 * @param {string} sql - Full EXECUTE SQL string
 * @returns {object} Query result
 */
export function handleExecute(db, sql) {
  const match = sql.match(/EXECUTE\s+(\w+)\s*(?:\(([^)]*)\))?/is);
  if (!match) throw new Error('Invalid EXECUTE syntax. Use: EXECUTE name (val1, val2)');
  const name = match[1];
  const paramsStr = match[2] || '';
  
  const stmt = db._preparedStatements.get(name);
  if (!stmt) throw new Error(`Prepared statement "${name}" not found`);
  
  // Parse parameter values
  const params = paramsStr.split(',').map(p => p.trim()).filter(p => p);
  
  // Validate param count: find max $N in the SQL
  const paramRefs = [...stmt.sql.matchAll(/\$(\d+)/g)].map(m => parseInt(m[1]));
  const maxParam = paramRefs.length > 0 ? Math.max(...paramRefs) : 0;
  if (params.length < maxParam) {
    throw new Error(`Prepared statement "${name}" requires ${maxParam} parameters, got ${params.length}`);
  }
  
  // Substitute $1, $2, etc. in the SQL
  let resolved = stmt.sql;
  for (let i = 0; i < params.length; i++) {
    resolved = resolved.replace(new RegExp('\\$' + (i + 1), 'g'), params[i]);
  }
  
  return db.execute(resolved);
}

/**
 * DEALLOCATE name / DEALLOCATE ALL
 * @param {object} db - Database instance
 * @param {string} sql - Full DEALLOCATE SQL string
 * @returns {object} Result
 */
export function handleDeallocate(db, sql) {
  const match = sql.match(/DEALLOCATE\s+(\w+)/i);
  if (!match) throw new Error('Invalid DEALLOCATE syntax');
  const name = match[1].replace(/;$/, '');
  if (name.toUpperCase() === 'ALL') {
    db._preparedStatements.clear();
    return { type: 'OK', message: 'All prepared statements deallocated' };
  }
  if (!db._preparedStatements.delete(name)) {
    throw new Error(`Prepared statement "${name}" not found`);
  }
  return { type: 'OK', message: `Prepared statement "${name}" deallocated` };
}
