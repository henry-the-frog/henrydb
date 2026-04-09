// table-utils.js — Table manipulation utilities for HenryDB
// Copy, sample, describe, info

/**
 * Copy a table (structure + data or structure only).
 * @param {Database} db
 * @param {string} source - Source table name
 * @param {string} target - Target table name
 * @param {Object} options - { dataOnly, where }
 */
export function copyTable(db, source, target, options = {}) {
  const { dataOnly = false, where } = options;
  
  if (dataOnly) {
    // Just copy matching rows
    let sql = `INSERT INTO ${target} SELECT * FROM ${source}`;
    if (where) sql += ` WHERE ${where}`;
    return db.execute(sql);
  }
  
  // CREATE TABLE AS copies structure + data
  let sql = `CREATE TABLE ${target} AS SELECT * FROM ${source}`;
  if (where) sql += ` WHERE ${where}`;
  return db.execute(sql);
}

/**
 * Sample random rows from a table.
 * @param {Database} db
 * @param {string} table
 * @param {number} n - Number of rows to sample
 */
export function sampleTable(db, table, n = 10) {
  // Simple approach: get all, pick random subset
  const all = db.execute(`SELECT * FROM ${table}`);
  const rows = all.rows;
  if (rows.length <= n) return rows;
  
  // Fisher-Yates partial shuffle
  const result = [];
  const used = new Set();
  let attempts = 0;
  while (result.length < n && attempts < n * 3) {
    const idx = Math.floor(Math.random() * rows.length);
    if (!used.has(idx)) {
      used.add(idx);
      result.push(rows[idx]);
    }
    attempts++;
  }
  return result;
}

/**
 * Describe a table (like SQL DESCRIBE).
 */
export function describeTable(db, table) {
  const peek = db.execute(`SELECT * FROM ${table} LIMIT 1`);
  if (!peek.rows || peek.rows.length === 0) {
    return { table, columns: [] };
  }
  
  const columns = Object.entries(peek.rows[0]).map(([name, val]) => ({
    name,
    type: val === null ? 'UNKNOWN' : typeof val === 'number' ? 
      (Number.isInteger(val) ? 'INTEGER' : 'REAL') : 'TEXT',
    nullable: true,
    isPK: name === 'id',
  }));
  
  return { table, columns };
}

/**
 * Get table info (row count, column count, size estimate).
 */
export function tableInfo(db, table) {
  const count = db.execute(`SELECT COUNT(*) as cnt FROM ${table}`);
  const desc = describeTable(db, table);
  
  return {
    table,
    rowCount: count.rows[0].cnt,
    columnCount: desc.columns.length,
    columns: desc.columns.map(c => c.name),
    sizeEstimate: count.rows[0].cnt * desc.columns.length * 20,
  };
}

/**
 * Truncate a table (delete all rows).
 */
export function truncateTable(db, table) {
  return db.execute(`DELETE FROM ${table}`);
}

/**
 * Get top N rows by a column.
 */
export function topN(db, table, column, n = 10, order = 'DESC') {
  return db.execute(`SELECT * FROM ${table} ORDER BY ${column} ${order} LIMIT ${n}`);
}
