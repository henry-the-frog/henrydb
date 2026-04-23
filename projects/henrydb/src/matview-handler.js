// matview-handler.js — Extracted from db.js (2026-04-23)
// CREATE MATERIALIZED VIEW and REFRESH MATERIALIZED VIEW

/**
 * Create a materialized view by executing the query and storing results.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed CREATE MATVIEW AST
 * @returns {object} Result
 */
export function createMatView(db, ast) {
  const result = db._select(ast.query);
  
  if (result.rows.length === 0) {
    db.views.set(ast.name, { query: ast.query, materializedRows: [], isMaterialized: true });
    return { type: 'OK', message: `Materialized view ${ast.name} created (empty)` };
  }

  const firstRow = result.rows[0];
  const schema = Object.keys(firstRow).filter(k => !k.includes('.')).map(name => ({
    name,
    type: typeof firstRow[name] === 'number' ? 'INT' : 'TEXT',
    primaryKey: false,
  }));

  const heap = db._heapFactory(ast.name);
  const indexes = new Map();
  const tableObj = { schema, heap, indexes };
  db.tables.set(ast.name, tableObj);

  for (const row of result.rows) {
    const values = schema.map(col => row[col.name]);
    db._insertRow(tableObj, null, values);
  }

  db.views.set(ast.name, { query: ast.query, isMaterialized: true });
  return { type: 'OK', message: `Materialized view ${ast.name} created with ${result.rows.length} rows` };
}

/**
 * Refresh a materialized view by re-executing its query.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed REFRESH MATVIEW AST
 * @returns {object} Result
 */
export function refreshMatView(db, ast) {
  const viewDef = db.views.get(ast.name);
  if (!viewDef || !viewDef.isMaterialized) {
    throw new Error(`${ast.name} is not a materialized view`);
  }

  const result = db._select(viewDef.query);
  
  const table = db.tables.get(ast.name);
  if (table) {
    if (table.heap.truncate) {
      table.heap.truncate();
    } else {
      table.heap = db._heapFactory(ast.name);
    }
    
    for (const row of result.rows) {
      const values = table.schema.map(col => row[col.name]);
      db._insertRow(table, null, values);
    }
  }

  if (db._resultCache) db._resultCache.clear();
  return { type: 'OK', message: `Materialized view ${ast.name} refreshed with ${result.rows.length} rows` };
}
