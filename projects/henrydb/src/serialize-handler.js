// serialize-handler.js — Extracted from db.js (2026-04-23)
// Database serialization, save, and bulk insert

/**
 * Serialize the entire database to a JSON-compatible object.
 * @param {object} db - Database instance
 * @returns {object} Serialized database state
 */
export function serialize(db) {
  const tables = {};
  for (const [name, table] of db.tables) {
    const rows = [];
    for (const { values } of table.heap.scan()) {
      rows.push(values);
    }
    tables[name] = {
      schema: table.schema,
      rows,
      indexes: [...table.indexes.keys()],
      indexMeta: table.indexMeta ? Object.fromEntries(table.indexMeta) : {},
    };
  }
  
  const views = {};
  for (const [name, view] of db.views) {
    views[name] = view;
  }
  
  const sequences = {};
  for (const [name, seq] of db.sequences) {
    sequences[name] = {
      current: seq.current,
      increment: seq.increment,
      min: seq.min,
      max: seq.max,
      cycle: seq.cycle,
      ownedBy: seq.ownedBy,
    };
  }
  
  const matViews = {};
  for (const [name, mv] of (db.materializedViews || new Map())) {
    matViews[name] = mv;
  }
  
  const comments = {};
  for (const [key, val] of (db._comments || new Map())) {
    comments[key] = val;
  }
  
  return {
    version: 1,
    tables,
    views,
    triggers: db.triggers,
    sequences,
    materializedViews: matViews,
    comments,
    indexCatalog: Object.fromEntries(db.indexCatalog),
  };
}

/**
 * Save database to a file (Node.js environments).
 * @param {object} db - Database instance
 * @param {string} path - File path to save to
 * @returns {object|string} Result or JSON string
 */
export function save(db, path) {
  const fs = globalThis.__fs || null;
  if (!fs) {
    return JSON.stringify(serialize(db));
  }
  fs.writeFileSync(path, JSON.stringify(serialize(db), null, 2));
  return { type: 'OK', message: `Database saved to ${path}` };
}

/**
 * Bulk insert rows without parsing SQL for each row.
 * @param {object} db - Database instance
 * @param {string} tableName - Target table
 * @param {Array<Array>} rows - Array of value arrays
 * @returns {object} Result with count
 */
export function bulkInsert(db, tableName, rows) {
  const table = db.tables.get(tableName);
  if (!table) throw new Error(`Table ${tableName} not found`);
  
  let inserted = 0;
  for (const values of rows) {
    db._insertRow(table, null, values);
    inserted++;
  }
  if (table.liveTupleCount !== undefined) table.liveTupleCount += inserted;
  return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
}
