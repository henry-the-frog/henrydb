// schema-diff.js — Compare two database schemas and generate migration SQL
// Like pg_diff or sqldiff — finds added/removed/changed tables and columns.

/**
 * Compare two schemas and return differences.
 * @param {Object} source - Source schema { tableName: { colName: type } }
 * @param {Object} target - Target schema { tableName: { colName: type } }
 * @returns {Object} { added, removed, modified, sql }
 */
export function diffSchemas(source, target) {
  const result = {
    addedTables: [],
    removedTables: [],
    addedColumns: [],
    removedColumns: [],
    modifiedColumns: [],
    sql: [],
  };

  const srcTables = new Set(Object.keys(source));
  const tgtTables = new Set(Object.keys(target));

  // New tables
  for (const table of tgtTables) {
    if (!srcTables.has(table)) {
      result.addedTables.push(table);
      const cols = Object.entries(target[table])
        .map(([name, type]) => `${name} ${type}`)
        .join(', ');
      result.sql.push(`CREATE TABLE ${table} (${cols});`);
    }
  }

  // Removed tables
  for (const table of srcTables) {
    if (!tgtTables.has(table)) {
      result.removedTables.push(table);
      result.sql.push(`DROP TABLE ${table};`);
    }
  }

  // Modified tables (column changes)
  for (const table of srcTables) {
    if (!tgtTables.has(table)) continue;
    const srcCols = source[table];
    const tgtCols = target[table];
    const srcColNames = new Set(Object.keys(srcCols));
    const tgtColNames = new Set(Object.keys(tgtCols));

    // New columns
    for (const col of tgtColNames) {
      if (!srcColNames.has(col)) {
        result.addedColumns.push({ table, column: col, type: tgtCols[col] });
        result.sql.push(`ALTER TABLE ${table} ADD COLUMN ${col} ${tgtCols[col]};`);
      }
    }

    // Removed columns
    for (const col of srcColNames) {
      if (!tgtColNames.has(col)) {
        result.removedColumns.push({ table, column: col, type: srcCols[col] });
        result.sql.push(`ALTER TABLE ${table} DROP COLUMN ${col};`);
      }
    }

    // Modified columns (type change)
    for (const col of srcColNames) {
      if (tgtColNames.has(col) && srcCols[col] !== tgtCols[col]) {
        result.modifiedColumns.push({
          table, column: col,
          from: srcCols[col], to: tgtCols[col],
        });
        result.sql.push(`-- ALTER TABLE ${table} ALTER COLUMN ${col} TYPE ${tgtCols[col]}; (type change: ${srcCols[col]} → ${tgtCols[col]})`);
      }
    }
  }

  return result;
}

/**
 * Extract schema from a Database instance.
 * @param {Database} db
 * @returns {Object} { tableName: { colName: type } }
 */
export function extractSchemaMap(db) {
  const schema = {};
  try {
    const tables = db.execute('SHOW TABLES');
    for (const row of tables.rows) {
      const name = row.table_name || row.name || Object.values(row)[0];
      if (name.startsWith('_')) continue;
      schema[name] = {};
      try {
        const peek = db.execute(`SELECT * FROM ${name} LIMIT 1`);
        if (peek.rows && peek.rows.length > 0) {
          for (const [col, val] of Object.entries(peek.rows[0])) {
            schema[name][col] = val === null ? 'TEXT' :
              typeof val === 'number' ? (Number.isInteger(val) ? 'INTEGER' : 'REAL') : 'TEXT';
          }
        }
      } catch(e) {}
    }
  } catch(e) {}
  return schema;
}

/**
 * Compare two databases and generate migration SQL.
 */
export function diffDatabases(source, target) {
  return diffSchemas(extractSchemaMap(source), extractSchemaMap(target));
}
