// db-dump.js — Database dump/restore for HenryDB
// Serializes schema + data to SQL script format (like pg_dump).

/**
 * Dump a database to a SQL script string.
 * @param {Database} db - HenryDB instance
 * @param {Object} [options] - { schemaOnly, dataOnly, tables, dropExisting }
 * @returns {string} SQL script
 */
export function dump(db, options = {}) {
  const { schemaOnly = false, dataOnly = false, tables: filterTables, dropExisting = false } = options;
  const lines = [];
  
  lines.push('-- HenryDB Database Dump');
  lines.push(`-- Generated: ${new Date().toISOString()}`);
  lines.push('-- Format: SQL script');
  lines.push('');
  
  // Get tables
  let tables;
  try {
    const result = db.execute('SHOW TABLES');
    tables = result.rows.map(r => r.table_name || r.name || Object.values(r)[0]);
  } catch(e) {
    return `-- Error: ${e.message}`;
  }
  
  // Filter tables if specified
  if (filterTables) {
    tables = tables.filter(t => filterTables.includes(t));
  }
  
  // Skip internal tables
  tables = tables.filter(t => !t.startsWith('_'));
  
  if (!dataOnly) {
    lines.push('-- Schema');
    lines.push('');
    
    for (const table of tables) {
      if (dropExisting) {
        lines.push(`DROP TABLE IF EXISTS ${table};`);
      }
      
      // Get schema by peeking at data
      try {
        const peek = db.execute(`SELECT * FROM ${table} LIMIT 1`);
        if (peek.rows && peek.rows.length > 0) {
          const cols = Object.entries(peek.rows[0]).map(([name, val]) => {
            let type = 'TEXT';
            if (val !== null) {
              if (typeof val === 'number') type = Number.isInteger(val) ? 'INTEGER' : 'REAL';
              else if (typeof val === 'boolean') type = 'INTEGER';
            }
            const pk = name === 'id' ? ' PRIMARY KEY' : '';
            return `${name} ${type}${pk}`;
          });
          lines.push(`CREATE TABLE ${table} (${cols.join(', ')});`);
          lines.push('');
        } else {
          // Empty table — try to infer from LIMIT 0
          lines.push(`-- Table ${table} (empty, schema not available)`);
          lines.push('');
        }
      } catch(e) {
        lines.push(`-- Error dumping schema for ${table}: ${e.message}`);
        lines.push('');
      }
    }
  }
  
  if (!schemaOnly) {
    lines.push('-- Data');
    lines.push('');
    
    for (const table of tables) {
      try {
        const result = db.execute(`SELECT * FROM ${table}`);
        if (result.rows && result.rows.length > 0) {
          lines.push(`-- ${table}: ${result.rows.length} rows`);
          const cols = Object.keys(result.rows[0]);
          
          for (const row of result.rows) {
            const values = cols.map(col => {
              const val = row[col];
              if (val === null || val === undefined) return 'NULL';
              if (typeof val === 'number') return String(val);
              if (typeof val === 'boolean') return val ? '1' : '0';
              return `'${String(val).replace(/'/g, "''")}'`;
            });
            lines.push(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${values.join(', ')});`);
          }
          lines.push('');
        }
      } catch(e) {
        lines.push(`-- Error dumping data for ${table}: ${e.message}`);
      }
    }
  }
  
  lines.push('-- Dump complete');
  return lines.join('\n');
}

/**
 * Restore a database from a SQL script string.
 * @param {Database} db - HenryDB instance (should be empty/fresh)
 * @param {string} script - SQL dump script
 * @returns {{statements: number, errors: string[]}}
 */
export function restore(db, script) {
  // Remove comments and split by semicolons
  const cleaned = script.split('\n')
    .filter(line => !line.trim().startsWith('--'))
    .join('\n');
  
  const statements = cleaned
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);
  
  let executed = 0;
  const errors = [];
  
  for (const stmt of statements) {
    try {
      db.execute(stmt);
      executed++;
    } catch(e) {
      errors.push(`${e.message} — SQL: ${stmt.substring(0, 80)}`);
    }
  }
  
  return { statements: executed, errors };
}

/**
 * Clone a database (dump + restore).
 * @param {Database} source - Source database
 * @param {Database} target - Target database (should be empty)
 * @returns {{tables: number, rows: number, errors: string[]}}
 */
export function clone(source, target) {
  const script = dump(source);
  const result = restore(target, script);
  
  // Count what was cloned
  let tables = 0, rows = 0;
  try {
    tables = target.execute('SHOW TABLES').rows.length;
    for (const row of target.execute('SHOW TABLES').rows) {
      const name = row.table_name || row.name || Object.values(row)[0];
      if (!name.startsWith('_')) {
        rows += target.execute(`SELECT COUNT(*) as cnt FROM ${name}`).rows[0].cnt;
      }
    }
  } catch(e) {}
  
  return { tables, rows, errors: result.errors };
}
