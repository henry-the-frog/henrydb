// pg-dump.js — PostgreSQL-compatible database dump utility for HenryDB
// Generates SQL output compatible with pg_restore / psql import.

import { Database } from './db.js';

/**
 * Generate a pg_dump-compatible SQL dump of the database.
 * @param {Database} db — Database instance to dump
 * @param {object} options — Options
 * @returns {string} SQL dump
 */
export function pgDump(db, options = {}) {
  const {
    dataOnly = false,      // Only dump data, no DDL
    schemaOnly = false,    // Only dump schema, no data
    tables = null,         // Array of table names to dump (null = all)
    format = 'sql',        // 'sql' or 'copy'
    includeDrops = false,  // Include DROP TABLE statements
  } = options;

  const lines = [];
  
  // Header
  lines.push('--');
  lines.push('-- HenryDB Database Dump');
  lines.push(`-- Generated: ${new Date().toISOString()}`);
  lines.push('--');
  lines.push('');
  lines.push('SET statement_timeout = 0;');
  lines.push("SET client_encoding = 'UTF8';");
  lines.push('SET standard_conforming_strings = on;');
  lines.push('');

  // Get tables to dump
  const tablesToDump = tables ? tables : [...db.tables.keys()];

  for (const tableName of tablesToDump) {
    const tableObj = db.tables.get(tableName);
    if (!tableObj) continue;

    lines.push(`--`);
    lines.push(`-- Table: ${tableName}`);
    lines.push(`--`);
    lines.push('');

    // DROP TABLE (optional)
    if (includeDrops && !dataOnly) {
      lines.push(`DROP TABLE IF EXISTS ${tableName};`);
    }

    // CREATE TABLE
    if (!dataOnly) {
      const schema = tableObj.schema || [];
      const colDefs = schema.map(col => {
        const type = mapType(col.type);
        const constraints = [];
        if (col.primaryKey) constraints.push('PRIMARY KEY');
        if (col.notNull) constraints.push('NOT NULL');
        if (col.unique) constraints.push('UNIQUE');
        if (col.default !== undefined) constraints.push(`DEFAULT ${formatValue(col.default)}`);
        return `    ${col.name} ${type}${constraints.length ? ' ' + constraints.join(' ') : ''}`;
      });
      lines.push(`CREATE TABLE ${tableName} (`);
      lines.push(colDefs.join(',\n'));
      lines.push(');');
      lines.push('');
    }

    // Data
    if (!schemaOnly) {
      const result = db.execute(`SELECT * FROM ${tableName}`);
      if (result.type === 'ROWS' && result.rows.length > 0) {
        const columns = Object.keys(result.rows[0]);

        if (format === 'copy') {
          // COPY format (more efficient)
          lines.push(`COPY ${tableName} (${columns.join(', ')}) FROM stdin;`);
          for (const row of result.rows) {
            const values = columns.map(c => {
              const v = row[c];
              if (v === null || v === undefined) return '\\N';
              return String(v).replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n');
            });
            lines.push(values.join('\t'));
          }
          lines.push('\\.');
          lines.push('');
        } else {
          // INSERT format
          for (const row of result.rows) {
            const values = columns.map(c => formatValue(row[c]));
            lines.push(`INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`);
          }
          lines.push('');
        }
      }
    }

    // Indexes
    if (!dataOnly && tableObj.indexes) {
      for (const [indexName, index] of tableObj.indexes) {
        if (index.columns) {
          lines.push(`CREATE INDEX ${indexName} ON ${tableName} (${index.columns.join(', ')});`);
        }
      }
      lines.push('');
    }
  }

  // Footer
  lines.push('--');
  lines.push('-- Dump complete');
  lines.push('--');

  return lines.join('\n');
}

/**
 * Import a SQL dump into a database.
 * @param {Database} db — Target database
 * @param {string} sql — SQL dump content
 * @returns {object} Import stats
 */
export function pgRestore(db, sql) {
  const stats = { statements: 0, errors: 0, tables: 0, rows: 0 };
  
  // Split on semicolons and filter out comments/SET statements
  const rawStatements = sql.split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  let inCopy = false;
  let copyBuffer = '';
  let copyTable = '';
  let copyColumns = [];

  for (const stmt of rawStatements) {
    // Clean comments from within statements
    const cleanStmt = stmt.split('\n').filter(l => !l.trim().startsWith('--')).join('\n').trim();
    if (!cleanStmt) continue;
    
    // Skip SET statements
    if (cleanStmt.startsWith('SET ')) continue;


    if (inCopy) {
      // Process line by line looking for end marker
      const lines = cleanStmt.split('\n');
      for (const line of lines) {
        if (line.trim() === '\\.') {
          // End of COPY — process buffer
          const dataLines = copyBuffer.split('\n').filter(l => l.length > 0);
          for (const dataLine of dataLines) {
            const values = dataLine.split('\t').map(v => {
              if (v === '\\N') return 'NULL';
              const unescaped = v.replace(/\\n/g, '\n').replace(/\\t/g, '\t').replace(/\\\\/g, '\\');
              if (/^-?\d+(\.\d+)?$/.test(unescaped)) return unescaped;
              return `'${unescaped.replace(/'/g, "''")}'`;
            });
            try {
              db.execute(`INSERT INTO ${copyTable} (${copyColumns.join(', ')}) VALUES (${values.join(', ')})`);
              stats.rows++;
            } catch (e) {
              stats.errors++;
            }
          }
          inCopy = false;
          copyBuffer = '';
          break;
        } else if (line.trim()) {
          copyBuffer += line + '\n';
        }
      }
      continue;
    }

    // Check for COPY ... FROM stdin
    const copyMatch = cleanStmt.match(/COPY\s+(\w+)\s*\(([^)]+)\)\s+FROM\s+stdin/i);
    if (copyMatch) {
      copyTable = copyMatch[1];
      copyColumns = copyMatch[2].split(',').map(c => c.trim());
      
      // The COPY data might be in the same statement (after newline)
      // or in subsequent statements. Extract the data.
      const afterCopy = cleanStmt.substring(cleanStmt.indexOf('stdin') + 5).trim();
      if (afterCopy) {
        // Data is in the same chunk
        const lines = afterCopy.split('\n').filter(l => l.length > 0 && l !== '\\.');
        for (const line of lines) {
          const values = line.split('\t').map(v => {
            if (v === '\\N') return 'NULL';
            const unescaped = v.replace(/\\n/g, '\n').replace(/\\t/g, '\t').replace(/\\\\/g, '\\');
            if (/^-?\d+(\.\d+)?$/.test(unescaped)) return unescaped;
            return `'${unescaped.replace(/'/g, "''")}'`;
          });
          try {
            db.execute(`INSERT INTO ${copyTable} (${copyColumns.join(', ')}) VALUES (${values.join(', ')})`);
            stats.rows++;
          } catch (e) {
            stats.errors++;
          }
        }
      } else {
        inCopy = true;
      }
      continue;
    }

    try {
      const result = db.execute(cleanStmt);
      stats.statements++;
      if (cleanStmt.toUpperCase().startsWith('CREATE TABLE')) stats.tables++;
      if (cleanStmt.toUpperCase().startsWith('INSERT')) stats.rows++;
    } catch (e) {
      stats.errors++;
    }
  }

  return stats;
}

function mapType(type) {
  if (!type) return 'text';
  const t = type.toUpperCase();
  if (t === 'INTEGER' || t === 'INT') return 'integer';
  if (t === 'REAL' || t === 'FLOAT') return 'real';
  if (t === 'TEXT' || t === 'VARCHAR') return 'text';
  if (t === 'BOOLEAN' || t === 'BOOL') return 'boolean';
  return type.toLowerCase();
}

function formatValue(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return `'${String(v).replace(/'/g, "''")}'`;
}

export default { pgDump, pgRestore };
