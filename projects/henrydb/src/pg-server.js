// pg-server.js — PostgreSQL wire protocol v3 server for HenryDB
// Usage: node pg-server.js [--port 5433] [--dir ./data]
// Connect: psql -h localhost -p 5433 -U henrydb

import { createServer } from 'node:net';
import { Database } from './db.js';

// --- PG Type OIDs ---
const PG_OIDS = {
  INT: 23,        // int4
  INTEGER: 23,    // int4
  INT4: 23,       // int4
  SMALLINT: 21,   // int2
  INT2: 21,       // int2
  FLOAT: 701,     // float8
  FLOAT4: 700,    // float4
  FLOAT8: 701,    // float8
  DOUBLE: 701,    // float8
  REAL: 700,      // float4
  NUMERIC: 1700,  // numeric
  DECIMAL: 1700,  // numeric
  TEXT: 25,       // text
  VARCHAR: 1043,  // varchar
  CHAR: 18,       // char
  BOOLEAN: 16,    // bool
  BOOL: 16,       // bool
  DATE: 1082,     // date
  TIMESTAMP: 1114, // timestamp
  SERIAL: 23,     // serial → int4
  BIGINT: 20,     // int8
  INT8: 20,       // int8
  BIGSERIAL: 20,  // bigserial → int8
  JSON: 114,      // json
  JSONB: 3802,    // jsonb
  UNKNOWN: 0,
};

function typeToOid(type) {
  if (!type) return PG_OIDS.TEXT;
  const upper = type.toUpperCase();
  return PG_OIDS[upper] || PG_OIDS.TEXT;
}

// --- Buffer helpers ---
function writeInt32BE(buf, val, offset) {
  buf[offset] = (val >>> 24) & 0xff;
  buf[offset + 1] = (val >>> 16) & 0xff;
  buf[offset + 2] = (val >>> 8) & 0xff;
  buf[offset + 3] = val & 0xff;
}

function readInt32BE(buf, offset) {
  return (buf[offset] << 24) | (buf[offset + 1] << 16) | (buf[offset + 2] << 8) | buf[offset + 3];
}

function readInt16BE(buf, offset) {
  return (buf[offset] << 8) | buf[offset + 1];
}

// --- Message builders ---
function authOk() {
  const buf = Buffer.alloc(9);
  buf[0] = 0x52; // 'R'
  writeInt32BE(buf, 8, 1); // length
  writeInt32BE(buf, 0, 5); // AuthenticationOk
  return buf;
}

function parameterStatus(name, value) {
  const nameBytes = Buffer.from(name + '\0', 'utf8');
  const valBytes = Buffer.from(value + '\0', 'utf8');
  const len = 4 + nameBytes.length + valBytes.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x53; // 'S'
  writeInt32BE(buf, len, 1);
  nameBytes.copy(buf, 5);
  valBytes.copy(buf, 5 + nameBytes.length);
  return buf;
}

function readyForQuery(txStatus = 'I') {
  const buf = Buffer.alloc(6);
  buf[0] = 0x5a; // 'Z'
  writeInt32BE(buf, 5, 1); // length
  buf[5] = txStatus.charCodeAt(0); // 'I'=idle, 'T'=in transaction, 'E'=error
  return buf;
}

function rowDescription(columns) {
  // Each field: name\0 + tableOid(4) + colAttrNum(2) + typeOid(4) + typeLen(2) + typeMod(4) + format(2) = 18 bytes + name
  let size = 4 + 2; // length + field count
  const fields = columns.map(col => {
    const nameBytes = Buffer.from(col.name + '\0', 'utf8');
    size += nameBytes.length + 18;
    return { nameBytes, oid: typeToOid(col.type) };
  });

  const buf = Buffer.alloc(1 + size);
  let offset = 0;
  buf[offset++] = 0x54; // 'T'
  writeInt32BE(buf, size, offset); offset += 4;
  buf[offset++] = (columns.length >> 8) & 0xff;
  buf[offset++] = columns.length & 0xff;

  for (const field of fields) {
    field.nameBytes.copy(buf, offset); offset += field.nameBytes.length;
    writeInt32BE(buf, 0, offset); offset += 4; // table OID
    buf[offset++] = 0; buf[offset++] = 0; // column attr number
    writeInt32BE(buf, field.oid, offset); offset += 4; // type OID
    buf[offset++] = 0xff; buf[offset++] = 0xff; // type size (-1 = variable)
    writeInt32BE(buf, -1, offset); offset += 4; // type modifier
    buf[offset++] = 0; buf[offset++] = 0; // format code (0=text)
  }

  return buf;
}

function dataRow(values) {
  // Each column: int32 length + data (or -1 for null)
  let size = 4 + 2; // length + column count
  const encoded = values.map(v => {
    if (v === null || v === undefined) return null;
    const str = String(v);
    const bytes = Buffer.from(str, 'utf8');
    size += 4 + bytes.length;
    return bytes;
  });
  // Null columns: just int32 -1
  for (const v of encoded) {
    if (v === null) size += 4;
  }

  const buf = Buffer.alloc(1 + size);
  let offset = 0;
  buf[offset++] = 0x44; // 'D'
  writeInt32BE(buf, size, offset); offset += 4;
  buf[offset++] = (values.length >> 8) & 0xff;
  buf[offset++] = values.length & 0xff;

  for (const bytes of encoded) {
    if (bytes === null) {
      writeInt32BE(buf, -1, offset); offset += 4;
    } else {
      writeInt32BE(buf, bytes.length, offset); offset += 4;
      bytes.copy(buf, offset); offset += bytes.length;
    }
  }

  return buf;
}

function commandComplete(tag) {
  const tagBytes = Buffer.from(tag + '\0', 'utf8');
  const len = 4 + tagBytes.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x43; // 'C'
  writeInt32BE(buf, len, 1);
  tagBytes.copy(buf, 5);
  return buf;
}

function errorResponse(severity, code, message) {
  // Fields: S=severity, V=severity(verbose), C=code, M=message, terminated by \0
  const fields = [
    'S' + severity + '\0',
    'V' + severity + '\0',
    'C' + code + '\0',
    'M' + message + '\0',
    '\0', // terminator
  ];
  const body = Buffer.from(fields.join(''), 'utf8');
  const len = 4 + body.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x45; // 'E'
  writeInt32BE(buf, len, 1);
  body.copy(buf, 5);
  return buf;
}

function noticeResponse(message) {
  const fields = [
    'SNOTICE\0',
    'VNOTICE\0',
    'C00000\0',
    'M' + message + '\0',
    '\0',
  ];
  const body = Buffer.from(fields.join(''), 'utf8');
  const len = 4 + body.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x4e; // 'N'
  writeInt32BE(buf, len, 1);
  body.copy(buf, 5);
  return buf;
}

function emptyQueryResponse() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x49; // 'I'
  writeInt32BE(buf, 4, 1);
  return buf;
}

// --- Command tag from result ---
function getCommandTag(sql, result) {
  const cmd = sql.trim().split(/\s+/)[0].toUpperCase();
  switch (cmd) {
    case 'SELECT': return `SELECT ${result.rows?.length || 0}`;
    case 'INSERT': return `INSERT 0 ${result.rowCount || result.changes || 1}`;
    case 'UPDATE': return `UPDATE ${result.rowCount || result.changes || 0}`;
    case 'DELETE': return `DELETE ${result.rowCount || result.changes || 0}`;
    case 'CREATE': return 'CREATE TABLE';
    case 'DROP': return 'DROP TABLE';
    case 'ALTER': return 'ALTER TABLE';
    case 'BEGIN': return 'BEGIN';
    case 'COMMIT': return 'COMMIT';
    case 'ROLLBACK': return 'ROLLBACK';
    case 'EXPLAIN': return `SELECT ${result.rows?.length || 0}`;
    case 'ANALYZE': return 'ANALYZE';
    default: return cmd;
  }
}

// --- Column info from result ---
function inferTypeFromValue(val) {
  if (val === null || val === undefined) return 'TEXT';
  if (typeof val === 'number') return Number.isInteger(val) ? 'INTEGER' : 'FLOAT';
  if (typeof val === 'boolean') return 'BOOLEAN';
  return 'TEXT';
}

function getColumns(result, db, sql) {
  if (result.columns && result.columns.length > 0) {
    // Try to infer types from table schema for string-only columns
    let schema = null;
    if (sql && db) {
      const tableMatch = sql.match(/FROM\s+(\w+)/i);
      if (tableMatch && db.tables) {
        const table = db.tables.get(tableMatch[1]) || db.tables.get(tableMatch[1].toLowerCase());
        if (table) schema = table.schema;
      }
    }
    return result.columns.map((c, i) => {
      if (typeof c === 'string') {
        if (schema) {
          const col = schema.find(sc => sc.name === c || sc.name === c.toLowerCase());
          if (col) return { name: c, type: col.type || 'TEXT' };
        }
        // Infer from first row if available
        if (result.rows && result.rows.length > 0) {
          return { name: c, type: inferTypeFromValue(result.rows[0][c]) };
        }
        return { name: c, type: 'TEXT' };
      }
      return c;
    });
  }
  if (result.rows && result.rows.length > 0) {
    // Try to infer types from table schema
    let schema = null;
    if (sql && db) {
      const tableMatch = sql.match(/FROM\s+(\w+)/i);
      if (tableMatch && db.tables) {
        const table = db.tables.get(tableMatch[1]) || db.tables.get(tableMatch[1].toLowerCase());
        if (table) schema = table.schema;
      }
    }
    return Object.keys(result.rows[0]).map(name => {
      if (schema) {
        const col = schema.find(c => c.name === name || c.name === name.toLowerCase());
        if (col) return { name, type: col.type || 'TEXT' };
      }
      // Infer type from actual values
      return { name, type: inferTypeFromValue(result.rows[0][name]) };
    });
  }
  // Try to infer columns from SQL for SELECT * FROM table
  if (sql && db) {
    const m = sql.match(/SELECT\s+\*\s+FROM\s+(\w+)/i);
    if (m) {
      const table = db.tables.get(m[1]) || db.tables.get(m[1].toLowerCase());
      if (table && table.schema) {
        return table.schema.map(c => ({ name: c.name, type: c.type || 'TEXT' }));
      }
    }
    // Use parseSelectColumns for expressions, aliases, functions
    const columnNames = parseSelectColumns(sql);
    if (columnNames.length > 0) {
      // Try to find table and infer types
      const tableMatch = sql.match(/FROM\s+(\w+)/i);
      let schema = null;
      if (tableMatch && db.tables) {
        const table = db.tables.get(tableMatch[1]) || db.tables.get(tableMatch[1].toLowerCase());
        if (table) schema = table.schema;
      }
      return columnNames.map(n => {
        if (schema) {
          const col = schema.find(c => c.name === n || c.name === n.toLowerCase());
          if (col) return { name: n, type: col.type || 'TEXT' };
        }
        return { name: n, type: 'TEXT' };
      });
    }
  }
  return [];
}

// --- Parse column names/aliases from SELECT statement ---
function parseSelectColumns(sql) {
  // Extract: SELECT col1, col2 as alias, func(x) as name FROM ...
  const match = sql.match(/SELECT\s+(.*?)\s+FROM\s/i);
  if (!match) return [];
  
  const selectList = match[1];
  if (selectList.trim() === '*') return []; // Can't determine without table
  
  const columns = [];
  // Split by commas (handle nested parentheses)
  let depth = 0, start = 0;
  for (let i = 0; i <= selectList.length; i++) {
    if (i < selectList.length && selectList[i] === '(') depth++;
    if (i < selectList.length && selectList[i] === ')') depth--;
    if ((i === selectList.length || (selectList[i] === ',' && depth === 0))) {
      const part = selectList.slice(start, i).trim();
      start = i + 1;
      
      // Check for AS alias
      const asMatch = part.match(/\bAS\s+(\w+)\s*$/i);
      if (asMatch) {
        columns.push(asMatch[1]);
      } else {
        // Use the last identifier (column name or function result)
        const idMatch = part.match(/(\w+)\s*$/);
        if (idMatch) {
          columns.push(idMatch[1]);
        }
      }
    }
  }
  return columns;
}

// --- pg_catalog query interceptor for psql/client compatibility ---
function interceptPgCatalog(sql, db) {
  const upper = sql.trim().toUpperCase();
  
  // psql startup: SET client_encoding, DateStyle, etc
  if (upper.startsWith('SET ')) {
    // Check for cost model parameter SET
    const setMatch = sql.match(/^SET\s+(\w+)\s*(?:=|TO)\s*['"]?([^'";\s]+)/i);
    if (setMatch) {
      const param = setMatch[1].toLowerCase();
      const value = parseFloat(setMatch[2]);
      
      const costParams = ['seq_page_cost', 'random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost', 'cpu_operator_cost'];
      if (costParams.includes(param) && !isNaN(value) && db.constructor?.COST_MODEL) {
        db.constructor.COST_MODEL[param] = value;
        return { type: 'OK', message: 'SET' };
      }
    }
    return { type: 'OK', message: 'SET' };
  }
  
  // SHOW TABLES — list all tables with metadata
  if (upper === 'SHOW TABLES' || upper === 'SHOW TABLES;') {
    const tables = [];
    if (db.tables) {
      for (const [name, table] of db.tables) {
        const rowCount = table.heap?.rowCount ?? '?';
        const colCount = table.schema?.length ?? 0;
        const indexCount = table.indexes?.size ?? 0;
        tables.push({
          table_name: name,
          columns: colCount,
          rows: rowCount,
          indexes: indexCount,
        });
      }
    }
    return { type: 'ROWS', rows: tables };
  }
  
  // SHOW INDEXES — list all indexes across all tables
  const showIdxMatch = upper.match(/^SHOW\s+INDEXES?\s*(?:FROM\s+(\w+))?/);
  if (showIdxMatch) {
    const filterTable = showIdxMatch[1]?.toLowerCase();
    const indexes = [];
    if (db.tables) {
      for (const [tableName, table] of db.tables) {
        if (filterTable && tableName !== filterTable) continue;
        if (table.indexes) {
          for (const [colName, idx] of table.indexes) {
            indexes.push({
              table_name: tableName,
              index_name: `idx_${tableName}_${colName}`,
              column_name: colName,
              unique: table.schema?.find(c => c.name === colName)?.primaryKey ? 'YES' : (table.schema?.find(c => c.name === colName)?.unique ? 'YES' : 'NO'),
              primary: table.schema?.find(c => c.name === colName)?.primaryKey ? 'YES' : 'NO',
            });
          }
        }
      }
    }
    return { type: 'ROWS', rows: indexes };
  }
  
  // SHOW CREATE TABLE tablename
  const showCreateMatch = upper.match(/^SHOW\s+CREATE\s+TABLE\s+(\w+)/);
  if (showCreateMatch) {
    const tableName = showCreateMatch[1].toLowerCase();
    const table = db.tables?.get(tableName);
    if (table && table.schema) {
      const cols = table.schema.map(col => {
        let def = `  ${col.name} ${(col.type || 'TEXT').toUpperCase()}`;
        if (col.primaryKey) def += ' PRIMARY KEY';
        else if (col.unique) def += ' UNIQUE';
        if (col.notNull) def += ' NOT NULL';
        if (col.defaultValue != null) def += ` DEFAULT ${col.defaultValue}`;
        return def;
      });
      const createSql = `CREATE TABLE ${tableName} (\n${cols.join(',\n')}\n)`;
      return { type: 'ROWS', rows: [{ table_name: tableName, create_statement: createSql }] };
    }
    return { type: 'ROWS', rows: [] };
  }
  
  // psql: SHOW search_path / server_version etc
  if (upper.startsWith('SHOW ')) {
    const param = upper.replace('SHOW ', '').replace(';', '').trim();
    
    if (param === 'ALL') {
      // Return all settings including cost model
      const C = db.constructor?.COST_MODEL || {};
      const settings = [
        { name: 'server_version', setting: '14.0 (HenryDB)', description: 'Server version' },
        { name: 'server_encoding', setting: 'UTF8', description: 'Server encoding' },
        { name: 'client_encoding', setting: 'UTF8', description: 'Client encoding' },
        { name: 'search_path', setting: '"$user", public', description: 'Schema search path' },
        { name: 'seq_page_cost', setting: String(C.seq_page_cost || 1.0), description: 'Sequential page fetch cost' },
        { name: 'random_page_cost', setting: String(C.random_page_cost || 1.1), description: 'Random page fetch cost' },
        { name: 'cpu_tuple_cost', setting: String(C.cpu_tuple_cost || 0.01), description: 'Per-tuple processing cost' },
        { name: 'cpu_index_tuple_cost', setting: String(C.cpu_index_tuple_cost || 0.005), description: 'Per-index-tuple processing cost' },
        { name: 'cpu_operator_cost', setting: String(C.cpu_operator_cost || 0.0025), description: 'Per-operator evaluation cost' },
        { name: 'page_size', setting: '32768', description: 'Page size in bytes' },
      ];
      return { type: 'ROWS', rows: settings };
    }
    
    switch (param) {
      case 'SERVER_VERSION':
        return { type: 'ROWS', rows: [{ server_version: '14.0 (HenryDB)' }] };
      case 'SERVER_ENCODING':
        return { type: 'ROWS', rows: [{ server_encoding: 'UTF8' }] };
      case 'CLIENT_ENCODING':
        return { type: 'ROWS', rows: [{ client_encoding: 'UTF8' }] };
      case 'SEARCH_PATH':
        return { type: 'ROWS', rows: [{ search_path: '"$user", public' }] };
      case 'STANDARD_CONFORMING_STRINGS':
        return { type: 'ROWS', rows: [{ standard_conforming_strings: 'on' }] };
      case 'IS_SUPERUSER':
        return { type: 'ROWS', rows: [{ is_superuser: 'on' }] };
      default:
        // Check cost model parameters
        const costParams = ['SEQ_PAGE_COST', 'RANDOM_PAGE_COST', 'CPU_TUPLE_COST', 'CPU_INDEX_TUPLE_COST', 'CPU_OPERATOR_COST'];
        if (costParams.includes(param)) {
          const C = db.constructor?.COST_MODEL || {};
          const val = C[param.toLowerCase()];
          return { type: 'ROWS', rows: [{ [param.toLowerCase()]: val != null ? String(val) : '' }] };
        }
        return { type: 'ROWS', rows: [{ [param.toLowerCase()]: '' }] };
    }
  }
  
  // psql: version()
  if (upper.includes('VERSION()')) {
    return { type: 'ROWS', rows: [{ version: 'HenryDB 0.1.0 on Node.js' }] };
  }
  
  // pg_catalog.pg_type query (common on connection)
  if (upper.includes('PG_TYPE') || upper.includes('PG_CATALOG')) {
    // Return empty result set for catalog queries
    return { type: 'ROWS', rows: [] };
  }
  
  // DESCRIBE table (MySQL-style, also common in tools)
  const describeMatch = upper.match(/^DESCRIBE\s+(\w+)/);
  
  if (describeMatch) {
    const tableName = describeMatch[1].toLowerCase();
    const table = db.tables?.get(tableName);
    if (table && table.schema) {
      const rows = table.schema.map(col => ({
        column_name: col.name,
        data_type: (col.type || 'TEXT').toUpperCase(),
        nullable: col.primaryKey ? 'NO' : 'YES',
        key: col.primaryKey ? 'PRI' : (col.unique ? 'UNI' : ''),
        default_value: col.defaultValue != null ? String(col.defaultValue) : null,
      }));
      return { type: 'ROWS', rows };
    }
    return { type: 'ROWS', rows: [] };
  }
  
  // current_schema() / current_database() / current_user
  if (upper.includes('CURRENT_SCHEMA')) {
    return { type: 'ROWS', rows: [{ current_schema: 'public' }] };
  }
  if (upper.includes('CURRENT_DATABASE')) {
    return { type: 'ROWS', rows: [{ current_database: 'henrydb' }] };
  }
  if (upper.includes('CURRENT_USER') || upper.includes('SESSION_USER')) {
    return { type: 'ROWS', rows: [{ current_user: 'henrydb' }] };
  }
  
  // \\dt (list tables) — psql sends query to pg_catalog
  // We handle it by returning table info from the database
  if (upper.includes('PG_CLASS') && upper.includes('RELKIND')) {
    const tables = [];
    if (db.tables) {
      for (const [name, table] of db.tables) {
        tables.push({
          schemaname: 'public',
          tablename: name,
          tableowner: 'henrydb',
          tablespace: null,
        });
      }
    }
    return { type: 'ROWS', rows: tables };
  }
  
  // information_schema.tables
  if (upper.includes('INFORMATION_SCHEMA') && upper.includes('TABLES')) {
    const tables = [];
    if (db.tables) {
      for (const [name] of db.tables) {
        tables.push({
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: name,
          table_type: 'BASE TABLE',
        });
      }
    }
    return { type: 'ROWS', rows: tables };
  }
  
  // information_schema.columns
  if (upper.includes('INFORMATION_SCHEMA') && upper.includes('COLUMNS')) {
    const columns = [];
    if (db.tables) {
      for (const [tableName, table] of db.tables) {
        if (table.schema) {
          table.schema.forEach((col, i) => {
            columns.push({
              table_catalog: 'henrydb',
              table_schema: 'public',
              table_name: tableName,
              column_name: col.name,
              ordinal_position: i + 1,
              data_type: (col.type || 'TEXT').toUpperCase(),
              is_nullable: col.primaryKey ? 'NO' : 'YES',
              column_default: col.defaultValue != null ? String(col.defaultValue) : null,
            });
          });
        }
      }
    }
    return { type: 'ROWS', rows: columns };
  }
  
  return null; // Not intercepted
}

function handleConnection(socket, db, connId = 0) {
  let buffer = Buffer.alloc(0);
  let startupDone = false;

  // Connection-scoped cursor state
  const cursors = new Map(); // name → { rows, columns, pos }

  function executeWithIntercept(sql) {
    // Advisory lock functions
    const advisoryMatch = sql.match(/SELECT\s+pg_(advisory_lock|advisory_unlock|try_advisory_lock)\s*\(\s*(\d+)\s*\)/i);
    if (advisoryMatch) {
      const func = advisoryMatch[1].toLowerCase();
      const key = parseInt(advisoryMatch[2], 10);
      const colName = 'pg_' + func;
      
      if (func === 'advisory_lock') {
        _advisoryLocks.lock(connId, key);
        return { type: 'ROWS', rows: [{ [colName]: '' }], columns: [{ name: colName, type: 'TEXT' }] };
      } else if (func === 'try_advisory_lock') {
        const success = _advisoryLocks.tryLock(connId, key);
        return { type: 'ROWS', rows: [{ [colName]: success ? 't' : 'f' }], columns: [{ name: colName, type: 'TEXT' }] };
      } else if (func === 'advisory_unlock') {
        const success = _advisoryLocks.unlock(connId, key);
        return { type: 'ROWS', rows: [{ [colName]: success ? 't' : 'f' }], columns: [{ name: colName, type: 'TEXT' }] };
      }
    }
    
    // DECLARE cursor
    const declareMatch = sql.match(/DECLARE\s+(\w+)\s+CURSOR\s+FOR\s+(.*)/is);
    if (declareMatch) {
      const name = declareMatch[1].toLowerCase();
      const query = declareMatch[2].trim();
      const result = db.execute(query);
      const columns = result.rows?.length > 0 ? Object.keys(result.rows[0]) : [];
      cursors.set(name, { 
        rows: result.rows || [], 
        columns: columns,
        pos: 0 
      });
      return { type: 'COMMAND', command: 'DECLARE CURSOR' };
    }
    
    // FETCH [ALL|NEXT|FORWARD N|N] FROM cursor
    const fetchMatch = sql.match(/FETCH\s+(ALL|NEXT|FORWARD\s+(\d+)|(\d+))\s+FROM\s+(\w+)/i);
    if (fetchMatch) {
      const cursorName = fetchMatch[4].toLowerCase();
      const cursor = cursors.get(cursorName);
      if (!cursor) throw new Error(`Cursor "${cursorName}" not found`);
      
      let count;
      if (fetchMatch[1].toUpperCase() === 'ALL') {
        count = cursor.rows.length - cursor.pos;
      } else if (fetchMatch[1].toUpperCase() === 'NEXT') {
        count = 1;
      } else if (fetchMatch[2]) {
        count = parseInt(fetchMatch[2], 10); // FORWARD N
      } else {
        count = parseInt(fetchMatch[3], 10); // bare N
      }
      
      const rows = cursor.rows.slice(cursor.pos, cursor.pos + count);
      cursor.pos += count;
      return { type: 'ROWS', rows };
    }
    
    // MOVE N FROM cursor
    const moveMatch = sql.match(/MOVE\s+(\d+)\s+FROM\s+(\w+)/i);
    if (moveMatch) {
      const count = parseInt(moveMatch[1], 10);
      const cursorName = moveMatch[2].toLowerCase();
      const cursor = cursors.get(cursorName);
      if (!cursor) throw new Error(`Cursor "${cursorName}" not found`);
      cursor.pos += count;
      return { type: 'COMMAND', command: `MOVE ${count}` };
    }
    
    // CLOSE ALL
    if (/^CLOSE\s+ALL\s*$/i.test(sql.trim())) {
      cursors.clear();
      return { type: 'COMMAND', command: 'CLOSE ALL' };
    }
    
    // CLOSE cursor
    const closeMatch = sql.match(/^CLOSE\s+(\w+)\s*$/i);
    if (closeMatch) {
      const cursorName = closeMatch[1].toLowerCase();
      cursors.delete(cursorName);
      return { type: 'COMMAND', command: 'CLOSE CURSOR' };
    }
    
    const intercepted = interceptPgCatalog(sql, db);
    if (intercepted) return intercepted;
    return db.execute(sql);
  }
  let inTransaction = false;
  const preparedStatements = new Map(); // name → { sql, paramTypes }
  const portals = new Map(); // name → { sql (with params substituted), result (if already executed) }
  let copyState = null; // { tableName, columns, rows: [] }

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    processBuffer();
  });

  socket.on('error', () => { _advisoryLocks.releaseAll(connId); });
  socket.on('close', () => { _advisoryLocks.releaseAll(connId); });

  function processBuffer() {
    while (buffer.length >= 4) {
      if (!startupDone) {
        // Startup message: int32 length + int32 version + key=val pairs
        if (buffer.length < 4) return;
        const len = readInt32BE(buffer, 0);
        if (buffer.length < len) return;

        const version = readInt32BE(buffer, 4);

        // SSLRequest (version = 80877103)
        if (version === 80877103) {
          socket.write(Buffer.from('N')); // No SSL
          buffer = buffer.slice(len);
          continue;
        }

        // CancelRequest (version = 80877102)
        if (version === 80877102) {
          buffer = buffer.slice(len);
          continue;
        }

        // Parse params
        const paramBuf = buffer.slice(8, len);
        const params = {};
        let i = 0;
        while (i < paramBuf.length) {
          if (paramBuf[i] === 0) break;
          const keyEnd = paramBuf.indexOf(0, i);
          if (keyEnd < 0) break;
          const key = paramBuf.slice(i, keyEnd).toString('utf8');
          const valEnd = paramBuf.indexOf(0, keyEnd + 1);
          if (valEnd < 0) break;
          const val = paramBuf.slice(keyEnd + 1, valEnd).toString('utf8');
          params[key] = val;
          i = valEnd + 1;
        }

        buffer = buffer.slice(len);
        startupDone = true;

        // Send auth + params + ready
        socket.write(authOk());
        socket.write(parameterStatus('server_version', '14.0 (HenryDB)'));
        socket.write(parameterStatus('server_encoding', 'UTF8'));
        socket.write(parameterStatus('client_encoding', 'UTF8'));
        socket.write(parameterStatus('DateStyle', 'ISO, MDY'));
        socket.write(parameterStatus('integer_datetimes', 'on'));
        socket.write(readyForQuery('I'));
        continue;
      }

      // Regular message: byte type + int32 length + payload
      if (buffer.length < 5) return;
      const msgType = buffer[0];
      const msgLen = readInt32BE(buffer, 1);
      const totalLen = 1 + msgLen;
      if (buffer.length < totalLen) return;

      const payload = buffer.slice(5, totalLen);
      buffer = buffer.slice(totalLen);

      switch (msgType) {
        case 0x51: // 'Q' — Simple Query
          handleQuery(payload, socket, db);
          break;

        case 0x58: // 'X' — Terminate
          socket.end();
          return;

        case 0x50: // 'P' — Parse
          handleParse(payload, socket);
          break;
        case 0x42: // 'B' — Bind
          handleBind(payload, socket);
          break;
        case 0x44: // 'D' — Describe
          handleDescribe(payload, socket, db);
          break;
        case 0x45: // 'E' — Execute
          handleExecute(payload, socket, db);
          break;
        case 0x53: // 'S' — Sync
          socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
          break;
        case 0x43: // 'C' — Close (statement or portal)
          handleClose(payload, socket);
          break;
        case 0x48: // 'H' — Flush
          // No-op, just ensure output is flushed (Node does this automatically)
          break;
        case 0x64: // 'd' — CopyData
          if (copyState) {
            // Parse tab-separated row from copy data
            const line = payload.toString('utf8').trim();
            if (line.length > 0) {
              copyState.rows.push(line);
            }
          }
          break;
        case 0x63: // 'c' — CopyDone
          if (copyState) {
            handleCopyDone(socket, db);
          }
          break;
        case 0x66: // 'f' — CopyFail
          copyState = null;
          socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
          break;

        default:
          // Unknown message type — ignore
          break;
      }
    }
  }

  // --- Extended Query Protocol Handlers ---

  function parseComplete() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x31; // '1' — ParseComplete
    writeInt32BE(buf, 4, 1);
    return buf;
  }

  function bindComplete() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x32; // '2' — BindComplete
    writeInt32BE(buf, 4, 1);
    return buf;
  }

  function closeComplete() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x33; // '3' — CloseComplete
    writeInt32BE(buf, 4, 1);
    return buf;
  }

  function noData() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x6e; // 'n' — NoData
    writeInt32BE(buf, 4, 1);
    return buf;
  }

  function handleParse(payload, socket) {
    try {
      let offset = 0;
      // Statement name (empty string = unnamed)
      const nameEnd = payload.indexOf(0, offset);
      const stmtName = payload.slice(offset, nameEnd).toString('utf8');
      offset = nameEnd + 1;

      // Query string
      const queryEnd = payload.indexOf(0, offset);
      const sql = payload.slice(offset, queryEnd).toString('utf8');
      offset = queryEnd + 1;

      // Number of parameter type OIDs
      const numParams = readInt16BE(payload, offset); offset += 2;
      const paramTypes = [];
      for (let i = 0; i < numParams; i++) {
        paramTypes.push(readInt32BE(payload, offset)); offset += 4;
      }

      preparedStatements.set(stmtName, { sql, paramTypes });
      socket.write(parseComplete());
    } catch (err) {
      socket.write(errorResponse('ERROR', '42000', 'Parse error: ' + err.message));
    }
  }

  function handleBind(payload, socket) {
    try {
      let offset = 0;
      // Portal name
      const portalEnd = payload.indexOf(0, offset);
      const portalName = payload.slice(offset, portalEnd).toString('utf8');
      offset = portalEnd + 1;

      // Statement name
      const stmtEnd = payload.indexOf(0, offset);
      const stmtName = payload.slice(offset, stmtEnd).toString('utf8');
      offset = stmtEnd + 1;

      const stmt = preparedStatements.get(stmtName);
      if (!stmt) {
        socket.write(errorResponse('ERROR', '26000', `Prepared statement "${stmtName}" does not exist`));
        return;
      }

      // Number of parameter format codes
      const numFormats = readInt16BE(payload, offset); offset += 2;
      const formats = [];
      for (let i = 0; i < numFormats; i++) {
        formats.push(readInt16BE(payload, offset)); offset += 2;
      }

      // Number of parameter values
      const numParams = readInt16BE(payload, offset); offset += 2;
      const params = [];
      for (let i = 0; i < numParams; i++) {
        const len = readInt32BE(payload, offset); offset += 4;
        if (len === -1) {
          params.push(null);
        } else {
          // Text format (format 0) — read as string
          params.push(payload.slice(offset, offset + len).toString('utf8'));
          offset += len;
        }
      }

      // Substitute $1, $2, ... with actual parameter values in the SQL
      let boundSql = stmt.sql;
      // Replace from highest to lowest to avoid $1 matching part of $10
      for (let i = params.length; i >= 1; i--) {
        const val = params[i - 1];
        const regex = new RegExp('\\$' + i + '(?![0-9])', 'g');
        if (val === null) {
          boundSql = boundSql.replace(regex, 'NULL');
        } else if (/^-?\d+(\.\d+)?$/.test(val)) {
          boundSql = boundSql.replace(regex, val);
        } else {
          // Escape single quotes
          boundSql = boundSql.replace(regex, "'" + val.replace(/'/g, "''") + "'");
        }
      }

      portals.set(portalName, { sql: boundSql, stmt, rowDescSent: false });
      socket.write(bindComplete());
    } catch (err) {
      socket.write(errorResponse('ERROR', '42000', 'Bind error: ' + err.message));
    }
  }

  function handleDescribe(payload, socket, db) {
    try {
      const describeType = payload[0]; // 'S' = statement, 'P' = portal
      const name = payload.slice(1, payload.indexOf(0, 1)).toString('utf8');

      if (describeType === 0x53) { // 'S' — Statement
        const stmt = preparedStatements.get(name);
        if (!stmt) {
          socket.write(errorResponse('ERROR', '26000', `Prepared statement "${name}" does not exist`));
          return;
        }
        // ParameterDescription — describe parameter types
        const numParams = stmt.paramTypes.length;
        const paramDescBuf = Buffer.alloc(1 + 4 + 2 + numParams * 4);
        paramDescBuf[0] = 0x74; // 't' — ParameterDescription
        writeInt32BE(paramDescBuf, 4 + 2 + numParams * 4, 1);
        paramDescBuf[5] = (numParams >> 8) & 0xff;
        paramDescBuf[6] = numParams & 0xff;
        for (let i = 0; i < numParams; i++) {
          writeInt32BE(paramDescBuf, stmt.paramTypes[i] || 0, 7 + i * 4);
        }
        socket.write(paramDescBuf);

        // Try to determine the output columns by running a describe
        // For simplicity, we'll try to execute the query and report NoData for non-SELECT
        const sql = stmt.sql.trim().toUpperCase();
        if (sql.startsWith('SELECT') || sql.startsWith('WITH') || sql.startsWith('EXPLAIN')) {
          // We need to describe the columns without actually running it.
          // Try executing with NULLs and LIMIT 0 to get column structure
          let testSql = stmt.sql;
          for (let i = stmt.paramTypes.length; i >= 1; i--) {
            testSql = testSql.replace(new RegExp('\\$' + i + '(?![0-9])', 'g'), 'NULL');
          }
          try {
            // Add LIMIT 0 to avoid side effects
            const limitedSql = testSql.replace(/;?\s*$/, '') + ' LIMIT 0';
            const testResult = db.execute(limitedSql);
            // Even with no rows, if we can determine columns from the result, use them
            const columns = getColumns(testResult, db, stmt.sql);
            if (columns.length > 0) {
              socket.write(rowDescription(columns));
            } else {
              // Fallback: parse column aliases from SELECT list
              const columnNames = parseSelectColumns(stmt.sql);
              if (columnNames.length > 0) {
                socket.write(rowDescription(columnNames.map(n => ({ name: n, type: 'TEXT' }))));
              } else {
                socket.write(noData());
              }
            }
          } catch {
            socket.write(noData());
          }
        } else {
          socket.write(noData());
        }
      } else if (describeType === 0x50) { // 'P' — Portal
        const portal = portals.get(name);
        if (!portal) {
          socket.write(errorResponse('ERROR', '34000', `Portal "${name}" does not exist`));
          return;
        }
        // For portal describe, check if it's a SELECT-type query
        const upper = portal.sql.trim().toUpperCase();
        if (upper.startsWith('SELECT') || upper.startsWith('WITH') || upper.startsWith('EXPLAIN')) {
          // Need to describe columns without executing
          // Substitute test values to get column info
          try {
            // Use a dry run: wrap in a subquery with LIMIT 0 to avoid side effects
            const testSql = portal.sql.replace(/;?\s*$/, ' LIMIT 0');
            const testResult = db.execute(testSql);
            const columns = getColumns(testResult, db, portal.sql);
            if (columns.length > 0) {
              socket.write(rowDescription(columns));
              portal.rowDescSent = true;
            } else {
              const columnNames = parseSelectColumns(portal.sql);
              if (columnNames.length > 0) {
                socket.write(rowDescription(columnNames.map(n => ({ name: n, type: 'TEXT' }))));
                portal.rowDescSent = true;
              } else {
                socket.write(noData());
              }
            }
          } catch {
            const columnNames = parseSelectColumns(portal.sql);
            if (columnNames.length > 0) {
              socket.write(rowDescription(columnNames.map(n => ({ name: n, type: 'TEXT' }))));
              portal.rowDescSent = true;
            } else {
              socket.write(noData());
            }
          }
        } else {
          socket.write(noData());
        }
      }
    } catch (err) {
      socket.write(errorResponse('ERROR', '42000', 'Describe error: ' + err.message));
    }
  }

  function handleExecute(payload, socket, db) {
    try {
      const portalEnd = payload.indexOf(0);
      const portalName = payload.slice(0, portalEnd).toString('utf8');
      // int32 max rows (0 = no limit)
      // const maxRows = readInt32BE(payload, portalEnd + 1);

      const portal = portals.get(portalName);
      if (!portal) {
        socket.write(errorResponse('ERROR', '34000', `Portal "${portalName}" does not exist`));
        return;
      }

      const sql = portal.sql.trim();
      if (!sql) {
        socket.write(emptyQueryResponse());
        return;
      }

      const upper = sql.toUpperCase().trim();
      if (upper === 'BEGIN' || upper === 'START TRANSACTION') inTransaction = true;
      if (upper === 'COMMIT' || upper === 'ROLLBACK' || upper === 'END') inTransaction = false;

      const result = executeWithIntercept(sql);
      const tag = getCommandTag(sql, result);

      if (result.rows && result.rows.length > 0) {
        const columns = getColumns(result, db, sql);
        // Send RowDescription if not already sent during Describe
        if (!portal.rowDescSent && columns.length > 0) {
          socket.write(rowDescription(columns));
        }
        for (const row of result.rows) {
          const values = columns.map(c => {
            const val = row[c.name];
            return val === null || val === undefined ? null : val;
          });
          socket.write(dataRow(values));
        }
      }

      socket.write(commandComplete(tag));
    } catch (err) {
      const code = err.message.includes('syntax') ? '42601' :
                   err.message.includes('not found') || err.message.includes('does not exist') ? '42P01' :
                   err.message.includes('already exists') ? '42P07' :
                   err.message.includes('violates') ? '23505' :
                   '42000';
      socket.write(errorResponse('ERROR', code, err.message));
      if (inTransaction) inTransaction = false;
    }
  }

  function handleClose(payload, socket) {
    const closeType = payload[0]; // 'S' = statement, 'P' = portal
    const name = payload.slice(1, payload.indexOf(0, 1)).toString('utf8');
    if (closeType === 0x53) {
      preparedStatements.delete(name);
    } else if (closeType === 0x50) {
      portals.delete(name);
    }
    socket.write(closeComplete());
  }

  // --- Simple Query Protocol ---

  function copyInResponse(numColumns) {
    // CopyInResponse: format(1=text) + numColumns + format-per-column
    const len = 4 + 1 + 2 + numColumns * 2;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x47; // 'G' — CopyInResponse
    writeInt32BE(buf, len, 1);
    buf[5] = 0; // overall format: 0=text
    buf[6] = (numColumns >> 8) & 0xff;
    buf[7] = numColumns & 0xff;
    for (let i = 0; i < numColumns; i++) {
      buf[8 + i * 2] = 0;
      buf[9 + i * 2] = 0; // text format per column
    }
    return buf;
  }

  function handleCopyIn(socket, db, tableName, columns) {
    // Validate table exists
    const table = db.tables?.get(tableName) || db.tables?.get(tableName.toLowerCase());
    if (!table) {
      socket.write(errorResponse('ERROR', '42P01', `Table "${tableName}" does not exist`));
      socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
      return;
    }

    const colNames = columns || table.schema.map(c => c.name);
    copyState = { tableName, columns: colNames, rows: [] };
    socket.write(copyInResponse(colNames.length));
  }

  function handleCopyDone(socket, db) {
    const state = copyState;
    copyState = null;

    if (!state) {
      socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
      return;
    }

    try {
      let inserted = 0;
      for (const line of state.rows) {
        if (line === '\\.' || line === '') continue; // End marker or empty
        const values = line.split('\t').map(v => v === '\\N' ? 'NULL' : "'" + v.replace(/'/g, "''") + "'");
        const colList = state.columns.join(', ');
        const valList = values.join(', ');
        executeWithIntercept(`INSERT INTO ${state.tableName} (${colList}) VALUES (${valList})`);
        inserted++;
      }
      socket.write(commandComplete(`COPY ${inserted}`));
    } catch (err) {
      socket.write(errorResponse('ERROR', '42000', err.message));
    }
    socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
  }

  function copyOutResponse(numColumns) {
    const len = 4 + 1 + 2 + numColumns * 2;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x48; // 'H' — CopyOutResponse
    writeInt32BE(buf, len, 1);
    buf[5] = 0; // text format
    buf[6] = (numColumns >> 8) & 0xff;
    buf[7] = numColumns & 0xff;
    for (let i = 0; i < numColumns; i++) {
      buf[8 + i * 2] = 0;
      buf[9 + i * 2] = 0;
    }
    return buf;
  }

  function copyDataMsg(data) {
    const dataBuf = Buffer.from(data, 'utf8');
    const len = 4 + dataBuf.length;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x64; // 'd' — CopyData
    writeInt32BE(buf, len, 1);
    dataBuf.copy(buf, 5);
    return buf;
  }

  function copyDoneMsg() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x63; // 'c' — CopyDone
    writeInt32BE(buf, 4, 1);
    return buf;
  }

  function handleCopyOut(socket, db, tableName, columns) {
    const table = db.tables?.get(tableName) || db.tables?.get(tableName.toLowerCase());
    if (!table) {
      socket.write(errorResponse('ERROR', '42P01', `Table "${tableName}" does not exist`));
      return;
    }

    const colNames = columns || table.schema.map(c => c.name);
    const colIndices = colNames.map(name => table.schema.findIndex(c => c.name === name));

    try {
      socket.write(copyOutResponse(colNames.length));

      let rowCount = 0;
      const result = executeWithIntercept(`SELECT ${colNames.join(', ')} FROM ${tableName}`);
      if (result.rows) {
        for (const row of result.rows) {
          const values = colNames.map(c => {
            const val = row[c];
            return val === null || val === undefined ? '\\N' : String(val);
          });
          socket.write(copyDataMsg(values.join('\t') + '\n'));
          rowCount++;
        }
      }

      socket.write(copyDoneMsg());
      socket.write(commandComplete(`COPY ${rowCount}`));
    } catch (err) {
      socket.write(errorResponse('ERROR', '42000', err.message));
    }
  }

  function handleQuery(payload, socket, db) {
    // payload is the query string, null-terminated
    const query = payload.slice(0, payload.length - 1).toString('utf8').trim();

    if (!query) {
      socket.write(emptyQueryResponse());
      socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
      return;
    }

    // Split on semicolons for multi-statement queries
    const statements = query.split(';').map(s => s.trim()).filter(s => s.length > 0);

    for (const sql of statements) {
      try {
        // Detect COPY FROM STDIN
        const copyMatch = sql.match(/^COPY\s+(\w+)\s*(?:\(([^)]+)\))?\s+FROM\s+STDIN/i);
        if (copyMatch) {
          const tableName = copyMatch[1];
          const columns = copyMatch[2] ? copyMatch[2].split(',').map(c => c.trim()) : null;
          handleCopyIn(socket, db, tableName, columns);
          return; // COPY takes over the connection until CopyDone
        }

        // Detect COPY TO STDOUT
        const copyOutMatch = sql.match(/^COPY\s+(\w+)\s*(?:\(([^)]+)\))?\s+TO\s+STDOUT/i);
        if (copyOutMatch) {
          const tableName = copyOutMatch[1];
          const columns = copyOutMatch[2] ? copyOutMatch[2].split(',').map(c => c.trim()) : null;
          handleCopyOut(socket, db, tableName, columns);
          continue;
        }
        // Track transaction state
        const upper = sql.toUpperCase().trim();
        if (upper === 'BEGIN' || upper === 'START TRANSACTION') inTransaction = true;
        if (upper === 'COMMIT' || upper === 'ROLLBACK' || upper === 'END') inTransaction = false;

        const result = executeWithIntercept(sql);
        const tag = getCommandTag(sql, result);

        if (result.rows && result.rows.length > 0) {
          const columns = getColumns(result, db, sql);
          socket.write(rowDescription(columns));
          for (const row of result.rows) {
            const values = columns.map(c => {
              const val = row[c.name];
              return val === null || val === undefined ? null : val;
            });
            socket.write(dataRow(values));
          }
        } else if (result.columns && result.columns.length > 0) {
          // Query returned columns but no rows
          const columns = getColumns(result, db, sql);
          socket.write(rowDescription(columns));
        }

        socket.write(commandComplete(tag));
      } catch (err) {
        // Determine error code
        const code = err.message.includes('syntax') ? '42601' :
                     err.message.includes('not found') || err.message.includes('does not exist') ? '42P01' :
                     err.message.includes('already exists') ? '42P07' :
                     err.message.includes('violates') ? '23505' :
                     '42000';
        socket.write(errorResponse('ERROR', code, err.message));
        if (inTransaction) inTransaction = false; // Transaction aborted
      }
    }

    socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
  }
}

// --- Server creation ---
// --- Advisory Lock Manager (connection-scoped) ---
class AdvisoryLockManager {
  constructor() {
    this._locks = new Map(); // key -> { holder: connectionId, count: number }
  }
  
  lock(connId, key) {
    const existing = this._locks.get(key);
    if (existing) {
      if (existing.holder === connId) {
        existing.count++;
        return true; // Re-entrant
      }
      return false; // Held by another connection
    }
    this._locks.set(key, { holder: connId, count: 1 });
    return true;
  }
  
  tryLock(connId, key) {
    return this.lock(connId, key);
  }
  
  unlock(connId, key) {
    const existing = this._locks.get(key);
    if (!existing || existing.holder !== connId) return false;
    existing.count--;
    if (existing.count <= 0) this._locks.delete(key);
    return true;
  }
  
  releaseAll(connId) {
    for (const [key, info] of this._locks) {
      if (info.holder === connId) this._locks.delete(key);
    }
  }
}

// Global advisory lock manager shared across all connections
const _advisoryLocks = new AdvisoryLockManager();
let _nextConnId = 1;

export function createPgServer(db, port = 5433) {
  const server = createServer((socket) => {
    const connId = _nextConnId++;
    handleConnection(socket, db, connId);
  });

  server.listen(port, () => {
    console.log(`HenryDB PG wire protocol server listening on port ${port}`);
    console.log(`Connect: psql -h localhost -p ${port}`);
  });

  return server;
}

// --- CLI entry point ---
if (process.argv[1]?.endsWith('pg-server.js')) {
  const { PersistentDatabase } = await import('./persistent-db.js');
  const { join } = await import('node:path');
  
  const args = process.argv.slice(2);
  const portIdx = args.indexOf('--port');
  const dirIdx = args.indexOf('--dir');
  const port = portIdx >= 0 ? parseInt(args[portIdx + 1], 10) : 5433;
  const dataDir = dirIdx >= 0 ? args[dirIdx + 1] : null;

  let db;
  if (dataDir) {
    db = PersistentDatabase.open(dataDir, { poolSize: 64 });
    console.log(`HenryDB persistent storage: ${dataDir}`);
  } else {
    db = new Database();
    console.log('HenryDB in-memory instance (use --dir for persistence)');
  }

  const server = createPgServer(db, port);

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    if (db.close) db.close();
    server.close();
    process.exit(0);
  });
}
