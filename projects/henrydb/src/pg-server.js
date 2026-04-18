// pg-server.js — PostgreSQL wire protocol v3 server for HenryDB
// Usage: node pg-server.js [--port 5433] [--dir ./data]
// Connect: psql -h localhost -p 5433 -U henrydb

import { createServer } from 'node:net';
import { Database } from './db.js';

// --- PG Type OIDs ---
const PG_OIDS = {
  INT: 23,        // int4
  FLOAT: 701,     // float8
  TEXT: 25,       // text
  BOOLEAN: 16,    // bool
  DATE: 1082,     // date
  SERIAL: 23,     // serial → int4
  BIGINT: 20,     // int8
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
function getColumns(result, db) {
  if (result.columns && result.columns.length > 0) {
    return result.columns.map(c => typeof c === 'string' ? { name: c, type: 'TEXT' } : c);
  }
  if (result.rows && result.rows.length > 0) {
    return Object.keys(result.rows[0]).map(name => ({ name, type: 'TEXT' }));
  }
  return [];
}

// --- Connection handler ---
function handleConnection(socket, db) {
  let buffer = Buffer.alloc(0);
  let startupDone = false;
  let inTransaction = false;

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    processBuffer();
  });

  socket.on('error', () => { /* client disconnected */ });
  socket.on('close', () => { /* cleanup */ });

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

        case 0x50: // 'P' — Parse (extended query — send error: not supported)
        case 0x42: // 'B' — Bind
        case 0x44: // 'D' — Describe
        case 0x45: // 'E' — Execute
        case 0x53: // 'S' — Sync
          // For extended query protocol, just send error and ready
          socket.write(errorResponse('ERROR', '0A000', 'Extended query protocol not yet supported. Use simple query mode.'));
          socket.write(readyForQuery(inTransaction ? 'T' : 'I'));
          break;

        default:
          // Unknown message type — ignore
          break;
      }
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
        // Track transaction state
        const upper = sql.toUpperCase().trim();
        if (upper === 'BEGIN' || upper === 'START TRANSACTION') inTransaction = true;
        if (upper === 'COMMIT' || upper === 'ROLLBACK' || upper === 'END') inTransaction = false;

        const result = db.execute(sql);
        const tag = getCommandTag(sql, result);

        if (result.rows && result.rows.length > 0) {
          const columns = getColumns(result, db);
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
          const columns = getColumns(result, db);
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
export function createPgServer(db, port = 5433) {
  const server = createServer((socket) => {
    handleConnection(socket, db);
  });

  server.listen(port, () => {
    console.log(`HenryDB PG wire protocol server listening on port ${port}`);
    console.log(`Connect: psql -h localhost -p ${port}`);
  });

  return server;
}

// --- CLI entry point ---
if (process.argv[1]?.endsWith('pg-server.js')) {
  const args = process.argv.slice(2);
  const portIdx = args.indexOf('--port');
  const port = portIdx >= 0 ? parseInt(args[portIdx + 1], 10) : 5433;

  const db = new Database();
  console.log('HenryDB in-memory instance created');

  const server = createPgServer(db, port);

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    server.close();
    process.exit(0);
  });
}
