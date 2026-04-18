// pg-server.test.js — Tests for PostgreSQL wire protocol server
import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createConnection } from 'node:net';
import { createPgServer } from './pg-server.js';
import { Database } from './db.js';

// --- PG wire helpers ---
function writeInt32BE(buf, val, offset) {
  buf[offset] = (val >>> 24) & 0xff;
  buf[offset + 1] = (val >>> 16) & 0xff;
  buf[offset + 2] = (val >>> 8) & 0xff;
  buf[offset + 3] = val & 0xff;
}

function readInt32BE(buf, offset) {
  return (buf[offset] << 24) | (buf[offset + 1] << 16) | (buf[offset + 2] << 8) | buf[offset + 3];
}

function buildStartup(user = 'henrydb', database = 'henrydb') {
  const params = Buffer.from(`user\0${user}\0database\0${database}\0\0`, 'utf8');
  const len = 4 + 4 + params.length;
  const buf = Buffer.alloc(len);
  writeInt32BE(buf, len, 0);
  writeInt32BE(buf, 196608, 4); // v3.0
  params.copy(buf, 8);
  return buf;
}

function buildQuery(sql) {
  const sqlBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + sqlBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51; // 'Q'
  writeInt32BE(buf, len, 1);
  sqlBuf.copy(buf, 5);
  return buf;
}

function buildTerminate() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x58; // 'X'
  writeInt32BE(buf, 4, 1);
  return buf;
}

function parseMessages(buf) {
  const messages = [];
  let offset = 0;
  while (offset < buf.length) {
    if (offset + 5 > buf.length) break;
    const type = String.fromCharCode(buf[offset]);
    const len = readInt32BE(buf, offset + 1);
    if (offset + 1 + len > buf.length) break;
    const payload = buf.slice(offset + 5, offset + 1 + len);
    messages.push({ type, len, payload });
    offset += 1 + len;
  }
  return messages;
}

function extractRows(messages) {
  const rows = [];
  const columns = [];
  
  for (const msg of messages) {
    if (msg.type === 'T') {
      // RowDescription
      const fieldCount = (msg.payload[0] << 8) | msg.payload[1];
      let off = 2;
      for (let i = 0; i < fieldCount; i++) {
        const end = msg.payload.indexOf(0, off);
        columns.push(msg.payload.slice(off, end).toString('utf8'));
        off = end + 1 + 18; // skip: null + tableOid(4) + colAttr(2) + typeOid(4) + typeLen(2) + typeMod(4) + format(2)
      }
    }
    if (msg.type === 'D') {
      // DataRow
      const colCount = (msg.payload[0] << 8) | msg.payload[1];
      let off = 2;
      const row = {};
      for (let i = 0; i < colCount; i++) {
        const valLen = readInt32BE(msg.payload, off); off += 4;
        if (valLen === -1) {
          row[columns[i]] = null;
        } else {
          row[columns[i]] = msg.payload.slice(off, off + valLen).toString('utf8');
          off += valLen;
        }
      }
      rows.push(row);
    }
  }
  return { columns, rows };
}

function extractCommandTag(messages) {
  for (const msg of messages) {
    if (msg.type === 'C') {
      return msg.payload.slice(0, msg.payload.indexOf(0)).toString('utf8');
    }
  }
  return null;
}

function extractError(messages) {
  for (const msg of messages) {
    if (msg.type === 'E') {
      // Parse error fields
      const fields = {};
      let off = 0;
      while (off < msg.payload.length) {
        const fieldType = msg.payload[off];
        if (fieldType === 0) break;
        off++;
        const end = msg.payload.indexOf(0, off);
        fields[String.fromCharCode(fieldType)] = msg.payload.slice(off, end).toString('utf8');
        off = end + 1;
      }
      return fields;
    }
  }
  return null;
}

// --- Test helpers ---
function connect(port) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(port, 'localhost', () => resolve(socket));
    socket.on('error', reject);
  });
}

function sendAndReceive(socket, buf, waitMs = 200) {
  return new Promise((resolve) => {
    let response = Buffer.alloc(0);
    const handler = (data) => { response = Buffer.concat([response, data]); };
    socket.on('data', handler);
    socket.write(buf);
    setTimeout(() => {
      socket.removeListener('data', handler);
      resolve(response);
    }, waitMs);
  });
}

// Wait for ReadyForQuery
function waitForReady(socket, timeout = 2000) {
  return new Promise((resolve, reject) => {
    let response = Buffer.alloc(0);
    const timer = setTimeout(() => {
      socket.removeListener('data', handler);
      reject(new Error('Timeout waiting for ReadyForQuery'));
    }, timeout);
    
    const handler = (data) => {
      response = Buffer.concat([response, data]);
      // Check for 'Z' message (ReadyForQuery)
      const msgs = parseMessages(response);
      if (msgs.some(m => m.type === 'Z')) {
        clearTimeout(timer);
        socket.removeListener('data', handler);
        resolve(response);
      }
    };
    socket.on('data', handler);
  });
}

let db, server, port;

// Use a different port for each test run to avoid conflicts
const BASE_PORT = 15433 + Math.floor(Math.random() * 1000);
let portCounter = 0;

describe('PG Wire Protocol', () => {
  beforeEach(() => {
    db = new Database();
    port = BASE_PORT + portCounter++;
    server = createPgServer(db, port);
  });

  afterEach((_, done) => {
    server.close(() => done());
  });

  test('startup handshake succeeds', async () => {
    const socket = await connect(port);
    const response = await sendAndReceive(socket, buildStartup());
    const messages = parseMessages(response);
    
    // Should get: R (auth ok), S (param status)..., Z (ready)
    const types = messages.map(m => m.type);
    assert.ok(types.includes('R'), 'should get AuthenticationOk');
    assert.ok(types.includes('S'), 'should get ParameterStatus');
    assert.ok(types.includes('Z'), 'should get ReadyForQuery');
    
    // Check ReadyForQuery status = 'I' (idle)
    const readyMsg = messages.find(m => m.type === 'Z');
    assert.equal(readyMsg.payload[0], 'I'.charCodeAt(0));
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('SSL negotiation returns N', async () => {
    const socket = await connect(port);
    
    // Send SSLRequest
    const sslBuf = Buffer.alloc(8);
    writeInt32BE(sslBuf, 8, 0); // length
    writeInt32BE(sslBuf, 80877103, 4); // SSL request code
    
    const response = await sendAndReceive(socket, sslBuf, 100);
    assert.equal(response.toString(), 'N', 'should refuse SSL with N');
    
    // Now send real startup
    const startupResponse = await sendAndReceive(socket, buildStartup());
    const messages = parseMessages(startupResponse);
    assert.ok(messages.some(m => m.type === 'Z'), 'should complete startup after SSL refusal');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('CREATE TABLE + INSERT + SELECT', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    // CREATE TABLE
    let resp = await sendAndReceive(socket, buildQuery('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)'));
    let tag = extractCommandTag(parseMessages(resp));
    assert.equal(tag, 'CREATE TABLE');
    
    // INSERT
    resp = await sendAndReceive(socket, buildQuery("INSERT INTO users VALUES (1, 'Alice')"));
    tag = extractCommandTag(parseMessages(resp));
    assert.ok(tag.startsWith('INSERT'), `Expected INSERT tag, got: ${tag}`);
    
    // INSERT another
    resp = await sendAndReceive(socket, buildQuery("INSERT INTO users VALUES (2, 'Bob')"));
    
    // SELECT
    resp = await sendAndReceive(socket, buildQuery('SELECT * FROM users ORDER BY id'));
    const messages = parseMessages(resp);
    const { columns, rows } = extractRows(messages);
    
    assert.deepEqual(columns, ['id', 'name']);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].id, '1');
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].id, '2');
    assert.equal(rows[1].name, 'Bob');
    
    tag = extractCommandTag(messages);
    assert.equal(tag, 'SELECT 2');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('error response for bad SQL', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    const resp = await sendAndReceive(socket, buildQuery('SELECT * FROM nonexistent'));
    const messages = parseMessages(resp);
    const error = extractError(messages);
    
    assert.ok(error, 'should get error response');
    assert.equal(error.S, 'ERROR');
    assert.ok(error.M.length > 0, 'should have error message');
    
    // Should still get ReadyForQuery after error
    assert.ok(messages.some(m => m.type === 'Z'), 'should get ReadyForQuery after error');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('empty query returns EmptyQueryResponse', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    const resp = await sendAndReceive(socket, buildQuery(''));
    const messages = parseMessages(resp);
    
    assert.ok(messages.some(m => m.type === 'I'), 'should get EmptyQueryResponse');
    assert.ok(messages.some(m => m.type === 'Z'), 'should get ReadyForQuery');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('NULL values sent as -1 length', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    await sendAndReceive(socket, buildQuery('CREATE TABLE t (id INT, val TEXT)'));
    await sendAndReceive(socket, buildQuery('INSERT INTO t VALUES (1, NULL)'));
    
    const resp = await sendAndReceive(socket, buildQuery('SELECT * FROM t'));
    const { rows } = extractRows(parseMessages(resp));
    
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, '1');
    assert.equal(rows[0].val, null);
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('UPDATE and DELETE command tags', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    await sendAndReceive(socket, buildQuery('CREATE TABLE t (id INT PRIMARY KEY, val INT)'));
    await sendAndReceive(socket, buildQuery('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)'));
    
    // UPDATE
    let resp = await sendAndReceive(socket, buildQuery('UPDATE t SET val = 99 WHERE id = 1'));
    let tag = extractCommandTag(parseMessages(resp));
    assert.ok(tag.startsWith('UPDATE'), `Expected UPDATE tag, got: ${tag}`);
    
    // DELETE
    resp = await sendAndReceive(socket, buildQuery('DELETE FROM t WHERE id = 2'));
    tag = extractCommandTag(parseMessages(resp));
    assert.ok(tag.startsWith('DELETE'), `Expected DELETE tag, got: ${tag}`);
    
    // Verify
    resp = await sendAndReceive(socket, buildQuery('SELECT * FROM t ORDER BY id'));
    const { rows } = extractRows(parseMessages(resp));
    assert.equal(rows.length, 2);
    assert.equal(rows[0].val, '99');
    assert.equal(rows[1].id, '3');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('multi-statement query (separated by semicolons)', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    // Send multiple statements in one query
    await sendAndReceive(socket, buildQuery('CREATE TABLE t (id INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)'));
    
    const resp = await sendAndReceive(socket, buildQuery('SELECT * FROM t ORDER BY id'));
    const { rows } = extractRows(parseMessages(resp));
    assert.equal(rows.length, 2);
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('multiple concurrent connections', async () => {
    const socket1 = await connect(port);
    const socket2 = await connect(port);
    
    await sendAndReceive(socket1, buildStartup());
    await sendAndReceive(socket2, buildStartup());
    
    // Both can create and query independently (separate DB instances per connection? No — shared DB)
    await sendAndReceive(socket1, buildQuery('CREATE TABLE shared (id INT)'));
    await sendAndReceive(socket1, buildQuery('INSERT INTO shared VALUES (1)'));
    
    // Socket 2 should see the same data (shared DB)
    const resp = await sendAndReceive(socket2, buildQuery('SELECT * FROM shared'));
    const { rows } = extractRows(parseMessages(resp));
    assert.equal(rows.length, 1);
    
    socket1.write(buildTerminate()); socket1.end();
    socket2.write(buildTerminate()); socket2.end();
  });

  test('parameter status includes server_version', async () => {
    const socket = await connect(port);
    const response = await sendAndReceive(socket, buildStartup());
    const messages = parseMessages(response);
    
    const paramMsgs = messages.filter(m => m.type === 'S');
    assert.ok(paramMsgs.length >= 1, 'should have parameter status messages');
    
    // Check that one of them is server_version
    let foundVersion = false;
    for (const msg of paramMsgs) {
      const payload = msg.payload.toString('utf8');
      if (payload.includes('server_version')) {
        foundVersion = true;
        assert.ok(payload.includes('HenryDB'), 'version should mention HenryDB');
      }
    }
    assert.ok(foundVersion, 'should include server_version parameter');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('aggregate queries work through wire protocol', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    await sendAndReceive(socket, buildQuery('CREATE TABLE nums (val INT)'));
    await sendAndReceive(socket, buildQuery('INSERT INTO nums VALUES (10), (20), (30)'));
    
    const resp = await sendAndReceive(socket, buildQuery('SELECT COUNT(*) as cnt, SUM(val) as total FROM nums'));
    const { rows } = extractRows(parseMessages(resp));
    assert.equal(rows.length, 1);
    assert.equal(rows[0].cnt, '3');
    assert.equal(rows[0].total, '60');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('UDF works through wire protocol', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    await sendAndReceive(socket, buildQuery("CREATE FUNCTION double(x INT) RETURNS INT AS $$ SELECT x * 2 $$"));
    
    const resp = await sendAndReceive(socket, buildQuery('SELECT double(21) as result'));
    const { rows } = extractRows(parseMessages(resp));
    assert.equal(rows.length, 1);
    assert.equal(rows[0].result, '42');
    
    socket.write(buildTerminate());
    socket.end();
  });

  test('transaction state indicator changes', async () => {
    const socket = await connect(port);
    await sendAndReceive(socket, buildStartup());
    
    await sendAndReceive(socket, buildQuery('CREATE TABLE t (id INT)'));
    
    // BEGIN
    let resp = await sendAndReceive(socket, buildQuery('BEGIN'));
    let messages = parseMessages(resp);
    let readyMsg = messages.find(m => m.type === 'Z');
    assert.equal(readyMsg.payload[0], 'T'.charCodeAt(0), 'should be in transaction after BEGIN');
    
    // INSERT in transaction
    resp = await sendAndReceive(socket, buildQuery('INSERT INTO t VALUES (1)'));
    messages = parseMessages(resp);
    readyMsg = messages.find(m => m.type === 'Z');
    assert.equal(readyMsg.payload[0], 'T'.charCodeAt(0), 'should still be in transaction');
    
    // COMMIT
    resp = await sendAndReceive(socket, buildQuery('COMMIT'));
    messages = parseMessages(resp);
    readyMsg = messages.find(m => m.type === 'Z');
    assert.equal(readyMsg.payload[0], 'I'.charCodeAt(0), 'should be idle after COMMIT');
    
    socket.write(buildTerminate());
    socket.end();
  });
});
