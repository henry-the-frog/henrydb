// server.test.js — Tests for HenryDB TCP server
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';
import {
  parseStartupMessage, writeRowDescription, writeDataRow,
} from './pg-protocol.js';

// Helper: create a raw TCP client that speaks PostgreSQL wire protocol
function createPgClient(port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port }, () => {
      resolve(socket);
    });
    socket.on('error', reject);
  });
}

// Helper: send startup message
function sendStartup(socket, user = 'test', database = 'testdb') {
  const params = `user\0${user}\0database\0${database}\0\0`;
  const paramsBuf = Buffer.from(params, 'utf8');
  const len = 4 + 4 + paramsBuf.length;
  const buf = Buffer.alloc(len);
  buf.writeInt32BE(len, 0);
  buf.writeInt32BE(196608, 4); // protocol 3.0
  paramsBuf.copy(buf, 8);
  socket.write(buf);
}

// Helper: send simple query
function sendQuery(socket, sql) {
  const queryBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + queryBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51; // 'Q'
  buf.writeInt32BE(len, 1);
  queryBuf.copy(buf, 5);
  socket.write(buf);
}

// Helper: send terminate
function sendTerminate(socket) {
  const buf = Buffer.alloc(5);
  buf[0] = 0x58; // 'X'
  buf.writeInt32BE(4, 1);
  socket.write(buf);
}

// Helper: collect response bytes until ReadyForQuery
function collectUntilReady(socket, timeout = 2000) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const timer = setTimeout(() => {
      socket.removeAllListeners('data');
      resolve(Buffer.concat(chunks));
    }, timeout);

    socket.on('data', (data) => {
      chunks.push(data);
      // Check if we got ReadyForQuery ('Z')
      const combined = Buffer.concat(chunks);
      for (let i = 0; i < combined.length; i++) {
        if (combined[i] === 0x5A) { // 'Z'
          clearTimeout(timer);
          socket.removeAllListeners('data');
          resolve(combined);
          return;
        }
      }
    });
  });
}

// Helper: parse response messages
function parseMessages(buf) {
  const messages = [];
  let offset = 0;
  while (offset < buf.length) {
    if (offset + 5 > buf.length) break;
    const type = String.fromCharCode(buf[offset]);
    const len = buf.readInt32BE(offset + 1);
    const totalLen = 1 + len;
    if (offset + totalLen > buf.length) break;
    
    const body = buf.subarray(offset + 1, offset + totalLen);
    messages.push({ type, body, raw: buf.subarray(offset, offset + totalLen) });
    offset += totalLen;
  }
  return messages;
}

// Helper: extract text fields from DataRow
function extractDataRow(msg) {
  const body = msg.body;
  const fieldCount = body.readInt16BE(4);
  const values = [];
  let off = 6;
  for (let i = 0; i < fieldCount; i++) {
    const fieldLen = body.readInt32BE(off);
    off += 4;
    if (fieldLen === -1) {
      values.push(null);
    } else {
      values.push(body.toString('utf8', off, off + fieldLen));
      off += fieldLen;
    }
  }
  return values;
}

// Helper: extract column names from RowDescription
function extractColumns(msg) {
  const body = msg.body;
  const fieldCount = body.readInt16BE(4);
  const names = [];
  let off = 6;
  for (let i = 0; i < fieldCount; i++) {
    const nameEnd = body.indexOf(0, off);
    names.push(body.toString('utf8', off, nameEnd));
    off = nameEnd + 1 + 4 + 2 + 4 + 2 + 4 + 2; // skip fixed fields
  }
  return names;
}

describe('HenryDB Server', () => {
  let server;
  const PORT = 15433;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('accepts connection and completes startup handshake', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    // Should have: R (AuthOk), multiple S (ParameterStatus), K (BackendKeyData), Z (ReadyForQuery)
    const types = msgs.map(m => m.type);
    assert.ok(types.includes('R'), 'Missing AuthenticationOk');
    assert.ok(types.includes('S'), 'Missing ParameterStatus');
    assert.ok(types.includes('K'), 'Missing BackendKeyData');
    assert.ok(types.includes('Z'), 'Missing ReadyForQuery');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles SSL negotiation', async () => {
    const socket = await createPgClient(PORT);
    
    // Send SSL request
    const sslBuf = Buffer.alloc(8);
    sslBuf.writeInt32BE(8, 0);
    sslBuf.writeInt32BE(80877103, 4);
    socket.write(sslBuf);

    // Should get 'N' back
    const response = await new Promise(resolve => {
      socket.once('data', resolve);
    });
    assert.strictEqual(response.toString(), 'N');

    // Then normal startup should work
    sendStartup(socket);
    const startupResp = await collectUntilReady(socket);
    const msgs = parseMessages(startupResp);
    assert.ok(msgs.some(m => m.type === 'Z'));

    sendTerminate(socket);
    socket.destroy();
  });

  it('executes CREATE TABLE', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'CREATE TABLE test_users (id INTEGER, name TEXT, email TEXT)');
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    // Should have CommandComplete + ReadyForQuery
    assert.ok(msgs.some(m => m.type === 'C'), 'Missing CommandComplete');
    assert.ok(msgs.some(m => m.type === 'Z'), 'Missing ReadyForQuery');

    sendTerminate(socket);
    socket.destroy();
  });

  it('executes INSERT and SELECT', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    // INSERT
    sendQuery(socket, "INSERT INTO test_users VALUES (1, 'Alice', 'alice@example.com')");
    const insertResp = await collectUntilReady(socket);
    const insertMsgs = parseMessages(insertResp);
    assert.ok(insertMsgs.some(m => m.type === 'C'));

    sendQuery(socket, "INSERT INTO test_users VALUES (2, 'Bob', 'bob@example.com')");
    await collectUntilReady(socket);

    // SELECT
    sendQuery(socket, 'SELECT * FROM test_users ORDER BY id');
    const selectResp = await collectUntilReady(socket);
    const selectMsgs = parseMessages(selectResp);

    // Should have: T (RowDescription), D (DataRow) x2, C (CommandComplete), Z (ReadyForQuery)
    const types = selectMsgs.map(m => m.type);
    assert.ok(types.includes('T'), 'Missing RowDescription');
    assert.strictEqual(types.filter(t => t === 'D').length, 2, 'Expected 2 DataRows');

    // Check column names
    const rowDesc = selectMsgs.find(m => m.type === 'T');
    const columns = extractColumns(rowDesc);
    assert.ok(columns.includes('id'));
    assert.ok(columns.includes('name'));

    // Check data
    const dataRows = selectMsgs.filter(m => m.type === 'D');
    const row1 = extractDataRow(dataRows[0]);
    assert.strictEqual(row1[0], '1');
    assert.strictEqual(row1[1], 'Alice');

    const row2 = extractDataRow(dataRows[1]);
    assert.strictEqual(row2[0], '2');
    assert.strictEqual(row2[1], 'Bob');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles SELECT with WHERE clause', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, "SELECT name, email FROM test_users WHERE id = 1");
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    const dataRows = msgs.filter(m => m.type === 'D');
    assert.strictEqual(dataRows.length, 1);
    const row = extractDataRow(dataRows[0]);
    assert.strictEqual(row[0], 'Alice');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles SQL errors gracefully', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'SELECT * FROM nonexistent_table');
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    // Should have ErrorResponse + ReadyForQuery
    assert.ok(msgs.some(m => m.type === 'E'), 'Missing ErrorResponse');
    assert.ok(msgs.some(m => m.type === 'Z'), 'Missing ReadyForQuery');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles empty query', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, '  ');
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);
    assert.ok(msgs.some(m => m.type === 'Z'));

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles multiple sequential queries', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    // Query 1
    sendQuery(socket, 'SELECT COUNT(*) AS cnt FROM test_users');
    const r1 = await collectUntilReady(socket);
    const m1 = parseMessages(r1);
    const d1 = m1.filter(m => m.type === 'D');
    assert.strictEqual(d1.length, 1);

    // Query 2
    sendQuery(socket, "SELECT name FROM test_users WHERE name = 'Bob'");
    const r2 = await collectUntilReady(socket);
    const m2 = parseMessages(r2);
    const d2 = m2.filter(m => m.type === 'D');
    assert.strictEqual(d2.length, 1);
    assert.strictEqual(extractDataRow(d2[0])[0], 'Bob');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles transactions (BEGIN/COMMIT)', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'BEGIN');
    const r1 = await collectUntilReady(socket);
    const m1 = parseMessages(r1);
    // ReadyForQuery should show 'T' (in transaction)
    const z1 = m1.find(m => m.type === 'Z');
    assert.ok(z1);
    assert.strictEqual(z1.raw[z1.raw.length - 1], 0x54); // 'T'

    sendQuery(socket, "INSERT INTO test_users VALUES (3, 'Charlie', 'charlie@example.com')");
    await collectUntilReady(socket);

    sendQuery(socket, 'COMMIT');
    const r3 = await collectUntilReady(socket);
    const m3 = parseMessages(r3);
    const z3 = m3.find(m => m.type === 'Z');
    assert.strictEqual(z3.raw[z3.raw.length - 1], 0x49); // 'I' (idle)

    // Verify the insert persisted
    sendQuery(socket, "SELECT name FROM test_users WHERE id = 3");
    const r4 = await collectUntilReady(socket);
    const m4 = parseMessages(r4);
    const d4 = m4.filter(m => m.type === 'D');
    assert.strictEqual(d4.length, 1);
    assert.strictEqual(extractDataRow(d4[0])[0], 'Charlie');

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles multiple concurrent connections', async () => {
    const socket1 = await createPgClient(PORT);
    const socket2 = await createPgClient(PORT);

    sendStartup(socket1);
    sendStartup(socket2);

    await collectUntilReady(socket1);
    await collectUntilReady(socket2);

    // Both connections can query
    sendQuery(socket1, 'SELECT COUNT(*) AS cnt FROM test_users');
    sendQuery(socket2, "SELECT name FROM test_users WHERE id = 1");

    const r1 = await collectUntilReady(socket1);
    const r2 = await collectUntilReady(socket2);

    const d1 = parseMessages(r1).filter(m => m.type === 'D');
    const d2 = parseMessages(r2).filter(m => m.type === 'D');

    assert.strictEqual(d1.length, 1);
    assert.strictEqual(d2.length, 1);
    assert.strictEqual(extractDataRow(d2[0])[0], 'Alice');

    sendTerminate(socket1);
    sendTerminate(socket2);
    socket1.destroy();
    socket2.destroy();
  });

  it('handles aggregate queries', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'SELECT COUNT(*) AS total, MIN(id) AS min_id, MAX(id) AS max_id FROM test_users');
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    const dataRows = msgs.filter(m => m.type === 'D');
    assert.strictEqual(dataRows.length, 1);

    const row = extractDataRow(dataRows[0]);
    assert.strictEqual(parseInt(row[0]), 3); // 3 users
    assert.strictEqual(parseInt(row[1]), 1); // min id
    assert.strictEqual(parseInt(row[2]), 3); // max id

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles NULL values in results', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'CREATE TABLE null_test (id INTEGER, val TEXT)');
    await collectUntilReady(socket);

    sendQuery(socket, "INSERT INTO null_test VALUES (1, NULL)");
    await collectUntilReady(socket);

    sendQuery(socket, 'SELECT * FROM null_test');
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    const dataRows = msgs.filter(m => m.type === 'D');
    assert.strictEqual(dataRows.length, 1);
    const row = extractDataRow(dataRows[0]);
    assert.strictEqual(row[0], '1');
    assert.strictEqual(row[1], null);

    sendTerminate(socket);
    socket.destroy();
  });

  it('handles JOIN queries', async () => {
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    sendQuery(socket, 'CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)');
    await collectUntilReady(socket);

    sendQuery(socket, 'INSERT INTO orders VALUES (1, 1, 100)');
    await collectUntilReady(socket);
    sendQuery(socket, 'INSERT INTO orders VALUES (2, 1, 200)');
    await collectUntilReady(socket);
    sendQuery(socket, 'INSERT INTO orders VALUES (3, 2, 150)');
    await collectUntilReady(socket);

    sendQuery(socket, `
      SELECT u.name, SUM(o.amount) AS total
      FROM test_users u
      JOIN orders o ON u.id = o.user_id
      GROUP BY u.name
      ORDER BY u.name
    `);
    const response = await collectUntilReady(socket);
    const msgs = parseMessages(response);

    const dataRows = msgs.filter(m => m.type === 'D');
    assert.strictEqual(dataRows.length, 2);
    // DB returns multiple column aliases (u.name, name, total, SUM(o.amount))
    // Find 'Alice' and '300' somewhere in the row
    const row1 = extractDataRow(dataRows[0]);
    assert.ok(row1.includes('Alice'), `Row should contain Alice: ${JSON.stringify(row1)}`);
    assert.ok(row1.includes('300'), `Row should contain 300: ${JSON.stringify(row1)}`);

    sendTerminate(socket);
    socket.destroy();
  });

  it('tracks connection count', async () => {
    // Wait for any previous sockets to fully close
    await new Promise(r => setTimeout(r, 200));
    const initialCount = server.connections.size;
    const socket = await createPgClient(PORT);
    sendStartup(socket);
    await collectUntilReady(socket);

    assert.strictEqual(server.connections.size, initialCount + 1);

    sendTerminate(socket);
    // Wait for cleanup
    await new Promise(r => setTimeout(r, 200));
    assert.strictEqual(server.connections.size, initialCount);
  });
});
