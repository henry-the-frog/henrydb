// concurrent-pg.test.js — Test concurrent PG protocol connections with transaction isolation
// Uses a lightweight TCP test to verify the protocol works with concurrent connections
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, createConnection } from 'node:net';
import { TransactionalDatabase } from './transactional-db.js';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
} from './pg-protocol.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Minimal PG client
function pgQuery(port, sql) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    let buffer = Buffer.alloc(0);
    let phase = 'startup';
    const rows = [];
    let columns = [];
    let error = null;
    let tag = '';

    socket.on('connect', () => {
      // Send startup message
      const pairs = 'user\0henrydb\0database\0henrydb\0\0';
      const len = 4 + 4 + pairs.length;
      const buf = Buffer.alloc(len);
      buf.writeInt32BE(len, 0);
      buf.writeInt32BE(196608, 4);
      buf.write(pairs, 8, 'utf8');
      socket.write(buf);
    });

    socket.on('data', (data) => {
      buffer = Buffer.concat([buffer, data]);
      processMessages();
    });

    socket.on('error', reject);

    function processMessages() {
      while (buffer.length >= 5) {
        const msgType = String.fromCharCode(buffer[0]);
        const msgLen = buffer.readInt32BE(1);
        const totalLen = 1 + msgLen;
        if (buffer.length < totalLen) break;
        const payload = buffer.subarray(0, totalLen);
        buffer = buffer.subarray(totalLen);

        if (msgType === 'Z') {
          if (phase === 'startup') {
            phase = 'query';
            // Now send the query
            const qBuf = Buffer.from(sql + '\0', 'utf8');
            const msg = Buffer.alloc(1 + 4 + qBuf.length);
            msg[0] = 0x51;
            msg.writeInt32BE(4 + qBuf.length, 1);
            qBuf.copy(msg, 5);
            socket.write(msg);
          } else {
            // Done — close and resolve
            const term = Buffer.alloc(5);
            term[0] = 0x58;
            term.writeInt32BE(4, 1);
            socket.write(term);
            socket.end();
            if (error) reject(new Error(error));
            else resolve({ rows, tag, columns });
          }
        } else if (msgType === 'T') {
          // RowDescription
          const numCols = payload.readInt16BE(5);
          columns = [];
          let offset = 7;
          for (let i = 0; i < numCols; i++) {
            const nameEnd = payload.indexOf(0, offset);
            columns.push(payload.toString('utf8', offset, nameEnd));
            offset = nameEnd + 1 + 18;
          }
        } else if (msgType === 'D') {
          // DataRow
          const numCols2 = payload.readInt16BE(5);
          const row = {};
          let off = 7;
          for (let i = 0; i < numCols2; i++) {
            const colLen = payload.readInt32BE(off);
            off += 4;
            if (colLen === -1) {
              row[columns[i]] = null;
            } else {
              const val = payload.toString('utf8', off, off + colLen);
              row[columns[i]] = isNaN(val) || val === '' ? val : Number(val);
              off += colLen;
            }
          }
          rows.push(row);
        } else if (msgType === 'C') {
          tag = payload.toString('utf8', 5, payload.indexOf(0, 5));
        } else if (msgType === 'E') {
          let eoff = 5;
          while (eoff < payload.length) {
            const field = payload[eoff];
            if (field === 0) break;
            eoff++;
            const end = payload.indexOf(0, eoff);
            if (field === 0x4D) error = payload.toString('utf8', eoff, end);
            eoff = end + 1;
          }
        }
      }
    }

    setTimeout(() => { socket.destroy(); reject(new Error('Timeout')); }, 5000);
  });
}

// Multi-query PG session (connects once, sends multiple queries)
function createPGSession(port) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    let buffer = Buffer.alloc(0);
    let queryCallback = null;
    let phase = 'startup';

    socket.on('connect', () => {
      const pairs = 'user\0henrydb\0database\0henrydb\0\0';
      const len = 4 + 4 + pairs.length;
      const buf = Buffer.alloc(len);
      buf.writeInt32BE(len, 0);
      buf.writeInt32BE(196608, 4);
      buf.write(pairs, 8, 'utf8');
      socket.write(buf);
    });

    socket.on('data', (data) => {
      buffer = Buffer.concat([buffer, data]);
      processMessages();
    });

    socket.on('error', (e) => { if (queryCallback) queryCallback.reject(e); });

    let rows = [], columns = [], error = null, tag = '';

    function processMessages() {
      while (buffer.length >= 5) {
        const msgType = String.fromCharCode(buffer[0]);
        const msgLen = buffer.readInt32BE(1);
        const totalLen = 1 + msgLen;
        if (buffer.length < totalLen) break;
        const payload = buffer.subarray(0, totalLen);
        buffer = buffer.subarray(totalLen);

        if (msgType === 'Z') {
          if (phase === 'startup') {
            phase = 'ready';
            resolve(session);
          } else if (queryCallback) {
            const cb = queryCallback;
            queryCallback = null;
            if (error) cb.reject(new Error(error));
            else cb.resolve({ rows: [...rows], tag, columns: [...columns] });
          }
        } else if (msgType === 'T') {
          const numCols = payload.readInt16BE(5);
          columns = [];
          let offset = 7;
          for (let i = 0; i < numCols; i++) {
            const nameEnd = payload.indexOf(0, offset);
            columns.push(payload.toString('utf8', offset, nameEnd));
            offset = nameEnd + 1 + 18;
          }
        } else if (msgType === 'D') {
          const numCols2 = payload.readInt16BE(5);
          const row = {};
          let off = 7;
          for (let i = 0; i < numCols2; i++) {
            const colLen = payload.readInt32BE(off);
            off += 4;
            if (colLen === -1) { row[columns[i]] = null; }
            else {
              const val = payload.toString('utf8', off, off + colLen);
              row[columns[i]] = isNaN(val) || val === '' ? val : Number(val);
              off += colLen;
            }
          }
          rows.push(row);
        } else if (msgType === 'C') {
          tag = payload.toString('utf8', 5, payload.indexOf(0, 5));
        } else if (msgType === 'E') {
          let eoff = 5;
          while (eoff < payload.length) {
            const field = payload[eoff];
            if (field === 0) break;
            eoff++;
            const end = payload.indexOf(0, eoff);
            if (field === 0x4D) error = payload.toString('utf8', eoff, end);
            eoff = end + 1;
          }
        }
      }
    }

    const session = {
      query(sql) {
        return new Promise((resolveQ, rejectQ) => {
          rows = []; columns = []; error = null; tag = '';
          queryCallback = { resolve: resolveQ, reject: rejectQ };
          const qBuf = Buffer.from(sql + '\0', 'utf8');
          const msg = Buffer.alloc(1 + 4 + qBuf.length);
          msg[0] = 0x51;
          msg.writeInt32BE(4 + qBuf.length, 1);
          qBuf.copy(msg, 5);
          socket.write(msg);
        });
      },
      close() {
        const term = Buffer.alloc(5);
        term[0] = 0x58;
        term.writeInt32BE(4, 1);
        socket.write(term);
        socket.end();
      }
    };

    setTimeout(() => reject(new Error('Connect timeout')), 5000);
  });
}

describe('Concurrent PG Protocol Server', () => {
  let dir, db, server, port;

  before(async () => {
    dir = join(tmpdir(), `henrydb-pg-test-${Date.now()}`);
    mkdirSync(dir, { recursive: true });
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");

    port = 15432 + Math.floor(Math.random() * 1000);
    server = createServer((socket) => {
      const session = db.session();
      let txState = 'I';
      let state = 'init';
      let buf = Buffer.alloc(0);

      socket.on('data', (data) => {
        buf = Buffer.concat([buf, data]);
        while (buf.length >= 4) {
          if (state === 'init') {
            if (buf.length >= 8) {
              const len = buf.readInt32BE(0);
              const code = buf.readInt32BE(4);
              if (code === 80877103) { socket.write(Buffer.from('N')); buf = buf.subarray(len); continue; }
              if (buf.length >= len) {
                buf = buf.subarray(len);
                socket.write(writeAuthenticationOk());
                socket.write(writeParameterStatus('server_version', '15.0'));
                socket.write(writeParameterStatus('server_encoding', 'UTF8'));
                socket.write(writeParameterStatus('client_encoding', 'UTF8'));
                socket.write(writeBackendKeyData(1, 1));
                socket.write(writeReadyForQuery(txState));
                state = 'ready';
                continue;
              }
            }
            break;
          }
          if (state === 'ready') {
            if (buf.length < 5) break;
            const msgLen = buf.readInt32BE(1);
            const totalLen = 1 + msgLen;
            if (buf.length < totalLen) break;
            const msgType = String.fromCharCode(buf[0]);
            const msgBuf = buf.subarray(1, totalLen);
            buf = buf.subarray(totalLen);
            if (msgType === 'Q') {
              const sql = parseQueryMessage(msgBuf);
              const trimmed = sql.trim().toUpperCase();
              try {
                const result = session.execute(sql);
                if (trimmed.startsWith('BEGIN')) txState = 'T';
                if (trimmed === 'COMMIT' || trimmed === 'ROLLBACK') txState = 'I';
                if (result?.rows?.length > 0) {
                  const cols = Object.keys(result.rows[0]);
                  socket.write(writeRowDescription(cols.map(n => ({
                    name: n, typeOid: inferTypeOid(result.rows[0][n]),
                    typeSize: typeof result.rows[0][n] === 'number' ? 4 : -1,
                  }))));
                  for (const row of result.rows) socket.write(writeDataRow(cols.map(c => row[c])));
                  socket.write(writeCommandComplete(`SELECT ${result.rows.length}`));
                } else {
                  socket.write(writeCommandComplete(result?.message || 'OK'));
                }
              } catch (err) {
                socket.write(writeErrorResponse('ERROR', '42000', err.message));
                if (txState === 'T') txState = 'E';
              }
              socket.write(writeReadyForQuery(txState));
            } else if (msgType === 'X') { session.close(); socket.end(); }
          }
        }
      });
      socket.on('close', () => session.close());
    });

    await new Promise((res) => server.listen(port, '127.0.0.1', res));
  });

  after(() => {
    server.close();
    db.close();
    try { rmSync(dir, { recursive: true, force: true }); } catch (e) { /* ignore */ }
  });

  it('should serve a single query', async () => {
    const result = await pgQuery(port, 'SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('should serve two concurrent clients', async () => {
    const [r1, r2] = await Promise.all([
      pgQuery(port, 'SELECT * FROM users ORDER BY id'),
      pgQuery(port, 'SELECT * FROM users ORDER BY id'),
    ]);
    assert.equal(r1.rows.length, 2);
    assert.equal(r2.rows.length, 2);
  });

  it('should isolate transactions between clients', async () => {
    const c1 = await createPGSession(port);
    const c2 = await createPGSession(port);

    await c1.query('BEGIN');
    await c1.query("INSERT INTO users VALUES (3, 'Charlie')");

    // c1 sees Charlie
    const r1 = await c1.query('SELECT * FROM users ORDER BY id');
    assert.equal(r1.rows.length, 3);

    // c2 doesn't see Charlie
    const r2 = await c2.query('SELECT * FROM users ORDER BY id');
    assert.equal(r2.rows.length, 2);

    await c1.query('COMMIT');

    // c2 now sees Charlie
    const r3 = await c2.query('SELECT * FROM users ORDER BY id');
    assert.equal(r3.rows.length, 3);

    c1.close();
    c2.close();
  });

  it('should handle rollback over PG protocol', async () => {
    const c1 = await createPGSession(port);

    await c1.query('BEGIN');
    await c1.query("INSERT INTO users VALUES (99, 'Phantom')");
    const r1 = await c1.query('SELECT * FROM users WHERE id = 99');
    assert.equal(r1.rows.length, 1);
    
    await c1.query('ROLLBACK');
    const r2 = await c1.query('SELECT * FROM users WHERE id = 99');
    assert.equal(r2.rows.length, 0);

    c1.close();
  });
});
