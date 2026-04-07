// explain-pg.test.js — Test EXPLAIN via PG protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, createConnection } from 'node:net';
import { TransactionalDatabase } from './transactional-db.js';
import { buildPlan, explainPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseQueryMessage, inferTypeOid,
} from './pg-protocol.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Minimal single-query PG client
function pgQuery(port, sql) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    let buffer = Buffer.alloc(0);
    let phase = 'startup';
    const rows = [];
    let columns = [];
    let error = null;

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
            const qBuf = Buffer.from(sql + '\0', 'utf8');
            const msg = Buffer.alloc(1 + 4 + qBuf.length);
            msg[0] = 0x51;
            msg.writeInt32BE(4 + qBuf.length, 1);
            qBuf.copy(msg, 5);
            socket.write(msg);
          } else {
            const term = Buffer.alloc(5);
            term[0] = 0x58;
            term.writeInt32BE(4, 1);
            socket.write(term);
            socket.end();
            if (error) reject(new Error(error));
            else resolve({ rows, columns });
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
            if (colLen !== -1) {
              row[columns[i]] = payload.toString('utf8', off, off + colLen);
              off += colLen;
            }
          }
          rows.push(row);
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
    });

    socket.on('error', reject);
    setTimeout(() => { socket.destroy(); reject(new Error('Timeout')); }, 5000);
  });
}

describe('EXPLAIN via PG Protocol', () => {
  let dir, db, server, port;

  before(async () => {
    dir = join(tmpdir(), `henrydb-explain-test-${Date.now()}`);
    mkdirSync(dir, { recursive: true });
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

    port = 16432 + Math.floor(Math.random() * 1000);
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
                if (trimmed.startsWith('EXPLAIN ANALYZE')) {
                  const innerSql = sql.trim().replace(/^EXPLAIN\s+ANALYZE\s+/i, '');
                  const ast = parse(innerSql);
                  const plan = buildPlan(ast, db._db.tables, db._db.indexCatalog);
                  const vStart = performance.now();
                  const vRows = plan.toArray();
                  const vElapsed = (performance.now() - vStart).toFixed(2);
                  const sStart = performance.now();
                  const sResult = session.execute(innerSql);
                  const sElapsed = (performance.now() - sStart).toFixed(2);
                  const planStr = explainPlan(ast, db._db.tables, db._db.indexCatalog);
                  const lines = [
                    ...planStr.split('\n'),
                    '',
                    `Volcano: ${vRows.length} rows, ${vElapsed}ms`,
                    `Standard: ${sResult?.rows?.length || 0} rows, ${sElapsed}ms`,
                  ];
                  socket.write(writeRowDescription([{ name: 'QUERY PLAN', typeOid: 25, typeSize: -1 }]));
                  for (const line of lines) socket.write(writeDataRow([line]));
                  socket.write(writeCommandComplete(`EXPLAIN ${lines.length}`));
                } else if (trimmed.startsWith('EXPLAIN')) {
                  const innerSql = sql.trim().replace(/^EXPLAIN\s+/i, '');
                  const ast = parse(innerSql);
                  const planStr = explainPlan(ast, db._db.tables, db._db.indexCatalog);
                  const lines = planStr.split('\n');
                  socket.write(writeRowDescription([{ name: 'QUERY PLAN', typeOid: 25, typeSize: -1 }]));
                  for (const line of lines) socket.write(writeDataRow([line]));
                  socket.write(writeCommandComplete(`EXPLAIN ${lines.length}`));
                } else {
                  const result = session.execute(sql);
                  if (result?.rows?.length > 0) {
                    const cols = Object.keys(result.rows[0]);
                    socket.write(writeRowDescription(cols.map(n => ({
                      name: n, typeOid: inferTypeOid(result.rows[0][n]), typeSize: -1,
                    }))));
                    for (const row of result.rows) socket.write(writeDataRow(cols.map(c => row[c])));
                    socket.write(writeCommandComplete(`SELECT ${result.rows.length}`));
                  } else {
                    socket.write(writeCommandComplete(result?.message || 'OK'));
                  }
                }
              } catch (err) {
                socket.write(writeErrorResponse('ERROR', '42000', err.message));
              }
              socket.write(writeReadyForQuery(txState));
            } else if (msgType === 'X') { session.close(); socket.end(); }
          }
        }
      });
      socket.on('close', () => session.close());
    });

    await new Promise(res => server.listen(port, '127.0.0.1', res));
  });

  after(() => {
    server.close();
    db.close();
    try { rmSync(dir, { recursive: true, force: true }); } catch (e) {}
  });

  it('EXPLAIN returns plan tree', async () => {
    const result = await pgQuery(port, 'EXPLAIN SELECT * FROM users WHERE age > 25');
    assert.ok(result.rows.length > 0);
    assert.ok(result.columns.includes('QUERY PLAN'));
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('SeqScan'));
    assert.ok(plan.includes('Filter'));
  });

  it('EXPLAIN ANALYZE returns plan with timing', async () => {
    const result = await pgQuery(port, 'EXPLAIN ANALYZE SELECT * FROM users');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('SeqScan'));
    assert.ok(plan.includes('Volcano:'));
    assert.ok(plan.includes('Standard:'));
  });

  it('EXPLAIN shows correct operator for aggregate', async () => {
    const result = await pgQuery(port, 'EXPLAIN SELECT COUNT(*) as cnt FROM users');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('HashAggregate'));
  });

  it('regular queries still work alongside EXPLAIN', async () => {
    const result = await pgQuery(port, 'SELECT name FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
  });
});
