// pool-proxy.test.js — Tests for N:M connection pool proxy
import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { Database } from './db.js';
import { PoolProxy } from './pool-proxy.js';

let db, proxy;
const PORT = 16433;

function createDb() {
  const d = new Database();
  d.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)');
  d.execute("INSERT INTO items VALUES (1, 'alpha', 10)");
  d.execute("INSERT INTO items VALUES (2, 'beta', 20)");
  d.execute("INSERT INTO items VALUES (3, 'gamma', 30)");
  return d;
}

/**
 * Connect to the proxy as a PostgreSQL client.
 * Returns { socket, query, close }
 */
function connectClient(port = PORT) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(port, '127.0.0.1');
    let buffer = Buffer.alloc(0);
    let readyResolve = null;
    let queryResolve = null;
    let rows = [];
    let fields = [];

    socket.on('connect', () => {
      // Send startup message
      const user = Buffer.from('user\0test\0database\0henrydb\0\0');
      const len = 4 + 4 + user.length;
      const buf = Buffer.alloc(len);
      buf.writeInt32BE(len, 0);
      buf.writeInt32BE(196608, 4); // v3.0
      user.copy(buf, 8);
      socket.write(buf);
    });

    socket.on('data', (data) => {
      buffer = Buffer.concat([buffer, data]);
      processMessages();
    });

    function processMessages() {
      while (buffer.length >= 5) {
        const msgType = String.fromCharCode(buffer[0]);
        const msgLen = buffer.readInt32BE(1);
        if (buffer.length < 1 + msgLen) break;
        const body = buffer.subarray(5, 1 + msgLen);
        buffer = buffer.subarray(1 + msgLen);

        switch (msgType) {
          case 'R': // Authentication
            break;
          case 'S': // ParameterStatus
            break;
          case 'K': // BackendKeyData
            break;
          case 'Z': { // ReadyForQuery
            const status = String.fromCharCode(body[0]);
            if (readyResolve) {
              const r = readyResolve;
              readyResolve = null;
              r({ rows: [...rows], fields: [...fields], status });
              rows = [];
              fields = [];
            } else {
              // Initial ready — resolve the connect promise
              resolve({
                socket,
                query: (sql) => new Promise((res) => {
                  rows = [];
                  fields = [];
                  readyResolve = res;
                  // Send Query message
                  const sqlBuf = Buffer.from(sql + '\0', 'utf8');
                  const qLen = 4 + sqlBuf.length;
                  const qBuf = Buffer.alloc(1 + qLen);
                  qBuf[0] = 0x51; // 'Q'
                  qBuf.writeInt32BE(qLen, 1);
                  sqlBuf.copy(qBuf, 5);
                  socket.write(qBuf);
                }),
                close: () => {
                  // Send Terminate
                  const term = Buffer.alloc(5);
                  term[0] = 0x58; // 'X'
                  term.writeInt32BE(4, 1);
                  socket.write(term);
                  socket.destroy();
                },
              });
            }
            break;
          }
          case 'T': { // RowDescription
            const numFields = body.readInt16BE(0);
            let offset = 2;
            fields = [];
            for (let i = 0; i < numFields; i++) {
              const nullIdx = body.indexOf(0, offset);
              const name = body.subarray(offset, nullIdx).toString('utf8');
              fields.push(name);
              offset = nullIdx + 1 + 18; // skip fixed fields
            }
            break;
          }
          case 'D': { // DataRow
            const numCols = body.readInt16BE(0);
            let offset = 2;
            const row = {};
            for (let i = 0; i < numCols; i++) {
              const colLen = body.readInt32BE(offset);
              offset += 4;
              if (colLen === -1) {
                row[fields[i]] = null;
              } else {
                row[fields[i]] = body.subarray(offset, offset + colLen).toString('utf8');
                offset += colLen;
              }
            }
            rows.push(row);
            break;
          }
          case 'C': // CommandComplete
            break;
          case 'E': { // ErrorResponse
            // Parse error fields
            let errMsg = 'error';
            let offset = 0;
            while (offset < body.length) {
              const fieldType = body[offset++];
              if (fieldType === 0) break;
              const nullIdx = body.indexOf(0, offset);
              const val = body.subarray(offset, nullIdx).toString('utf8');
              if (fieldType === 0x4D) errMsg = val; // 'M' = Message
              offset = nullIdx + 1;
            }
            if (readyResolve) {
              // Error will be followed by ReadyForQuery
            }
            break;
          }
        }
      }
    }

    socket.on('error', reject);
    setTimeout(() => reject(new Error('connect timeout')), 3000);
  });
}

describe('PoolProxy', () => {
  beforeEach(async () => {
    db = createDb();
    proxy = new PoolProxy({ db, port: PORT, poolSize: 3, maxClients: 10 });
    await proxy.start();
  });

  afterEach(async () => {
    await proxy.stop();
  });

  test('single client query', async () => {
    const c = await connectClient();
    const r = await c.query('SELECT * FROM items ORDER BY id');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].name, 'alpha');
    assert.equal(r.status, 'I');
    c.close();
  });

  test('multiple clients share backends (N:M multiplexing)', async () => {
    // 5 clients but only 3 backends
    const clients = [];
    for (let i = 0; i < 5; i++) {
      clients.push(await connectClient());
    }

    // All 5 can query
    for (let i = 0; i < 5; i++) {
      const r = await clients[i].query('SELECT COUNT(*) as cnt FROM items');
      assert.equal(r.rows[0].cnt, '3');
    }

    const stats = proxy.getStats();
    assert.equal(stats.backends.total, 3); // Only 3 backends
    assert.ok(stats.clients.active >= 5); // 5 clients connected

    for (const c of clients) c.close();
  });

  test('transaction-mode pooling: backend held during transaction', async () => {
    const c = await connectClient();

    // Start transaction
    await c.query('BEGIN');

    const stats1 = proxy.getStats();
    assert.equal(stats1.backends.active, 1); // Backend held

    await c.query("INSERT INTO items VALUES (4, 'delta', 40)");
    await c.query('COMMIT');

    // After commit, backend should be released
    // Need a tick for state update
    const stats2 = proxy.getStats();
    assert.equal(stats2.backends.active, 0);

    // Verify the insert worked
    const r = await c.query('SELECT * FROM items WHERE id = 4');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'delta');

    c.close();
  });

  test('session-mode pooling: backend held for entire session', async () => {
    await proxy.stop();
    proxy = new PoolProxy({ db, port: PORT, poolSize: 2, maxClients: 10, poolMode: 'session' });
    await proxy.start();

    const c1 = await connectClient();
    const c2 = await connectClient();

    await c1.query('SELECT 1 as x');
    await c2.query('SELECT 2 as x');

    const stats = proxy.getStats();
    assert.equal(stats.backends.active, 2); // Each client holds a backend
    assert.equal(stats.pool.mode, 'session');

    c1.close();
    c2.close();
  });

  test('statement-mode pooling: backend released after every statement', async () => {
    await proxy.stop();
    proxy = new PoolProxy({ db, port: PORT, poolSize: 2, maxClients: 10, poolMode: 'statement' });
    await proxy.start();

    const c = await connectClient();
    await c.query('SELECT 1 as x');

    const stats = proxy.getStats();
    assert.equal(stats.backends.active, 0); // Released immediately
    assert.equal(stats.pool.mode, 'statement');

    c.close();
  });

  test('pool utilization tracking', async () => {
    const stats0 = proxy.getStats();
    assert.equal(stats0.backends.total, 3);
    assert.equal(stats0.backends.idle, 3);
    assert.equal(stats0.pool.utilizationPct, 0);

    const c1 = await connectClient();
    const c2 = await connectClient();

    // Start transactions to hold backends
    await c1.query('BEGIN');
    await c2.query('BEGIN');

    const stats1 = proxy.getStats();
    assert.equal(stats1.backends.active, 2);
    assert.equal(stats1.backends.idle, 1);
    assert.ok(stats1.pool.utilizationPct > 0.5);
    assert.equal(stats1.pool.ratio, '2:3');

    await c1.query('COMMIT');
    await c2.query('COMMIT');
    c1.close();
    c2.close();
  });

  test('query metrics tracked', async () => {
    const c = await connectClient();
    await c.query('SELECT 1 as x');
    await c.query('SELECT 2 as x');
    await c.query('SELECT 3 as x');

    const stats = proxy.getStats();
    assert.equal(stats.queries.total, 3);
    assert.equal(stats.queries.errors, 0);
    c.close();
  });

  test('client disconnect releases backend and clears transaction state', async () => {
    const c = await connectClient();
    await c.query('BEGIN');

    const stats1 = proxy.getStats();
    assert.equal(stats1.backends.active, 1); // Backend held during tx

    c.close();
    await new Promise(r => setTimeout(r, 50));

    // Backend should be released after disconnect
    const stats2 = proxy.getStats();
    assert.equal(stats2.backends.active, 0);
    assert.equal(stats2.clients.active, 0);
    // Backend is available for reuse
    assert.equal(stats2.backends.idle, 3);
  });

  test('waiting clients get backend when released', async () => {
    await proxy.stop();
    // Pool with only 1 backend
    proxy = new PoolProxy({ db, port: PORT, poolSize: 1, maxClients: 5 });
    await proxy.start();

    const c1 = await connectClient();
    await c1.query('BEGIN');

    // c1 holds the only backend. c2 connects and queries — should wait.
    const c2 = await connectClient();
    const queryPromise = c2.query('SELECT COUNT(*) as cnt FROM items');

    // Release the backend by committing
    await c1.query('COMMIT');
    c1.close();

    // c2's query should now complete
    const r = await queryPromise;
    assert.equal(r.rows[0].cnt, '3');

    c2.close();
  });

  test('SSL negotiation handled', async () => {
    // Connect with SSL request first (like psql does)
    const socket = net.createConnection(PORT, '127.0.0.1');
    await new Promise((resolve) => {
      socket.on('connect', () => {
        // Send SSL request
        const sslReq = Buffer.alloc(8);
        sslReq.writeInt32BE(8, 0);
        sslReq.writeInt32BE(80877103, 4);
        socket.write(sslReq);
      });
      socket.on('data', (data) => {
        // Should get 'N' (no SSL)
        assert.equal(data.toString(), 'N');
        socket.destroy();
        resolve();
      });
    });
  });

  test('DDL commands work through proxy', async () => {
    const c = await connectClient();
    await c.query('CREATE TABLE proxy_test (id INTEGER, val TEXT)');
    await c.query("INSERT INTO proxy_test VALUES (1, 'hello')");
    const r = await c.query('SELECT * FROM proxy_test');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'hello');
    await c.query('DROP TABLE proxy_test');
    c.close();
  });

  test('concurrent queries from multiple clients', async () => {
    const clients = [];
    for (let i = 0; i < 4; i++) {
      clients.push(await connectClient());
    }

    // All query simultaneously
    const results = await Promise.all(
      clients.map((c, i) => c.query(`SELECT ${i + 1} as num`))
    );

    for (let i = 0; i < 4; i++) {
      assert.equal(results[i].rows[0].num, String(i + 1));
    }

    for (const c of clients) c.close();
  });

  test('getStats returns complete metrics', async () => {
    const stats = proxy.getStats();
    assert.ok('clients' in stats);
    assert.ok('backends' in stats);
    assert.ok('pool' in stats);
    assert.ok('queries' in stats);
    assert.ok('waits' in stats);
    assert.equal(typeof stats.pool.utilization, 'string');
    assert.ok(stats.pool.utilization.endsWith('%'));
    assert.equal(stats.pool.mode, 'transaction');
  });
});
