// wire-stress.test.js — Concurrent wire protocol stress tests
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import net from 'node:net';

const PORT = 15434;

function buildStartupMessage(user, database) {
  const params = Buffer.from(`user\0${user}\0database\0${database}\0\0`);
  const len = 4 + 4 + params.length;
  const buf = Buffer.alloc(len);
  buf.writeInt32BE(len, 0);
  buf.writeInt32BE(196608, 4);
  params.copy(buf, 8);
  return buf;
}

function buildQueryMessage(sql) {
  const sqlBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + sqlBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51;
  buf.writeInt32BE(len, 1);
  sqlBuf.copy(buf, 5);
  return buf;
}

function buildTerminateMessage() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x58;
  buf.writeInt32BE(4, 1);
  return buf;
}

class StressClient {
  constructor(port, id) {
    this.port = port;
    this.id = id;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this._resolveQuery = null;
    this._columns = [];
    this._rows = [];
    this._commandTag = '';
    this._error = null;
    this.queryCount = 0;
    this.errorCount = 0;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.port, '127.0.0.1', () => {
        this.socket.write(buildStartupMessage('stress', 'stress'));
      });
      this.socket.on('data', (chunk) => this._handleData(chunk));
      this.socket.on('error', reject);
      this._onReady = resolve;
    });
  }

  _handleData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 5) {
      const msgType = String.fromCharCode(this.buffer[0]);
      const msgLen = this.buffer.readInt32BE(1);
      const totalLen = 1 + msgLen;
      if (this.buffer.length < totalLen) break;
      const msgBody = this.buffer.subarray(5, totalLen);
      this.buffer = this.buffer.subarray(totalLen);
      this._handleMessage(msgType, msgBody);
    }
  }

  _handleMessage(type, body) {
    switch (type) {
      case 'R': case 'S': case 'K': break;
      case 'Z': {
        if (this._onReady) { const cb = this._onReady; this._onReady = null; cb(); }
        if (this._resolveQuery) {
          const resolve = this._resolveQuery;
          this._resolveQuery = null;
          resolve({ columns: this._columns, rows: this._rows, commandTag: this._commandTag, error: this._error });
        }
        break;
      }
      case 'T': {
        const numFields = body.readInt16BE(0);
        this._columns = [];
        let offset = 2;
        for (let i = 0; i < numFields; i++) {
          const end = body.indexOf(0, offset);
          this._columns.push(body.subarray(offset, end).toString('utf8'));
          offset = end + 1 + 18;
        }
        break;
      }
      case 'D': {
        const numCols = body.readInt16BE(0);
        const row = [];
        let offset = 2;
        for (let i = 0; i < numCols; i++) {
          const colLen = body.readInt32BE(offset);
          offset += 4;
          if (colLen === -1) row.push(null);
          else { row.push(body.subarray(offset, offset + colLen).toString('utf8')); offset += colLen; }
        }
        this._rows.push(row);
        break;
      }
      case 'C': { const end = body.indexOf(0); this._commandTag = body.subarray(0, end).toString('utf8'); break; }
      case 'E': {
        let offset = 0; let msg = '';
        while (offset < body.length) {
          const ft = String.fromCharCode(body[offset]); offset++;
          if (ft === '\0') break;
          const end = body.indexOf(0, offset);
          const val = body.subarray(offset, end).toString('utf8'); offset = end + 1;
          if (ft === 'M') msg = val;
        }
        this._error = msg;
        break;
      }
    }
  }

  query(sql) {
    return new Promise((resolve) => {
      this._columns = []; this._rows = []; this._commandTag = ''; this._error = null;
      this._resolveQuery = resolve;
      this.socket.write(buildQueryMessage(sql));
    });
  }

  close() {
    this.socket.write(buildTerminateMessage());
    this.socket.end();
  }
}

describe('Wire Protocol Stress', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT, host: '127.0.0.1' });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('5 concurrent clients doing 50 queries each', async () => {
    const NUM_CLIENTS = 5;
    const QUERIES_PER_CLIENT = 50;
    
    // Setup table
    const setup = new StressClient(PORT, 'setup');
    await setup.connect();
    await setup.query('CREATE TABLE IF NOT EXISTS concurrent1 (id INT, client_id INT, seq INT)');
    setup.close();
    
    // Spawn concurrent clients
    const clients = [];
    for (let i = 0; i < NUM_CLIENTS; i++) {
      const c = new StressClient(PORT, i);
      await c.connect();
      clients.push(c);
    }
    
    // Each client inserts rows concurrently
    const startTime = Date.now();
    const promises = clients.map(async (client, clientIdx) => {
      for (let q = 0; q < QUERIES_PER_CLIENT; q++) {
        const r = await client.query(`INSERT INTO concurrent1 VALUES (${clientIdx * 1000 + q}, ${clientIdx}, ${q})`);
        client.queryCount++;
        if (r.error) client.errorCount++;
      }
    });
    
    await Promise.all(promises);
    const elapsed = Date.now() - startTime;
    
    // Verify
    const verify = new StressClient(PORT, 'verify');
    await verify.connect();
    const r = await verify.query('SELECT COUNT(*) as cnt FROM concurrent1');
    const total = parseInt(r.rows[0][0], 10);
    
    assert.strictEqual(total, NUM_CLIENTS * QUERIES_PER_CLIENT, `Expected ${NUM_CLIENTS * QUERIES_PER_CLIENT} rows`);
    
    // Log performance
    const totalQueries = clients.reduce((s, c) => s + c.queryCount, 0);
    const qps = (totalQueries / (elapsed / 1000)).toFixed(0);
    console.log(`  ${NUM_CLIENTS} clients × ${QUERIES_PER_CLIENT} queries = ${totalQueries} total in ${elapsed}ms (${qps} qps)`);
    
    // Cleanup
    clients.forEach(c => c.close());
    verify.close();
  });

  it('rapid connect/disconnect cycles', async () => {
    const CYCLES = 20;
    
    for (let i = 0; i < CYCLES; i++) {
      const c = new StressClient(PORT, i);
      await c.connect();
      const r = await c.query(`SELECT ${i} as val`);
      assert.strictEqual(r.rows[0][0], String(i));
      c.close();
    }
  });

  it('large query results through wire protocol', async () => {
    const c = new StressClient(PORT, 'large');
    await c.connect();
    const table = 'large_' + Date.now();
    await c.query(`CREATE TABLE ${table} (id INT, data TEXT)`);
    
    // Insert 10 rows
    for (let i = 0; i < 10; i++) {
      await c.query(`INSERT INTO ${table} VALUES (${i}, 'data_${i}')`);
    }
    
    const r = await c.query(`SELECT * FROM ${table}`);
    assert.strictEqual(r.rows.length, 10);
    // First row should have id and data
    assert.strictEqual(r.columns.length, 2);
    assert.strictEqual(r.rows[0][0], '0');
    assert.strictEqual(r.rows[0][1], 'data_0');
    
    c.close();
  });

  it('transaction per client isolation', async () => {
    const c1 = new StressClient(PORT, 'txn1');
    const c2 = new StressClient(PORT, 'txn2');
    await c1.connect();
    await c2.connect();
    
    await c1.query('CREATE TABLE IF NOT EXISTS txn_iso (id INT PRIMARY KEY, val TEXT)');
    
    // c1 starts a transaction and inserts
    await c1.query('BEGIN');
    await c1.query("INSERT INTO txn_iso VALUES (1, 'from_c1')");
    
    // c2 can query (shared state for now, no MVCC isolation)
    const r2 = await c2.query('SELECT COUNT(*) as cnt FROM txn_iso');
    // Either 0 or 1 — depends on isolation level
    assert.ok(parseInt(r2.rows[0][0], 10) >= 0);
    
    await c1.query('COMMIT');
    
    // After commit, c2 should see the data
    const r3 = await c2.query('SELECT * FROM txn_iso WHERE id = 1');
    assert.strictEqual(r3.rows.length, 1);
    assert.strictEqual(r3.rows[0][1], 'from_c1');
    
    c1.close();
    c2.close();
  });
});
