// server-adaptive.test.js — Tests for adaptive engine integration via server
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';

class PgClient {
  constructor(port) {
    this.port = port;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this._resolve = null;
  }

  async connect() {
    this.socket = await new Promise((resolve, reject) => {
      const s = net.createConnection({ host: '127.0.0.1', port: this.port }, () => resolve(s));
      s.on('error', reject);
    });
    this.socket.on('data', (data) => {
      this.buffer = Buffer.concat([this.buffer, data]);
      if (this._resolve && this._hasReadyForQuery()) {
        const r = this._resolve;
        this._resolve = null;
        r(this._consumeMessages());
      }
    });

    const params = `user\0test\0database\0test\0\0`;
    const paramsBuf = Buffer.from(params, 'utf8');
    const len = 4 + 4 + paramsBuf.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    buf.writeInt32BE(196608, 4);
    paramsBuf.copy(buf, 8);
    this.socket.write(buf);
    await this._waitReady();
  }

  async query(sql) {
    const queryBuf = Buffer.from(sql + '\0', 'utf8');
    const len = 4 + queryBuf.length;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x51;
    buf.writeInt32BE(len, 1);
    queryBuf.copy(buf, 5);
    this.socket.write(buf);
    return this._waitReady();
  }

  close() {
    if (this.socket) {
      const buf = Buffer.alloc(5);
      buf[0] = 0x58;
      buf.writeInt32BE(4, 1);
      this.socket.write(buf);
      this.socket.destroy();
    }
  }

  _hasReadyForQuery() {
    for (let i = 0; i < this.buffer.length; i++) {
      if (this.buffer[i] === 0x5A) return true;
    }
    return false;
  }

  _waitReady() {
    if (this._hasReadyForQuery()) return Promise.resolve(this._consumeMessages());
    return new Promise((resolve) => {
      this._resolve = resolve;
      setTimeout(() => { if (this._resolve === resolve) { this._resolve = null; resolve(this._consumeMessages()); } }, 3000);
    });
  }

  _consumeMessages() {
    const msgs = [];
    let offset = 0;
    while (offset < this.buffer.length) {
      if (offset + 5 > this.buffer.length) break;
      const type = String.fromCharCode(this.buffer[offset]);
      const len = this.buffer.readInt32BE(offset + 1);
      const totalLen = 1 + len;
      if (offset + totalLen > this.buffer.length) break;
      msgs.push({ type, body: this.buffer.subarray(offset + 1, offset + totalLen) });
      offset += totalLen;
    }
    this.buffer = this.buffer.subarray(offset);
    const result = { columns: [], rows: [], error: null, tag: null, txStatus: null };
    for (const msg of msgs) {
      switch (msg.type) {
        case 'T': result.columns = this._parseColumns(msg.body); break;
        case 'D': result.rows.push(this._parseRow(msg.body)); break;
        case 'C': result.tag = msg.body.toString('utf8', 4).replace(/\0/g, ''); break;
        case 'E': { const m = msg.body.toString('utf8', 4).match(/M([^\0]+)/); result.error = m?.[1]; break; }
        case 'Z': result.txStatus = String.fromCharCode(msg.body[msg.body.length - 1]); break;
      }
    }
    return result;
  }

  _parseColumns(body) {
    const count = body.readInt16BE(4);
    const names = [];
    let off = 6;
    for (let i = 0; i < count; i++) {
      const nameEnd = body.indexOf(0, off);
      names.push(body.toString('utf8', off, nameEnd));
      off = nameEnd + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }
    return names;
  }

  _parseRow(body) {
    const count = body.readInt16BE(4);
    const values = [];
    let off = 6;
    for (let i = 0; i < count; i++) {
      const fieldLen = body.readInt32BE(off); off += 4;
      if (fieldLen === -1) { values.push(null); }
      else { values.push(body.toString('utf8', off, off + fieldLen)); off += fieldLen; }
    }
    return values;
  }
}

describe('Adaptive Engine via Server', () => {
  let server;
  const PORT = 15435;

  before(async () => {
    server = new HenryDBServer({ port: PORT, adaptive: true });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('serves SELECT queries through adaptive engine', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE items (id INTEGER, name TEXT, price REAL, qty INTEGER)');
    for (let i = 1; i <= 50; i++) {
      await client.query(`INSERT INTO items VALUES (${i}, 'item_${i}', ${(i * 3.14).toFixed(2)}, ${i * 10})`);
    }

    // Simple SELECT — adaptive engine should handle this
    const r = await client.query('SELECT name, price FROM items WHERE price > 100 ORDER BY price');
    assert.ok(!r.error, `Query failed: ${r.error}`);
    assert.ok(r.rows.length > 0, 'Expected results');
    assert.ok(r.columns.includes('name'));
    assert.ok(r.columns.includes('price'));

    client.close();
  });

  it('handles aggregation through standard path (not adaptive-eligible)', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    const r = await client.query('SELECT COUNT(*) AS total FROM items');
    assert.ok(!r.error, `Aggregate failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0][r.columns.indexOf('total')], '50');

    client.close();
  });

  it('handles JOIN through standard path (not adaptive-eligible)', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE suppliers (id INTEGER, name TEXT)');
    await client.query("INSERT INTO suppliers VALUES (1, 'Acme')");
    await client.query("INSERT INTO suppliers VALUES (2, 'Globex')");

    await client.query('CREATE TABLE supply (item_id INTEGER, supplier_id INTEGER)');
    await client.query('INSERT INTO supply VALUES (1, 1)');
    await client.query('INSERT INTO supply VALUES (2, 1)');
    await client.query('INSERT INTO supply VALUES (3, 2)');

    const r = await client.query(`
      SELECT s.name AS supplier, COUNT(*) AS item_count
      FROM suppliers s
      JOIN supply sp ON s.id = sp.supplier_id
      GROUP BY s.name
      ORDER BY s.name
    `);
    assert.ok(!r.error, `JOIN failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 2);

    client.close();
  });

  it('adaptive engine stats are populated', async () => {
    // After running queries above, the adaptive engine should have stats
    assert.ok(server.adaptiveEngine, 'Adaptive engine should exist');
    assert.ok(server.adaptiveEngine.stats.total > 0, 'Should have executed queries');
  });

  it('DDL still works through standard path', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    let r = await client.query('CREATE TABLE ddl_test (a INTEGER, b TEXT)');
    assert.ok(!r.error);

    r = await client.query("INSERT INTO ddl_test VALUES (1, 'x')");
    assert.ok(!r.error);

    r = await client.query('DROP TABLE ddl_test');
    assert.ok(!r.error);

    client.close();
  });

  it('falls back gracefully when adaptive engine cannot handle query', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    // Complex subquery that may not be optimizable by adaptive engine
    const r = await client.query('SELECT name FROM items WHERE id IN (SELECT item_id FROM supply)');
    assert.ok(!r.error, `Fallback query failed: ${r.error}`);
    assert.ok(r.rows.length > 0);

    client.close();
  });

  it('handles concurrent queries (mix of adaptive and standard)', async () => {
    const clients = [];
    for (let i = 0; i < 5; i++) {
      const c = new PgClient(PORT);
      await c.connect();
      clients.push(c);
    }

    // Mix of adaptive-eligible and standard queries
    const results = await Promise.all([
      clients[0].query(`SELECT name FROM items WHERE id > 45`), // adaptive eligible
      clients[1].query(`SELECT COUNT(*) AS c FROM items`), // not adaptive (aggregate)
      clients[2].query(`SELECT name, price FROM items WHERE price < 20 ORDER BY name`), // adaptive eligible
      clients[3].query(`SELECT name FROM items WHERE id = 1`), // adaptive eligible
      clients[4].query(`SELECT COUNT(*) AS c FROM items WHERE id > 40`), // not adaptive (aggregate)
    ]);

    for (const r of results) {
      assert.ok(!r.error, `Concurrent query failed: ${r.error}`);
      assert.ok(r.rows.length > 0, 'Expected results');
    }

    clients.forEach(c => c.close());
  });
});
