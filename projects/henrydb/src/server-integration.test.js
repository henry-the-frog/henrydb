// server-integration.test.js — End-to-end integration tests for HenryDB server
// Tests the full stack: TCP → wire protocol → SQL parser → planner → execution
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';

// ===== Helpers =====

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

    // Send startup
    const params = `user\0test\0database\0test\0\0`;
    const paramsBuf = Buffer.from(params, 'utf8');
    const len = 4 + 4 + paramsBuf.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    buf.writeInt32BE(196608, 4);
    paramsBuf.copy(buf, 8);
    this.socket.write(buf);

    // Wait for ReadyForQuery
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
    if (this._hasReadyForQuery()) {
      return Promise.resolve(this._consumeMessages());
    }
    return new Promise((resolve) => {
      this._resolve = resolve;
      setTimeout(() => {
        if (this._resolve === resolve) {
          this._resolve = null;
          resolve(this._consumeMessages());
        }
      }, 3000);
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

    // Parse into usable result
    const result = { columns: [], rows: [], error: null, tag: null, txStatus: null };
    for (const msg of msgs) {
      switch (msg.type) {
        case 'T': result.columns = this._parseColumns(msg.body); break;
        case 'D': result.rows.push(this._parseRow(msg.body)); break;
        case 'C': result.tag = msg.body.toString('utf8', 4).replace(/\0/g, ''); break;
        case 'E': {
          const text = msg.body.toString('utf8', 4);
          const m = text.match(/M([^\0]+)/);
          result.error = m ? m[1] : 'Unknown error';
          break;
        }
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
      const fieldLen = body.readInt32BE(off);
      off += 4;
      if (fieldLen === -1) { values.push(null); }
      else { values.push(body.toString('utf8', off, off + fieldLen)); off += fieldLen; }
    }
    return values;
  }
}

describe('HenryDB Server Integration', () => {
  let server;
  const PORT = 15434;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('full DDL + DML lifecycle', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    // CREATE TABLE
    let r = await client.query('CREATE TABLE products (id INTEGER, name TEXT, price REAL, in_stock INTEGER)');
    assert.ok(!r.error, `CREATE TABLE failed: ${r.error}`);

    // INSERT multiple rows
    r = await client.query("INSERT INTO products VALUES (1, 'Widget', 9.99, 100)");
    assert.ok(!r.error);
    r = await client.query("INSERT INTO products VALUES (2, 'Gadget', 24.99, 50)");
    assert.ok(!r.error);
    r = await client.query("INSERT INTO products VALUES (3, 'Doohickey', 4.99, 200)");
    assert.ok(!r.error);
    r = await client.query("INSERT INTO products VALUES (4, 'Thingamajig', 14.99, 0)");
    assert.ok(!r.error);

    // SELECT with WHERE
    r = await client.query('SELECT name, price FROM products WHERE price > 10 ORDER BY price');
    assert.strictEqual(r.rows.length, 2);
    assert.ok(r.columns.includes('name'));

    // UPDATE
    r = await client.query('UPDATE products SET in_stock = 75 WHERE id = 2');
    assert.ok(!r.error);

    // Verify UPDATE
    r = await client.query('SELECT in_stock FROM products WHERE id = 2');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0][r.columns.indexOf('in_stock')], '75');

    // DELETE
    r = await client.query('DELETE FROM products WHERE in_stock = 0');
    assert.ok(!r.error);

    // Verify DELETE
    r = await client.query('SELECT COUNT(*) AS cnt FROM products');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0][0], '3');

    client.close();
  });

  it('aggregate functions', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    const r = await client.query(`
      SELECT 
        COUNT(*) AS cnt,
        SUM(price) AS total_price,
        AVG(price) AS avg_price,
        MIN(price) AS cheapest,
        MAX(price) AS most_expensive
      FROM products
    `);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0][r.columns.indexOf('cnt')], '3');

    client.close();
  });

  it('multi-table JOIN with GROUP BY', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    // Setup
    await client.query('CREATE TABLE categories (id INTEGER, name TEXT)');
    await client.query("INSERT INTO categories VALUES (1, 'Electronics')");
    await client.query("INSERT INTO categories VALUES (2, 'Home')");

    await client.query('CREATE TABLE cat_products (id INTEGER, cat_id INTEGER, name TEXT, price REAL)');
    await client.query("INSERT INTO cat_products VALUES (1, 1, 'Phone', 599)");
    await client.query("INSERT INTO cat_products VALUES (2, 1, 'Laptop', 999)");
    await client.query("INSERT INTO cat_products VALUES (3, 2, 'Lamp', 29)");
    await client.query("INSERT INTO cat_products VALUES (4, 2, 'Chair', 149)");

    const r = await client.query(`
      SELECT c.name AS category, COUNT(*) AS cnt, SUM(p.price) AS total
      FROM categories c
      JOIN cat_products p ON c.id = p.cat_id
      GROUP BY c.name
      ORDER BY c.name
    `);
    assert.strictEqual(r.rows.length, 2);
    // Electronics should have 2 items totaling 1598
    const elecRow = r.rows.find(row => row.includes('Electronics'));
    assert.ok(elecRow, 'Expected Electronics row');

    client.close();
  });

  it('subqueries', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    const r = await client.query(`
      SELECT name, price FROM cat_products 
      WHERE price > (SELECT AVG(price) FROM cat_products)
      ORDER BY price
    `);
    assert.ok(!r.error, `Subquery failed: ${r.error}`);
    // Avg is ~444, so Phone (599) and Laptop (999) qualify
    assert.strictEqual(r.rows.length, 2);

    client.close();
  });

  it('transactions with proper status tracking', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    // Should start idle
    let r = await client.query('SELECT 1 AS x');
    assert.strictEqual(r.txStatus, 'I');

    // BEGIN → in transaction
    r = await client.query('BEGIN');
    assert.strictEqual(r.txStatus, 'T');

    // Operations in transaction
    r = await client.query('CREATE TABLE tx_test (id INTEGER, val TEXT)');
    assert.strictEqual(r.txStatus, 'T');

    r = await client.query("INSERT INTO tx_test VALUES (1, 'hello')");
    assert.strictEqual(r.txStatus, 'T');

    // COMMIT → idle
    r = await client.query('COMMIT');
    assert.strictEqual(r.txStatus, 'I');

    // Data should be visible
    r = await client.query('SELECT * FROM tx_test');
    assert.strictEqual(r.rows.length, 1);

    client.close();
  });

  it('error recovery — query after error', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    // Cause an error
    let r = await client.query('SELECT * FROM totally_fake_table');
    assert.ok(r.error, 'Expected an error');

    // Should still be able to query after error
    r = await client.query('SELECT 1 AS num');
    assert.ok(!r.error, `Query after error failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 1);

    client.close();
  });

  it('CREATE INDEX and indexed query', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE indexed_items (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 0; i < 20; i++) {
      await client.query(`INSERT INTO indexed_items VALUES (${i}, 'item_${i}', ${i * 10})`);
    }

    let r = await client.query('CREATE INDEX idx_score ON indexed_items (score)');
    assert.ok(!r.error, `CREATE INDEX failed: ${r.error}`);

    r = await client.query('SELECT name FROM indexed_items WHERE score >= 150 ORDER BY name');
    assert.ok(!r.error);
    assert.ok(r.rows.length > 0);

    client.close();
  });

  it('EXPLAIN shows query plan', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    const r = await client.query('EXPLAIN SELECT * FROM products WHERE price > 10');
    assert.ok(!r.error, `EXPLAIN failed: ${r.error}`);
    assert.ok(r.rows.length > 0, 'EXPLAIN should return rows');

    client.close();
  });

  it('handles large result sets', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE large_test (id INTEGER, val TEXT)');
    // Insert 100 rows
    for (let i = 0; i < 100; i++) {
      await client.query(`INSERT INTO large_test VALUES (${i}, 'row_${i}')`);
    }

    const r = await client.query('SELECT * FROM large_test ORDER BY id');
    assert.ok(!r.error);
    assert.strictEqual(r.rows.length, 100);
    assert.strictEqual(r.rows[0][0], '0');
    assert.strictEqual(r.rows[99][0], '99');

    client.close();
  });

  it('multiple concurrent clients', async () => {
    const c1 = new PgClient(PORT);
    const c2 = new PgClient(PORT);
    const c3 = new PgClient(PORT);
    await c1.connect();
    await c2.connect();
    await c3.connect();

    // All three clients query simultaneously
    const [r1, r2, r3] = await Promise.all([
      c1.query('SELECT COUNT(*) AS c FROM products'),
      c2.query('SELECT COUNT(*) AS c FROM categories'),
      c3.query('SELECT COUNT(*) AS c FROM large_test'),
    ]);

    assert.ok(!r1.error);
    assert.ok(!r2.error);
    assert.ok(!r3.error);
    assert.strictEqual(r1.rows[0][0], '3');
    assert.strictEqual(r2.rows[0][0], '2');
    assert.strictEqual(r3.rows[0][0], '100');

    c1.close();
    c2.close();
    c3.close();
  });

  it('views through wire protocol', async () => {
    const client = new PgClient(PORT);
    await client.connect();

    let r = await client.query('CREATE VIEW expensive_products AS SELECT name, price FROM products WHERE price > 10');
    assert.ok(!r.error, `CREATE VIEW failed: ${r.error}`);

    r = await client.query('SELECT * FROM expensive_products ORDER BY price');
    assert.ok(!r.error, `SELECT from view failed: ${r.error}`);
    assert.ok(r.rows.length > 0);

    client.close();
  });
});
