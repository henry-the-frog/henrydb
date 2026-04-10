// server-prepared.test.js — Tests for Extended Query Protocol (prepared statements, params, pipelining)
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client, Pool } = pg;

function getPort() {
  return 20000 + Math.floor(Math.random() * 10000);
}

async function startServer(port, opts = {}) {
  const server = new HenryDBServer({ port, ...opts });
  await server.start();
  return server;
}

async function connect(port) {
  const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
  await client.connect();
  return client;
}

describe('Extended Query Protocol', () => {
  let server, port;

  before(async () => {
    port = getPort();
    server = await startServer(port);
    const client = await connect(port);
    await client.query('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, qty INT)');
    await client.query("INSERT INTO products VALUES (1, 'Widget', 999, 100)");
    await client.query("INSERT INTO products VALUES (2, 'Gadget', 1999, 50)");
    await client.query("INSERT INTO products VALUES (3, 'Gizmo', 499, 200)");
    await client.query("INSERT INTO products VALUES (4, 'Doohickey', 2999, 25)");
    await client.query("INSERT INTO products VALUES (5, 'Thingamajig', 799, 75)");
    await client.end();
  });

  after(async () => {
    if (server) await server.stop();
  });

  describe('Parameterized Queries', () => {
    it('SELECT with integer parameter', async () => {
      const client = await connect(port);
      const r = await client.query('SELECT name, price FROM products WHERE id = $1', [3]);
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].name, 'Gizmo');
      await client.end();
    });

    it('SELECT with string parameter', async () => {
      const client = await connect(port);
      const r = await client.query('SELECT id, price FROM products WHERE name = $1', ['Widget']);
      assert.equal(r.rows.length, 1);
      assert.equal(String(r.rows[0].id), '1');
      await client.end();
    });

    it('SELECT with multiple parameters', async () => {
      const client = await connect(port);
      const r = await client.query('SELECT name FROM products WHERE price > $1 AND qty < $2 ORDER BY price', [500, 100]);
      assert.ok(r.rows.length >= 1);
      await client.end();
    });

    it('INSERT with parameters', async () => {
      const client = await connect(port);
      await client.query('INSERT INTO products VALUES ($1, $2, $3, $4)', [6, 'NewItem', 1500, 30]);
      const r = await client.query('SELECT name FROM products WHERE id = $1', [6]);
      assert.equal(r.rows[0].name, 'NewItem');
      await client.query('DELETE FROM products WHERE id = $1', [6]);
      await client.end();
    });

    it('UPDATE with parameters', async () => {
      const client = await connect(port);
      await client.query('UPDATE products SET price = $1 WHERE id = $2', [1099, 1]);
      const r = await client.query('SELECT price FROM products WHERE id = $1', [1]);
      assert.equal(String(r.rows[0].price), '1099');
      // Restore
      await client.query('UPDATE products SET price = $1 WHERE id = $2', [999, 1]);
      await client.end();
    });

    it('handles NULL parameter', async () => {
      const client = await connect(port);
      await client.query('INSERT INTO products VALUES ($1, $2, $3, $4)', [7, null, 0, 0]);
      const r = await client.query('SELECT name FROM products WHERE id = $1', [7]);
      assert.equal(r.rows[0].name, null);
      await client.query('DELETE FROM products WHERE id = $1', [7]);
      await client.end();
    });

    it('handles string with special characters', async () => {
      const client = await connect(port);
      await client.query('INSERT INTO products VALUES ($1, $2, $3, $4)', [8, "O'Reilly's Widget", 500, 10]);
      const r = await client.query('SELECT name FROM products WHERE id = $1', [8]);
      assert.equal(r.rows[0].name, "O'Reilly's Widget");
      await client.query('DELETE FROM products WHERE id = $1', [8]);
      await client.end();
    });
  });

  describe('Named Prepared Statements', () => {
    it('creates and reuses a named prepared statement', async () => {
      const client = await connect(port);
      
      // First execution creates the prepared statement
      const r1 = await client.query({ name: 'get_product', text: 'SELECT * FROM products WHERE id = $1', values: [1] });
      assert.equal(r1.rows[0].name, 'Widget');
      
      // Reuse (no re-parse)
      const r2 = await client.query({ name: 'get_product', values: [2] });
      assert.equal(r2.rows[0].name, 'Gadget');
      
      const r3 = await client.query({ name: 'get_product', values: [5] });
      assert.equal(r3.rows[0].name, 'Thingamajig');
      
      await client.end();
    });

    it('multiple named prepared statements on same connection', async () => {
      const client = await connect(port);
      
      const r1 = await client.query({ name: 'by_id', text: 'SELECT name FROM products WHERE id = $1', values: [1] });
      assert.equal(r1.rows[0].name, 'Widget');
      
      const r2 = await client.query({ name: 'by_name', text: 'SELECT id FROM products WHERE name = $1', values: ['Gizmo'] });
      assert.equal(String(r2.rows[0].id), '3');
      
      // Reuse both
      const r3 = await client.query({ name: 'by_id', values: [4] });
      assert.equal(r3.rows[0].name, 'Doohickey');
      
      const r4 = await client.query({ name: 'by_name', values: ['Gadget'] });
      assert.equal(String(r4.rows[0].id), '2');
      
      await client.end();
    });
  });

  describe('Connection Pool', () => {
    it('handles multiple pool clients concurrently', async () => {
      const pool = new Pool({ host: '127.0.0.1', port, user: 'test', database: 'testdb', max: 5 });
      
      // Run concurrent queries through pool
      const promises = [];
      for (let i = 1; i <= 5; i++) {
        promises.push(pool.query('SELECT name FROM products WHERE id = $1', [i]));
      }
      const results = await Promise.all(promises);
      
      assert.equal(results[0].rows[0].name, 'Widget');
      assert.equal(results[1].rows[0].name, 'Gadget');
      assert.equal(results[2].rows[0].name, 'Gizmo');
      assert.equal(results[3].rows[0].name, 'Doohickey');
      assert.equal(results[4].rows[0].name, 'Thingamajig');
      
      await pool.end();
    });

    it('pool with parameterized inserts and selects', async () => {
      const pool = new Pool({ host: '127.0.0.1', port, user: 'test', database: 'testdb', max: 3 });
      
      // Concurrent inserts
      await pool.query('CREATE TABLE pool_test (id INT PRIMARY KEY, val TEXT)');
      const inserts = [];
      for (let i = 1; i <= 10; i++) {
        inserts.push(pool.query('INSERT INTO pool_test VALUES ($1, $2)', [i, `value-${i}`]));
      }
      await Promise.all(inserts);
      
      // Verify
      const r = await pool.query('SELECT COUNT(*) as cnt FROM pool_test');
      assert.equal(String(r.rows[0].cnt), '10');
      
      await pool.query('DROP TABLE pool_test');
      await pool.end();
    });
  });

  describe('Edge Cases', () => {
    it('empty result set with parameters', async () => {
      const client = await connect(port);
      const r = await client.query('SELECT * FROM products WHERE id = $1', [999]);
      assert.equal(r.rows.length, 0);
      await client.end();
    });

    it('aggregate with parameters', async () => {
      const client = await connect(port);
      const r = await client.query('SELECT COUNT(*) as cnt FROM products WHERE price > $1', [1000]);
      assert.ok(parseInt(String(r.rows[0].cnt)) >= 0);
      await client.end();
    });

    it('many rapid parameterized queries', async () => {
      const client = await connect(port);
      for (let i = 0; i < 50; i++) {
        const id = 1 + (i % 5);
        const r = await client.query('SELECT name FROM products WHERE id = $1', [id]);
        assert.equal(r.rows.length, 1);
      }
      await client.end();
    });
  });
});
