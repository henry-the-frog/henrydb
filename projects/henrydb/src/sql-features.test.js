// sql-features.test.js — Comprehensive SQL feature test through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('SQL Features via Wire Protocol', () => {
  let server, port, c;
  
  before(async () => {
    port = 32000 + Math.floor(Math.random() * 3000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, category TEXT, price INT, qty INT)');
    await c.query("INSERT INTO items VALUES (1, 'Widget', 'A', 1000, 50)");
    await c.query("INSERT INTO items VALUES (2, 'Gadget', 'B', 2000, 30)");
    await c.query("INSERT INTO items VALUES (3, 'Gizmo', 'A', 1500, 0)");
    await c.query("INSERT INTO items VALUES (4, 'Doohickey', 'C', 500, 100)");
    await c.query("INSERT INTO items VALUES (5, 'Thingamajig', 'B', 3000, 10)");
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('String concatenation (||)', async () => {
    const r = await c.query("SELECT name || ' ($' || CAST(price AS TEXT) || ')' as label FROM items WHERE id = 1");
    assert.equal(r.rows[0].label, 'Widget ($1000)');
  });

  it('CASE WHEN expression', async () => {
    const r = await c.query("SELECT name, CASE WHEN qty > 0 THEN 'In Stock' ELSE 'Out of Stock' END as status FROM items ORDER BY id");
    assert.equal(r.rows[0].status, 'In Stock');
    assert.equal(r.rows[2].status, 'Out of Stock');
  });

  it('COALESCE', async () => {
    await c.query('CREATE TABLE nullable (id INT, val TEXT)');
    await c.query('INSERT INTO nullable VALUES (1, NULL)');
    await c.query("INSERT INTO nullable VALUES (2, 'present')");
    
    const r = await c.query("SELECT COALESCE(val, 'missing') as result FROM nullable ORDER BY id");
    assert.equal(r.rows[0].result, 'missing');
    assert.equal(r.rows[1].result, 'present');
  });

  it('BETWEEN', async () => {
    const r = await c.query('SELECT COUNT(*) as cnt FROM items WHERE price BETWEEN 1000 AND 2000');
    assert.equal(String(r.rows[0].cnt), '3'); // Widget(1000), Gadget(2000), Gizmo(1500)
  });

  it('LIKE patterns', async () => {
    const r = await c.query("SELECT name FROM items WHERE name LIKE '%g%' ORDER BY name");
    assert.ok(r.rows.length >= 1); // Gadget, Gizmo, Thingamajig
  });

  it('OFFSET + LIMIT', async () => {
    const r = await c.query('SELECT name FROM items ORDER BY id LIMIT 2 OFFSET 2');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Gizmo');
  });

  it('DISTINCT', async () => {
    const r = await c.query('SELECT DISTINCT category FROM items ORDER BY category');
    assert.equal(r.rows.length, 3);
  });

  it('COUNT DISTINCT', async () => {
    const r = await c.query('SELECT COUNT(DISTINCT category) as cnt FROM items');
    assert.equal(String(r.rows[0].cnt), '3');
  });

  it('Parameterized query ($1)', async () => {
    const r = await c.query('SELECT name, price FROM items WHERE category = $1 ORDER BY price', ['A']);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Widget');
  });

  it('GROUP BY + aggregates', async () => {
    const r = await c.query('SELECT category, COUNT(*) as cnt, SUM(price) as total, AVG(price) as avg_price FROM items GROUP BY category ORDER BY total DESC');
    assert.equal(r.rows.length, 3);
  });

  it('HAVING', async () => {
    const r = await c.query('SELECT category, SUM(price) as total FROM items GROUP BY category HAVING SUM(price) > 2000 ORDER BY total DESC');
    assert.ok(r.rows.length >= 1);
  });

  it('Subquery in WHERE', async () => {
    const r = await c.query('SELECT name FROM items WHERE price > (SELECT AVG(price) FROM items) ORDER BY price');
    assert.ok(r.rows.length >= 1);
  });

  it('EXISTS subquery', async () => {
    await c.query('CREATE TABLE orders (id INT, item_id INT)');
    await c.query('INSERT INTO orders VALUES (1, 1)');
    await c.query('INSERT INTO orders VALUES (2, 3)');
    
    const r = await c.query('SELECT name FROM items WHERE EXISTS (SELECT 1 FROM orders WHERE orders.item_id = items.id) ORDER BY name');
    assert.equal(r.rows.length, 2);
  });

  it('IN subquery', async () => {
    const r = await c.query('SELECT name FROM items WHERE id IN (SELECT item_id FROM orders) ORDER BY name');
    assert.equal(r.rows.length, 2);
  });

  it('LEFT JOIN', async () => {
    const r = await c.query('SELECT i.name, o.id as order_id FROM items i LEFT JOIN orders o ON i.id = o.item_id ORDER BY i.id');
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[1].order_id, null); // Gadget has no orders
  });

  it('Self-join', async () => {
    const r = await c.query("SELECT a.name as item1, b.name as item2 FROM items a JOIN items b ON a.category = b.category AND a.id < b.id ORDER BY a.name, b.name");
    assert.ok(r.rows.length >= 1);
  });

  it('ORDER BY multiple columns', async () => {
    const r = await c.query('SELECT category, name, price FROM items ORDER BY category ASC, price DESC');
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].category, 'A');
  });

  it('Arithmetic expressions', async () => {
    const r = await c.query('SELECT name, price * qty as total_value, price + 100 as adjusted FROM items WHERE id = 1');
    assert.equal(String(r.rows[0].total_value), '50000');
    assert.equal(String(r.rows[0].adjusted), '1100');
  });

  it('IS NULL / IS NOT NULL', async () => {
    const r1 = await c.query('SELECT COUNT(*) as cnt FROM nullable WHERE val IS NULL');
    assert.equal(String(r1.rows[0].cnt), '1');
    
    const r2 = await c.query('SELECT COUNT(*) as cnt FROM nullable WHERE val IS NOT NULL');
    assert.equal(String(r2.rows[0].cnt), '1');
  });

  it('NOT IN', async () => {
    const r = await c.query("SELECT name FROM items WHERE category NOT IN ('A', 'B') ORDER BY name");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Doohickey');
  });

  it('CREATE TABLE AS SELECT', async () => {
    await c.query('CREATE TABLE expensive AS SELECT name, price FROM items WHERE price > 1500');
    const r = await c.query('SELECT * FROM expensive ORDER BY price DESC');
    assert.ok(r.rows.length >= 2);
  });

  it('Escaped single quotes', async () => {
    await c.query("INSERT INTO nullable VALUES (3, 'it''s fine')");
    const r = await c.query('SELECT val FROM nullable WHERE id = $1', [3]);
    assert.equal(r.rows[0].val, "it's fine");
  });
});
