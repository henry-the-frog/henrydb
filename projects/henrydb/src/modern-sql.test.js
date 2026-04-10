// modern-sql.test.js — Tests for modern SQL features (PostgreSQL-like)
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Modern SQL Features', () => {
  let server, port, c;
  
  before(async () => {
    port = 33100 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('SERIAL auto-increment', async () => {
    await c.query('CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)');
    const r1 = await c.query("INSERT INTO items (name) VALUES ('first') RETURNING *");
    const r2 = await c.query("INSERT INTO items (name) VALUES ('second') RETURNING *");
    const r3 = await c.query("INSERT INTO items (name) VALUES ('third') RETURNING *");
    
    assert.equal(String(r1.rows[0].id), '1');
    assert.equal(String(r2.rows[0].id), '2');
    assert.equal(String(r3.rows[0].id), '3');
  });

  it('INSERT RETURNING specific columns', async () => {
    const r = await c.query("INSERT INTO items (name) VALUES ('fourth') RETURNING id");
    assert.equal(String(r.rows[0].id), '4');
    assert.equal(r.rows[0].name, undefined);
  });

  it('UPDATE RETURNING', async () => {
    const r = await c.query("UPDATE items SET name = 'updated' WHERE id = 1 RETURNING *");
    assert.equal(r.rows[0].name, 'updated');
    assert.equal(String(r.rows[0].id), '1');
  });

  it('DELETE RETURNING', async () => {
    const r = await c.query('DELETE FROM items WHERE id = 4 RETURNING *');
    assert.equal(r.rows[0].name, 'fourth');
    
    const count = await c.query('SELECT COUNT(*) as cnt FROM items');
    assert.equal(String(count.rows[0].cnt), '3');
  });

  it('INSERT ON CONFLICT DO UPDATE (upsert)', async () => {
    await c.query('CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)');
    await c.query("INSERT INTO kv VALUES ('a', 'original', '2024-01-01')");
    
    await c.query("INSERT INTO kv VALUES ('a', 'updated', '2024-06-01') ON CONFLICT (key) DO UPDATE SET value = 'updated'");
    
    const r = await c.query("SELECT value FROM kv WHERE key = 'a'");
    assert.equal(r.rows[0].value, 'updated');
  });

  it('INSERT ON CONFLICT DO NOTHING', async () => {
    await c.query("INSERT INTO kv VALUES ('a', 'should-not-apply', '2024-07-01') ON CONFLICT (key) DO NOTHING");
    
    const r = await c.query("SELECT value FROM kv WHERE key = 'a'");
    assert.equal(r.rows[0].value, 'updated'); // Unchanged
  });

  it('CTE with INSERT', async () => {
    await c.query('CREATE TABLE numbers (n INT)');
    // Non-recursive CTE
    await c.query('WITH data AS (SELECT 1 as n) INSERT INTO numbers SELECT n FROM data');
    
    const r = await c.query('SELECT * FROM numbers');
    assert.ok(r.rows.length >= 1);
  });

  it('Recursive CTE (counting)', async () => {
    const r = await c.query('WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT * FROM cnt');
    assert.equal(r.rows.length, 10);
  });

  it('CASE WHEN in SELECT', async () => {
    await c.query('CREATE TABLE scores (name TEXT, score INT)');
    await c.query("INSERT INTO scores VALUES ('Alice', 95)");
    await c.query("INSERT INTO scores VALUES ('Bob', 72)");
    await c.query("INSERT INTO scores VALUES ('Charlie', 45)");
    
    const r = await c.query("SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 50 THEN 'C' ELSE 'F' END as grade FROM scores ORDER BY name");
    assert.equal(r.rows[0].grade, 'A'); // Alice
    assert.equal(r.rows[1].grade, 'B'); // Bob
    assert.equal(r.rows[2].grade, 'F'); // Charlie
  });

  it('Correlated subquery', async () => {
    await c.query('CREATE TABLE emp (id INT, name TEXT, salary INT)');
    await c.query("INSERT INTO emp VALUES (1, 'Alice', 100)");
    await c.query("INSERT INTO emp VALUES (2, 'Bob', 200)");
    await c.query("INSERT INTO emp VALUES (3, 'Charlie', 150)");
    
    // Employees earning above average
    const r = await c.query('SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name');
    assert.equal(r.rows.length, 1); // Only Bob (200 > 150)
    assert.equal(r.rows[0].name, 'Bob');
  });

  it('CREATE TABLE AS with GROUP BY', async () => {
    await c.query("CREATE TABLE sales (product TEXT, amount INT)");
    await c.query("INSERT INTO sales VALUES ('A', 100)");
    await c.query("INSERT INTO sales VALUES ('A', 200)");
    await c.query("INSERT INTO sales VALUES ('B', 150)");
    
    await c.query('CREATE TABLE summary AS SELECT product, SUM(amount) as total, COUNT(*) as cnt FROM sales GROUP BY product');
    
    const r = await c.query('SELECT * FROM summary ORDER BY product');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].product, 'A');
    assert.equal(String(r.rows[0].total), '300');
  });

  it('String concatenation with ||', async () => {
    const r = await c.query("SELECT 'Hello' || ' ' || 'World' as greeting");
    assert.equal(r.rows[0].greeting, 'Hello World');
  });

  it('Complex query combining features', async () => {
    // Uses: subquery, CASE, aggregate, GROUP BY, HAVING, ORDER BY
    const r = await c.query(`
      SELECT product,
        SUM(amount) as total,
        COUNT(*) as cnt,
        CASE WHEN SUM(amount) > 200 THEN 'high' ELSE 'low' END as category
      FROM sales
      GROUP BY product
      HAVING COUNT(*) >= 1
      ORDER BY total DESC
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].category, 'high'); // A: 300
    assert.equal(r.rows[1].category, 'low');  // B: 150
  });
});
