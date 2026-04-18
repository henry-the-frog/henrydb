// pg-integration.test.js — Comprehensive integration test: HenryDB features via pg npm client
import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { createPgServer } from './pg-server.js';
import { Database } from './db.js';

const BASE_PORT = 18500 + Math.floor(Math.random() * 500);
let portCounter = 0;
let db, server, port;
const sockets = [];

describe('PG Wire Integration', () => {
  beforeEach(async () => {
    db = new Database();
    port = BASE_PORT + portCounter++;
    sockets.length = 0;
    server = createPgServer(db, port);
    server.on('connection', s => { sockets.push(s); s.on('close', () => { const i = sockets.indexOf(s); if (i >= 0) sockets.splice(i, 1); }); });
    await new Promise(r => setTimeout(r, 100));
  });

  afterEach(async () => {
    await new Promise(r => setTimeout(r, 50));
    for (const s of sockets) { try { s.destroy(); } catch {} }
    sockets.length = 0;
    await new Promise(r => server.close(r));
  });

  function cfg() { return { host: 'localhost', port, user: 'henrydb', database: 'henrydb', password: '' }; }

  test('INSERT RETURNING *', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)');
    const r = await client.query("INSERT INTO t (name) VALUES ('Alice') RETURNING *");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
    assert.ok(r.rows[0].id);
    await client.end();
  });

  test('INSERT RETURNING with params', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)');
    const r = await client.query('INSERT INTO t (name) VALUES ($1) RETURNING id, name', ['Bob']);
    assert.equal(r.rows[0].name, 'Bob');
    await client.end();
  });

  test('ON CONFLICT DO NOTHING', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    await client.query("INSERT INTO t VALUES (1, 'Alice')");
    await client.query("INSERT INTO t VALUES (1, 'Bob') ON CONFLICT DO NOTHING");
    const r = await client.query('SELECT name FROM t WHERE id = 1');
    assert.equal(r.rows[0].name, 'Alice'); // Not Bob
    await client.end();
  });

  test('ON CONFLICT DO UPDATE (UPSERT)', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    await client.query("INSERT INTO t VALUES (1, 'Alice')");
    await client.query("INSERT INTO t VALUES (1, 'Bob') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name");
    const r = await client.query('SELECT name FROM t WHERE id = 1');
    assert.equal(r.rows[0].name, 'Bob'); // Updated
    await client.end();
  });

  test('aggregate functions: COUNT, SUM, AVG, MIN, MAX', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE nums (val INT)');
    await client.query('INSERT INTO nums VALUES (10), (20), (30), (40), (50)');
    const r = await client.query('SELECT COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg, MIN(val) as lo, MAX(val) as hi FROM nums');
    assert.equal(r.rows[0].cnt, '5');
    assert.equal(r.rows[0].total, '150');
    assert.equal(r.rows[0].lo, '10');
    assert.equal(r.rows[0].hi, '50');
    await client.end();
  });

  test('GROUP BY with HAVING', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE sales (dept TEXT, amount INT)');
    await client.query("INSERT INTO sales VALUES ('Eng', 100), ('Eng', 200), ('Sales', 50), ('Sales', 60), ('HR', 10)");
    const r = await client.query('SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING SUM(amount) > 100 ORDER BY total DESC');
    assert.equal(r.rows.length, 2); // Eng and Sales
    assert.equal(r.rows[0].dept, 'Eng');
    await client.end();
  });

  test('window functions: RANK, ROW_NUMBER', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE emp (name TEXT, salary INT)');
    await client.query("INSERT INTO emp VALUES ('Alice', 100), ('Bob', 90), ('Carol', 100), ('Dave', 80)");
    const r = await client.query('SELECT name, RANK() OVER (ORDER BY salary DESC) as rnk FROM emp ORDER BY rnk, name');
    assert.equal(r.rows[0].rnk, '1'); // Alice or Carol (tied at 100)
    await client.end();
  });

  test('subquery in WHERE', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    await client.query('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    const r = await client.query('SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id');
    assert.equal(r.rows.length, 1); // Only id=3 (30 > 20)
    assert.equal(r.rows[0].id, '3');
    await client.end();
  });

  test('JOIN with cost-based method selection', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE depts (id INT PRIMARY KEY, name TEXT)');
    await client.query('CREATE TABLE emps (id INT PRIMARY KEY, dept_id INT, name TEXT)');
    await client.query("INSERT INTO depts VALUES (1, 'Eng'), (2, 'Sales')");
    await client.query("INSERT INTO emps VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')");
    const r = await client.query('SELECT d.name as dept, e.name as emp FROM depts d JOIN emps e ON d.id = e.dept_id ORDER BY d.name, e.name');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].dept, 'Eng');
    await client.end();
  });

  test('LEFT JOIN with NULLs', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    await client.query('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    await client.query("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    await client.query("INSERT INTO b VALUES (1, 1)");
    const r = await client.query('SELECT a.val, b.id as b_id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[1].b_id, null); // id=2 has no match
    await client.end();
  });

  test('UDF through wire protocol', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query("CREATE FUNCTION double(x INT) RETURNS INT AS $$ SELECT x * 2 $$");
    const r = await client.query('SELECT double(21) as result');
    assert.equal(r.rows[0].result, '42');
    await client.end();
  });

  test('information_schema.tables', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    client.on('error', () => {}); // Suppress async errors during cleanup
    await client.connect();
    await client.query('CREATE TABLE users (id INT PRIMARY KEY)');
    await client.query('CREATE TABLE orders (id INT PRIMARY KEY)');
    const r = await client.query('SELECT table_name FROM information_schema.tables ORDER BY table_name');
    assert.equal(r.rows.length, 2);
    const names = r.rows.map(row => row.table_name).sort();
    assert.equal(names[0], 'orders');
    assert.equal(names[1], 'users');
    await client.end();
  });

  test('information_schema.columns', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)');
    const r = await client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'none' OR 1=1 ORDER BY ordinal_position");
    // All columns from t
    const names = r.rows.map(row => row.column_name);
    assert.ok(names.includes('id'));
    assert.ok(names.includes('name'));
    await client.end();
  });

  test('SET and SHOW commands', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('SET client_encoding TO utf8');
    const r = await client.query('SHOW server_version');
    assert.ok(r.rows[0].server_version.includes('HenryDB'));
    await client.end();
  });

  test('version() and current_database()', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    const r1 = await client.query('SELECT version()');
    assert.ok(r1.rows[0].version.includes('HenryDB'));
    const r2 = await client.query('SELECT current_database()');
    assert.equal(r2.rows[0].current_database, 'henrydb');
    await client.end();
  });

  test('EXPLAIN through wire protocol', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    await client.query("INSERT INTO t VALUES (1, 'x')");
    const r = await client.query('EXPLAIN SELECT * FROM t WHERE id = 1');
    assert.ok(r.rows.length >= 1);
    await client.end();
  });

  test('string functions through wire', async () => {
    const client = new pg.Client(cfg()); client.on('error', () => {});
    await client.connect();
    await client.query('CREATE TABLE t (id INT, val TEXT)');
    await client.query("INSERT INTO t VALUES (1, 'hello world')");
    const r1 = await client.query('SELECT UPPER(val) as u FROM t');
    assert.equal(r1.rows[0].u, 'HELLO WORLD');
    const r2 = await client.query('SELECT LENGTH(val) as len FROM t');
    assert.equal(r2.rows[0].len, '11');
    await client.end();
  });
});
