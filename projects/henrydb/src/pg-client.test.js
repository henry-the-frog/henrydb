// pg-client.test.js — Test HenryDB PG wire protocol with real pg npm client
import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { createPgServer } from './pg-server.js';
import { Database } from './db.js';

const BASE_PORT = 16500 + Math.floor(Math.random() * 1000);
let portCounter = 0;
let db, server, port;
const connections = [];

describe('PG Client Compatibility', () => {
  beforeEach(() => {
    db = new Database();
    port = BASE_PORT + portCounter++;
    connections.length = 0;
    server = createPgServer(db, port);
    // Track connections for cleanup
    server.on('connection', (socket) => {
      connections.push(socket);
      socket.on('close', () => {
        const idx = connections.indexOf(socket);
        if (idx >= 0) connections.splice(idx, 1);
      });
    });
  });

  afterEach(async () => {
    // Small delay for pg client cleanup
    await new Promise(r => setTimeout(r, 50));
    // Force-close all sockets
    for (const s of connections) {
      try { s.destroy(); } catch {}
    }
    connections.length = 0;
    await new Promise(r => server.close(r));
  });

  function clientConfig() {
    return { host: 'localhost', port, user: 'henrydb', database: 'henrydb', password: '' };
  }

  test('pg.Client connects and runs simple query', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    const r = await client.query('SELECT 1+1 as result');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].result, '2');
    
    await client.end();
  });

  test('CREATE TABLE + INSERT + SELECT with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    await client.query("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
    
    const r = await client.query('SELECT * FROM users ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[1].name, 'Bob');
    
    await client.end();
  });

  test('parameterized SELECT with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    await client.query("INSERT INTO t VALUES (1, 'hello'), (2, 'world')");
    
    const r = await client.query('SELECT * FROM t WHERE id = $1', [1]);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'hello');
    
    await client.end();
  });

  test('parameterized INSERT with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    await client.query('INSERT INTO t VALUES ($1, $2)', [1, 'Alice']);
    await client.query('INSERT INTO t VALUES ($1, $2)', [2, 'Bob']);
    
    const r = await client.query('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    
    await client.end();
  });

  test('parameterized UPDATE and DELETE', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    await client.query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    
    await client.query('UPDATE t SET val = $1 WHERE id = $2', [99, 1]);
    const r1 = await client.query('SELECT val FROM t WHERE id = $1', [1]);
    assert.equal(r1.rows[0].val, 99);
    
    await client.query('DELETE FROM t WHERE id = $1', [2]);
    const r2 = await client.query('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r2.rows[0].cnt, '2');
    
    await client.end();
  });

  test('NULL parameter handling', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE t (id INT, val TEXT)');
    await client.query('INSERT INTO t VALUES ($1, $2)', [1, null]);
    
    const r = await client.query('SELECT * FROM t');
    assert.equal(r.rows[0].val, null);
    
    await client.end();
  });

  test('pg.Pool basic usage', async () => {
    const pool = new pg.Pool({ ...clientConfig(), max: 3 });
    
    await pool.query('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    await pool.query('INSERT INTO items VALUES ($1, $2)', [1, 'one']);
    await pool.query('INSERT INTO items VALUES ($1, $2)', [2, 'two']);
    
    const r = await pool.query('SELECT * FROM items ORDER BY id');
    assert.equal(r.rows.length, 2);
    
    await pool.end();
  });

  test('pg.Pool concurrent queries', async () => {
    const pool = new pg.Pool({ ...clientConfig(), max: 5 });
    
    await pool.query('CREATE TABLE nums (id INT PRIMARY KEY)');
    
    await Promise.all([
      pool.query('INSERT INTO nums VALUES ($1)', [1]),
      pool.query('INSERT INTO nums VALUES ($1)', [2]),
      pool.query('INSERT INTO nums VALUES ($1)', [3]),
      pool.query('INSERT INTO nums VALUES ($1)', [4]),
      pool.query('INSERT INTO nums VALUES ($1)', [5]),
    ]);
    
    const r = await pool.query('SELECT COUNT(*) as cnt FROM nums');
    assert.equal(r.rows[0].cnt, '5');
    
    await pool.end();
  });

  test('transaction with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    client.on('error', () => {}); // Suppress async errors during cleanup
    await client.connect();
    
    await client.query('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    await client.query("INSERT INTO accounts VALUES (1, 100), (2, 200)");
    
    await client.query('BEGIN');
    await client.query('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [50, 1]);
    await client.query('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [50, 2]);
    await client.query('COMMIT');
    
    const r = await client.query('SELECT * FROM accounts ORDER BY id');
    assert.equal(String(r.rows[0].balance), '50');
    assert.equal(String(r.rows[1].balance), '250');
    
    await client.end();
  });

  test('error handling with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await assert.rejects(
      () => client.query('SELECT * FROM nonexistent'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
    
    // Connection should still work after error
    const r = await client.query('SELECT 1 as alive');
    assert.equal(r.rows[0].alive, '1');
    
    await client.end();
  });

  test('multiple parameterized queries in sequence', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    
    for (let i = 0; i < 10; i++) {
      await client.query('INSERT INTO t VALUES ($1, $2)', [i, `item_${i}`]);
    }
    
    const r = await client.query('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, '10');
    
    // Query with different parameter types
    const r2 = await client.query('SELECT * FROM t WHERE name = $1', ['item_5']);
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].id, 5);
    
    await client.end();
  });

  test('aggregate queries with pg.Client', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE sales (amount INT)');
    await client.query('INSERT INTO sales VALUES (10), (20), (30), (40), (50)');
    
    const r = await client.query('SELECT SUM(amount) as total, AVG(amount) as avg_amt, COUNT(*) as cnt FROM sales');
    assert.equal(r.rows[0].total, '150');
    assert.equal(r.rows[0].cnt, '5');
    
    await client.end();
  });

  test('window functions through pg wire', async () => {
    const client = new pg.Client(clientConfig());
    await client.connect();
    
    await client.query('CREATE TABLE emp (name TEXT, dept TEXT, salary INT)');
    await client.query("INSERT INTO emp VALUES ('Alice', 'Eng', 100), ('Bob', 'Eng', 90), ('Carol', 'Sales', 80)");
    
    const r = await client.query('SELECT name, RANK() OVER (ORDER BY salary DESC) as rnk FROM emp');
    assert.equal(r.rows.length, 3);
    
    await client.end();
  });
});
