// server-pgdriver.test.js — Test HenryDB with the real Node.js 'pg' driver
// This is the ultimate integration test: a real PostgreSQL client library
// talking to HenryDB over the wire protocol.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client, Pool } = pg;
const PORT = 15437;

describe('HenryDB with pg driver', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('connects and runs simple query', async () => {
    const client = new Client({
      host: '127.0.0.1',
      port: PORT,
      user: 'test',
      database: 'test',
      // Disable SSL since HenryDB sends 'N' for SSL
    });
    await client.connect();

    const res = await client.query('SELECT 1 AS num');
    assert.strictEqual(res.rows.length, 1);
    assert.strictEqual(res.rows[0].num, 1);

    await client.end();
  });

  it('CREATE TABLE + INSERT + SELECT', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE pg_users (id INTEGER, name TEXT, email TEXT)');
    await client.query("INSERT INTO pg_users VALUES (1, 'Alice', 'alice@test.com')");
    await client.query("INSERT INTO pg_users VALUES (2, 'Bob', 'bob@test.com')");
    await client.query("INSERT INTO pg_users VALUES (3, 'Charlie', 'charlie@test.com')");

    const res = await client.query('SELECT * FROM pg_users ORDER BY id');
    assert.strictEqual(res.rows.length, 3);
    assert.strictEqual(res.rows[0].name, 'Alice');
    assert.strictEqual(res.rows[2].name, 'Charlie');

    await client.end();
  });

  it('parameterized queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const res = await client.query('SELECT name, email FROM pg_users WHERE id = $1', [2]);
    assert.strictEqual(res.rows.length, 1);
    assert.strictEqual(res.rows[0].name, 'Bob');
    assert.strictEqual(res.rows[0].email, 'bob@test.com');

    await client.end();
  });

  it('parameterized INSERT', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('INSERT INTO pg_users VALUES ($1, $2, $3)', [4, 'Diana', 'diana@test.com']);

    const res = await client.query('SELECT name FROM pg_users WHERE id = $1', [4]);
    assert.strictEqual(res.rows.length, 1);
    assert.strictEqual(res.rows[0].name, 'Diana');

    await client.end();
  });

  it('multiple parameterized queries on same connection', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    for (let i = 1; i <= 4; i++) {
      const res = await client.query('SELECT name FROM pg_users WHERE id = $1', [i]);
      assert.strictEqual(res.rows.length, 1);
    }

    await client.end();
  });

  it('aggregate queries via pg driver', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const res = await client.query('SELECT COUNT(*) AS total FROM pg_users');
    assert.strictEqual(res.rows.length, 1);
    assert.strictEqual(parseInt(res.rows[0].total), 4);

    await client.end();
  });

  it('transactions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('BEGIN');
    await client.query("INSERT INTO pg_users VALUES (5, 'Eve', 'eve@test.com')");
    await client.query('COMMIT');

    const res = await client.query('SELECT name FROM pg_users WHERE id = $1', [5]);
    assert.strictEqual(res.rows.length, 1);
    assert.strictEqual(res.rows[0].name, 'Eve');

    await client.end();
  });

  it('JOIN query via pg driver', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE pg_orders (id INTEGER, user_id INTEGER, amount INTEGER)');
    await client.query('INSERT INTO pg_orders VALUES (1, 1, 100)');
    await client.query('INSERT INTO pg_orders VALUES (2, 1, 200)');
    await client.query('INSERT INTO pg_orders VALUES (3, 2, 150)');

    const res = await client.query(`
      SELECT u.name, SUM(o.amount) AS total
      FROM pg_users u
      JOIN pg_orders o ON u.id = o.user_id
      GROUP BY u.name
      ORDER BY u.name
    `);
    assert.ok(res.rows.length >= 2);
    // Find Alice — should have total 300
    const alice = res.rows.find(r => r.name === 'Alice');
    assert.ok(alice, 'Expected Alice in results');
    assert.strictEqual(parseInt(alice.total), 300);

    await client.end();
  });

  it('error handling — query error recovers', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    try {
      await client.query('SELECT * FROM fake_table_pg');
      assert.fail('Should have thrown');
    } catch (e) {
      assert.ok(e.message.includes('fake_table_pg') || e.message.includes('not found'));
    }

    // Should still be able to query
    const res = await client.query('SELECT 1 AS alive');
    assert.strictEqual(res.rows[0].alive, 1);

    await client.end();
  });

  it('connection pool', async () => {
    const pool = new Pool({
      host: '127.0.0.1',
      port: PORT,
      user: 'test',
      database: 'test',
      max: 3,
    });

    // Run 5 queries through the pool
    const results = await Promise.all([
      pool.query('SELECT COUNT(*) AS c FROM pg_users'),
      pool.query('SELECT name FROM pg_users WHERE id = $1', [1]),
      pool.query('SELECT name FROM pg_users WHERE id = $1', [2]),
      pool.query('SELECT name FROM pg_users WHERE id = $1', [3]),
      pool.query('SELECT COUNT(*) AS c FROM pg_orders'),
    ]);

    assert.strictEqual(results.length, 5);
    assert.strictEqual(parseInt(results[0].rows[0].c), 5);
    assert.strictEqual(results[1].rows[0].name, 'Alice');
    assert.strictEqual(results[2].rows[0].name, 'Bob');

    await pool.end();
  });
});
