// server-knex.test.js — Test HenryDB with Knex ORM
// This is the ultimate integration test: a real ORM talking to HenryDB.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import knex from 'knex';
import { HenryDBServer } from './server.js';

const PORT = 15470;

describe('HenryDB with Knex ORM', () => {
  let server;
  let db;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    db = knex({
      client: 'pg',
      connection: {
        host: '127.0.0.1',
        port: PORT,
        user: 'test',
        database: 'test',
      },
      pool: { min: 1, max: 3 },
    });
  });

  after(async () => {
    await db.destroy();
    await server.stop();
  });

  it('raw SQL query', async () => {
    const result = await db.raw('SELECT 1 AS num');
    assert.strictEqual(result.rows[0].num, 1);
  });

  it('schema: create table', async () => {
    // Knex schema builder generates DDL
    await db.raw(`
      CREATE TABLE knex_users (
        id INTEGER,
        name TEXT,
        email TEXT,
        age INTEGER
      )
    `);

    // Verify table exists by querying
    const result = await db.raw('SELECT COUNT(*) AS cnt FROM knex_users');
    assert.strictEqual(parseInt(result.rows[0].cnt), 0);
  });

  it('insert rows', async () => {
    await db.raw("INSERT INTO knex_users VALUES (1, 'Alice', 'alice@example.com', 30)");
    await db.raw("INSERT INTO knex_users VALUES (2, 'Bob', 'bob@example.com', 25)");
    await db.raw("INSERT INTO knex_users VALUES (3, 'Charlie', 'charlie@example.com', 35)");
    await db.raw("INSERT INTO knex_users VALUES (4, 'Diana', 'diana@example.com', 28)");
    await db.raw("INSERT INTO knex_users VALUES (5, 'Eve', 'eve@example.com', 32)");

    const result = await db.raw('SELECT COUNT(*) AS cnt FROM knex_users');
    assert.strictEqual(parseInt(result.rows[0].cnt), 5);
  });

  it('select with where clause', async () => {
    const result = await db.raw('SELECT name, age FROM knex_users WHERE age > ? ORDER BY name', [29]);
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[1].name, 'Charlie');
    assert.strictEqual(result.rows[2].name, 'Eve');
  });

  it('parameterized query with Knex', async () => {
    const result = await db.raw('SELECT * FROM knex_users WHERE id = ?', [3]);
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'Charlie');
  });

  it('update rows', async () => {
    await db.raw('UPDATE knex_users SET age = ? WHERE id = ?', [31, 1]);
    const result = await db.raw('SELECT age FROM knex_users WHERE id = ?', [1]);
    assert.strictEqual(parseInt(result.rows[0].age), 31);
  });

  it('delete rows', async () => {
    await db.raw('DELETE FROM knex_users WHERE id = ?', [5]);
    const result = await db.raw('SELECT COUNT(*) AS cnt FROM knex_users');
    assert.strictEqual(parseInt(result.rows[0].cnt), 4);
  });

  it('aggregate queries', async () => {
    const result = await db.raw(`
      SELECT COUNT(*) AS cnt, MIN(age) AS youngest, MAX(age) AS oldest, AVG(age) AS avg_age
      FROM knex_users
    `);
    assert.strictEqual(parseInt(result.rows[0].cnt), 4);
    assert.strictEqual(parseInt(result.rows[0].youngest), 25);
    assert.strictEqual(parseInt(result.rows[0].oldest), 35);
  });

  it('JOIN query via Knex', async () => {
    // Create orders table
    await db.raw('CREATE TABLE knex_orders (id INTEGER, user_id INTEGER, product TEXT, amount INTEGER)');
    await db.raw("INSERT INTO knex_orders VALUES (1, 1, 'Widget', 50)");
    await db.raw("INSERT INTO knex_orders VALUES (2, 1, 'Gadget', 100)");
    await db.raw("INSERT INTO knex_orders VALUES (3, 2, 'Widget', 75)");
    await db.raw("INSERT INTO knex_orders VALUES (4, 3, 'Doohickey', 200)");

    const result = await db.raw(`
      SELECT u.name, COUNT(*) AS order_count, SUM(o.amount) AS total_spent
      FROM knex_users u
      JOIN knex_orders o ON u.id = o.user_id
      GROUP BY u.name
      ORDER BY u.name
    `);
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(parseInt(result.rows[0].total_spent), 150);
  });

  it('subquery via Knex', async () => {
    const result = await db.raw(`
      SELECT name FROM knex_users 
      WHERE id IN (SELECT user_id FROM knex_orders WHERE amount > ?)
      ORDER BY name
    `, [60]);
    assert.ok(!result.error);
    assert.ok(result.rows.length > 0);
  });

  it('transactions via Knex', async () => {
    await db.raw('BEGIN');
    await db.raw("INSERT INTO knex_users VALUES (6, 'Frank', 'frank@example.com', 40)");
    await db.raw('COMMIT');

    const result = await db.raw('SELECT name FROM knex_users WHERE id = ?', [6]);
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'Frank');
  });

  it('multiple concurrent Knex queries', async () => {
    const results = await Promise.all([
      db.raw('SELECT COUNT(*) AS c FROM knex_users'),
      db.raw('SELECT name FROM knex_users WHERE id = ?', [1]),
      db.raw('SELECT COUNT(*) AS c FROM knex_orders'),
      db.raw('SELECT name FROM knex_users WHERE id = ?', [3]),
    ]);

    assert.strictEqual(parseInt(results[0].rows[0].c), 5);
    assert.strictEqual(results[1].rows[0].name, 'Alice');
    assert.strictEqual(parseInt(results[2].rows[0].c), 4);
    assert.strictEqual(results[3].rows[0].name, 'Charlie');
  });

  it('error handling via Knex', async () => {
    try {
      await db.raw('SELECT * FROM nonexistent_knex_table');
      assert.fail('Should have thrown');
    } catch (e) {
      assert.ok(e.message.includes('nonexistent') || e.message.includes('not found') || e.message.includes('error'));
    }

    // Should recover
    const result = await db.raw('SELECT 1 AS alive');
    assert.strictEqual(result.rows[0].alive, 1);
  });
});
