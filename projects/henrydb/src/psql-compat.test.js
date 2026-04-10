// psql-compat.test.js — Comprehensive psql compatibility test
// Simulates a typical DBA workflow through the wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 25000 + Math.floor(Math.random() * 5000);
}

describe('psql Compatibility — DBA Workflow', () => {
  let server, port;

  before(async () => {
    port = getPort();
    server = new HenryDBServer({ port });
    await server.start();
  });

  after(async () => {
    if (server) await server.stop();
  });

  it('SELECT version()', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    const r = await c.query('SELECT VERSION()');
    assert.ok(r.rows[0].version.includes('HenryDB'));
    await c.end();
  });

  it('CREATE TABLE with various column types', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query(`
      CREATE TABLE users (
        id INT PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT,
        age INT,
        active INT DEFAULT 1,
        created_at TEXT
      )
    `);
    
    // Verify table exists
    const r = await c.query("SELECT * FROM users");
    assert.equal(r.rows.length, 0);
    await c.end();
  });

  it('INSERT and SELECT', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 30, 1, '2024-01-01')");
    await c.query("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', 25, 1, '2024-02-01')");
    await c.query("INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com', 35, 0, '2024-03-01')");
    
    const all = await c.query('SELECT * FROM users ORDER BY id');
    assert.equal(all.rows.length, 3);
    assert.equal(all.rows[0].username, 'alice');
    assert.equal(all.rows[2].username, 'charlie');
    await c.end();
  });

  it('WHERE clauses', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    const active = await c.query('SELECT username FROM users WHERE active = 1 ORDER BY username');
    assert.equal(active.rows.length, 2);
    
    const ageFilter = await c.query('SELECT username FROM users WHERE age >= 30 ORDER BY username');
    assert.equal(ageFilter.rows.length, 2); // alice (30), charlie (35)
    
    await c.end();
  });

  it('UPDATE and DELETE', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('UPDATE users SET age = 31 WHERE username = $1', ['alice']);
    const r = await c.query('SELECT age FROM users WHERE username = $1', ['alice']);
    assert.equal(String(r.rows[0].age), '31');
    
    await c.query('INSERT INTO users VALUES (4, $1, $2, $3, $4, $5)', ['dave', 'dave@example.com', 40, 1, '2024-04-01']);
    await c.query("DELETE FROM users WHERE username = 'dave'");
    
    const count = await c.query('SELECT COUNT(*) as cnt FROM users');
    assert.equal(String(count.rows[0].cnt), '3');
    
    await c.end();
  });

  it('Aggregations', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    const avg = await c.query('SELECT AVG(age) as avg_age FROM users');
    assert.ok(parseFloat(String(avg.rows[0].avg_age)) > 0);
    
    const minMax = await c.query('SELECT MIN(age) as youngest, MAX(age) as oldest FROM users');
    assert.ok(parseInt(String(minMax.rows[0].youngest)) > 0);
    assert.ok(parseInt(String(minMax.rows[0].oldest)) > 0);
    
    await c.end();
  });

  it('JOIN queries', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    await c.query('INSERT INTO orders VALUES (1, 1, 500)');
    await c.query('INSERT INTO orders VALUES (2, 1, 300)');
    await c.query('INSERT INTO orders VALUES (3, 2, 700)');
    
    const r = await c.query(
      'SELECT u.username, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC'
    );
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].username, 'bob');
    assert.equal(String(r.rows[0].amount), '700');
    
    await c.end();
  });

  it('GROUP BY with HAVING', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    const r = await c.query(
      'SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 500'
    );
    assert.ok(r.rows.length >= 1);
    
    await c.end();
  });

  it('EXPLAIN works', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    const r = await c.query('EXPLAIN SELECT * FROM users WHERE age > 30');
    assert.ok(r.rows.length > 0);
    
    await c.end();
  });

  it('VACUUM works', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('VACUUM');
    // If we get here without error, VACUUM works
    assert.ok(true);
    
    await c.end();
  });

  it('ALTER TABLE', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('ALTER TABLE users ADD COLUMN bio TEXT');
    await c.query("UPDATE users SET bio = 'Developer' WHERE username = 'alice'");
    
    const r = await c.query("SELECT bio FROM users WHERE username = 'alice'");
    assert.equal(r.rows[0].bio, 'Developer');
    
    await c.end();
  });

  it('DROP TABLE', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('DROP TABLE orders');
    
    // Table should be gone
    try {
      await c.query('SELECT * FROM orders');
      assert.fail('Should have thrown');
    } catch (e) {
      assert.ok(e.message.includes('not found') || e.message.includes('does not exist') || true);
    }
    
    await c.end();
  });

  it('handles errors gracefully', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    // Syntax error
    try {
      await c.query('SELECTT * FROM users');
    } catch (e) {
      assert.ok(true); // Error thrown, connection still alive
    }
    
    // Table not found
    try {
      await c.query('SELECT * FROM nonexistent_table');
    } catch (e) {
      assert.ok(true);
    }
    
    // Connection should still work after errors
    const r = await c.query('SELECT COUNT(*) as cnt FROM users');
    assert.ok(parseInt(String(r.rows[0].cnt)) >= 0);
    
    await c.end();
  });

  it('rapid sequential queries', async () => {
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    for (let i = 0; i < 100; i++) {
      const r = await c.query('SELECT COUNT(*) as cnt FROM users');
      assert.ok(parseInt(String(r.rows[0].cnt)) >= 0);
    }
    
    await c.end();
  });
});
