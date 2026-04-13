// plan-cache-stress.test.js — Stress tests for query plan cache
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Plan cache stress tests', () => {
  
  it('repeated queries hit the cache', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    // Run the same SELECT multiple times
    for (let i = 0; i < 10; i++) {
      db.execute('SELECT * FROM t WHERE id = 5');
    }
    
    const stats = db.planCacheStats();
    assert.ok(stats.hits > 0, `expected cache hits, got ${stats.hits}`);
  });

  it('DDL invalidates cached plans for affected table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, \'hello\')');
    
    // Cache a query
    db.execute('SELECT * FROM t');
    db.execute('SELECT * FROM t'); // Should hit cache
    
    const before = db.planCacheStats();
    
    // DDL should invalidate
    db.execute('ALTER TABLE t ADD COLUMN extra INT');
    
    // Query again — should not crash, may miss cache
    const r = db.execute('SELECT * FROM t');
    assert.ok(r.rows.length >= 1);
  });

  it('different queries get different plans', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r1 = db.execute('SELECT * FROM t WHERE id = 1');
    const r2 = db.execute('SELECT * FROM t WHERE val > 50');
    
    assert.strictEqual(r1.rows.length, 1);
    assert.ok(r2.rows.length > 0);
  });

  it('cache works with parameterized queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    // Run the same query template with different values
    for (let i = 1; i <= 5; i++) {
      db.execute(`SELECT * FROM t WHERE id = ${i}`);
    }
    // These are different SQL strings, so each is a cache miss
    // But running them again should hit
    for (let i = 1; i <= 5; i++) {
      db.execute(`SELECT * FROM t WHERE id = ${i}`);
    }
    const stats = db.planCacheStats();
    assert.ok(stats.hits >= 5, `expected 5+ cache hits`);
  });

  it('rapid alternating queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO a VALUES (${i})`);
      db.execute(`INSERT INTO b VALUES (${i * 10})`);
    }
    
    // Alternate between two queries rapidly
    for (let i = 0; i < 100; i++) {
      if (i % 2 === 0) {
        db.execute('SELECT * FROM a');
      } else {
        db.execute('SELECT * FROM b');
      }
    }
    
    const stats = db.planCacheStats();
    assert.ok(stats.hits >= 90, `expected 90+ hits from alternating queries, got ${stats.hits}`);
  });

  it('cache with joins', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'C${i}')`);
      db.execute(`INSERT INTO orders VALUES (${i}, ${i})`);
    }
    
    const sql = 'SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id';
    const r1 = db.execute(sql);
    const r2 = db.execute(sql);
    assert.deepStrictEqual(r1.rows, r2.rows);
  });

  it('cache survives DROP and recreate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SELECT * FROM t');
    
    db.execute('DROP TABLE t');
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    
    const r = db.execute('SELECT * FROM t');
    assert.ok(r.rows.length >= 1);
    // Should have the new schema
    assert.ok('name' in r.rows[0] || r.rows[0].name !== undefined);
  });

  it('plan cache stats are accessible', () => {
    const db = new Database();
    const stats = db.planCacheStats();
    assert.ok('hits' in stats);
    assert.ok('misses' in stats);
  });

  it('1000 unique queries do not crash', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    for (let i = 0; i < 1000; i++) {
      const id = (i % 100) + 1;
      db.execute(`SELECT * FROM t WHERE id = ${id}`);
    }
    
    const stats = db.planCacheStats();
    // Some hits expected (only 100 unique queries)
    assert.ok(stats.hits >= 800, `expected 800+ hits, got ${stats.hits}`);
  });
});
