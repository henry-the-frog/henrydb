// index-operations-stress.test.js — Stress tests for index correctness
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Index operations stress tests', () => {
  
  it('index point lookup after many inserts', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 7 % 997})`);
    
    // Point lookup should find the exact row
    const r = db.execute('SELECT id FROM t WHERE val = 500');
    assert.ok(r.rows.length >= 1);
    for (const row of r.rows) {
      const verify = db.execute(`SELECT val FROM t WHERE id = ${row.id}`);
      assert.strictEqual(verify.rows[0].val, 500);
    }
  });

  it('index range scan', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('SELECT id FROM t WHERE val BETWEEN 40 AND 60 ORDER BY id');
    assert.strictEqual(r.rows.length, 21); // 40, 41, ..., 60
  });

  it('index after DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    db.execute('DELETE FROM t WHERE val > 50');
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 50');
    assert.strictEqual(r.rows[0].cnt, 0);
    
    // Values <= 50 should still be findable
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = 25');
    assert.strictEqual(r2.rows[0].cnt, 1);
  });

  it('index after UPDATE on indexed column', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    db.execute('UPDATE t SET val = val + 1000 WHERE val > 25');
    
    // Old values should not be found
    const r1 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = 30');
    assert.strictEqual(r1.rows[0].cnt, 0);
    
    // New values should be found
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = 1030');
    assert.strictEqual(r2.rows[0].cnt, 1);
  });

  it('multiple indexes on same table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('CREATE INDEX idx_a ON t (a)');
    db.execute('CREATE INDEX idx_b ON t (b)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 10}, ${i % 5})`);
    
    const r1 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE a = 3');
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE b = 2');
    assert.strictEqual(r1.rows[0].cnt, 10);
    assert.strictEqual(r2.rows[0].cnt, 20);
  });

  it('EXPLAIN shows index usage', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE val = 50');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Index') || text.includes('idx'), `should show index usage: ${text}`);
  });

  it('index with duplicate values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, cat INT)');
    db.execute('CREATE INDEX idx ON t (cat)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 5})`);
    
    // Each category has 20 rows
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE cat = 3');
    assert.strictEqual(r.rows[0].cnt, 20);
  });

  it('index with NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    
    // IS NULL should work even with index
    const r = db.execute('SELECT id FROM t WHERE val IS NULL');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 2);
  });

  it('index built on existing data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    // Create index AFTER data exists
    db.execute('CREATE INDEX idx ON t (val)');
    
    const r = db.execute('SELECT id FROM t WHERE val = 75');
    assert.strictEqual(r.rows[0].id, 75);
  });

  it('INSERT then DELETE then INSERT cycle with index', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    
    // Insert, delete, re-insert
    for (let cycle = 0; cycle < 5; cycle++) {
      for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${cycle * 100 + i}, ${i})`);
      db.execute('DELETE FROM t WHERE val > 10');
    }
    
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.strictEqual(r.rows[0].cnt, 50); // 5 cycles × 10 remaining per cycle
  });

  it('large index (5000 rows)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 5000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 1000})`);
    
    const start = Date.now();
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = 500');
    const elapsed = Date.now() - start;
    
    assert.strictEqual(r.rows[0].cnt, 5);
    assert.ok(elapsed < 100, `index lookup on 5000 rows took ${elapsed}ms`);
  });
});
