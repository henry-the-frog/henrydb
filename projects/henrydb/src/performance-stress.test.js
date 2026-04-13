// performance-stress.test.js — Performance benchmark tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Performance stress tests', () => {
  
  it('10K row insertion benchmark', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, name TEXT)');
    const start = Date.now();
    for (let i = 1; i <= 10000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 7}, 'name${i}')`);
    const elapsed = Date.now() - start;
    console.log(`10K inserts: ${elapsed}ms`);
    assert.ok(elapsed < 30000, `too slow: ${elapsed}ms`);
  });

  it('full table scan on 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const start = Date.now();
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 5000');
    const elapsed = Date.now() - start;
    assert.strictEqual(r.rows[0].cnt, 5000);
    console.log(`Full scan 10K: ${elapsed}ms`);
  });

  it('indexed lookup on 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 10000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const start = Date.now();
    for (let i = 0; i < 100; i++) {
      db.execute(`SELECT * FROM t WHERE val = ${Math.floor(Math.random() * 10000) + 1}`);
    }
    const elapsed = Date.now() - start;
    console.log(`100 indexed lookups on 10K: ${elapsed}ms`);
    assert.ok(elapsed < 5000);
  });

  it('GROUP BY on 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 1; i <= 10000; i++) db.execute(`INSERT INTO t VALUES ('cat${i % 100}', ${i})`);
    const start = Date.now();
    const r = db.execute('SELECT cat, SUM(val) as total, COUNT(*) as cnt FROM t GROUP BY cat');
    const elapsed = Date.now() - start;
    assert.strictEqual(r.rows.length, 100);
    console.log(`GROUP BY 10K rows → 100 groups: ${elapsed}ms`);
  });

  it('JOIN on medium tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, val INT)');
    db.execute('CREATE TABLE b (a_id INT, data TEXT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO a VALUES (${i}, ${i})`);
    for (let i = 1; i <= 5000; i++) db.execute(`INSERT INTO b VALUES (${(i % 1000) + 1}, 'data${i}')`);
    const start = Date.now();
    const r = db.execute('SELECT COUNT(*) as cnt FROM a JOIN b ON a.id = b.a_id WHERE a.val > 500');
    const elapsed = Date.now() - start;
    assert.strictEqual(r.rows[0].cnt, 2500);
    console.log(`JOIN 1K × 5K: ${elapsed}ms`);
  });
});
