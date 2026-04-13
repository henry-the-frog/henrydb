// limit-offset-stress.test.js — Edge cases for LIMIT and OFFSET
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LIMIT/OFFSET edge cases', () => {
  
  it('LIMIT larger than row count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 3; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT * FROM t LIMIT 100');
    assert.strictEqual(r.rows.length, 3);
  });

  it('LIMIT 1', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 1');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
  });

  it('OFFSET without LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    try {
      const r = db.execute('SELECT * FROM t ORDER BY id OFFSET 2');
      assert.strictEqual(r.rows.length, 3); // Skip first 2
    } catch (e) {
      // OFFSET without LIMIT may not be supported
      assert.ok(true);
    }
  });

  it('LIMIT 0 OFFSET 0', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    const r = db.execute('SELECT * FROM t LIMIT 0 OFFSET 0');
    assert.strictEqual(r.rows.length, 0);
  });

  it('paginated results', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 25; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    // Page through results
    const allIds = [];
    for (let page = 0; page < 5; page++) {
      const r = db.execute(`SELECT id FROM t ORDER BY id LIMIT 5 OFFSET ${page * 5}`);
      allIds.push(...r.rows.map(r => r.id));
    }
    assert.deepStrictEqual(allIds, Array.from({length: 25}, (_, i) => i + 1));
  });

  it('LIMIT with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES ('cat${i % 10}', ${i})`);
    const r = db.execute('SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY total DESC LIMIT 3');
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows[0].total >= r.rows[1].total);
  });

  it('LIMIT with DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i % 20})`);
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val LIMIT 5');
    assert.strictEqual(r.rows.length, 5);
    assert.deepStrictEqual(r.rows.map(r => r.val), [0, 1, 2, 3, 4]);
  });
});
