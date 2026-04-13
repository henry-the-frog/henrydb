// multicolumn-stress.test.js — Tests for multi-column operations
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Multi-column operation stress tests', () => {
  
  it('composite key uniqueness', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 1, 'first')");
    db.execute("INSERT INTO t VALUES (1, 2, 'second')");
    db.execute("INSERT INTO t VALUES (2, 1, 'third')");
    const r = db.execute('SELECT * FROM t WHERE a = 1 AND b = 2');
    assert.strictEqual(r.rows[0].val, 'second');
  });

  it('multi-column ORDER BY with ties', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 2, 30)');
    db.execute('INSERT INTO t VALUES (1, 1, 20)');
    db.execute('INSERT INTO t VALUES (2, 1, 10)');
    db.execute('INSERT INTO t VALUES (1, 2, 10)');
    const r = db.execute('SELECT * FROM t ORDER BY a, b, c');
    assert.deepStrictEqual(r.rows.map(r => [r.a, r.b, r.c]), [[1,1,20],[1,2,10],[1,2,30],[2,1,10]]);
  });

  it('multi-column GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (region TEXT, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('East', 'A', 10)");
    db.execute("INSERT INTO t VALUES ('East', 'A', 20)");
    db.execute("INSERT INTO t VALUES ('East', 'B', 30)");
    db.execute("INSERT INTO t VALUES ('West', 'A', 40)");
    const r = db.execute('SELECT region, cat, SUM(val) as total FROM t GROUP BY region, cat ORDER BY region, cat');
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].total, 30); // East/A
  });

  it('multi-column IN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    for (let i = 1; i <= 5; i++) for (let j = 1; j <= 5; j++) db.execute(`INSERT INTO t VALUES (${i}, ${j})`);
    // Single-column IN as proxy
    const r = db.execute('SELECT a, b FROM t WHERE a IN (1, 3) AND b IN (2, 4) ORDER BY a, b');
    assert.strictEqual(r.rows.length, 4); // (1,2), (1,4), (3,2), (3,4)
  });

  it('multi-column DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x')");
    db.execute("INSERT INTO t VALUES (1, 'x')");
    db.execute("INSERT INTO t VALUES (1, 'y')");
    db.execute("INSERT INTO t VALUES (2, 'x')");
    const r = db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b');
    assert.strictEqual(r.rows.length, 3);
  });

  it('SELECT * with many columns', () => {
    const db = new Database();
    const cols = Array.from({length: 20}, (_, i) => `c${i} INT`).join(', ');
    db.execute(`CREATE TABLE wide (${cols})`);
    const vals = Array.from({length: 20}, (_, i) => i).join(', ');
    db.execute(`INSERT INTO wide VALUES (${vals})`);
    const r = db.execute('SELECT * FROM wide');
    assert.strictEqual(Object.keys(r.rows[0]).length, 20);
    assert.strictEqual(r.rows[0].c0, 0);
    assert.strictEqual(r.rows[0].c19, 19);
  });

  it('multi-column UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20, 30)');
    db.execute('UPDATE t SET a = 100, b = 200, c = 300 WHERE id = 1');
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.strictEqual(r.rows[0].a, 100);
    assert.strictEqual(r.rows[0].b, 200);
    assert.strictEqual(r.rows[0].c, 300);
  });
});
