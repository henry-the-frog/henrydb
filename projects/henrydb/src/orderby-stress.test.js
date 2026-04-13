// orderby-stress.test.js — Stress tests for ORDER BY, LIMIT, OFFSET
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ORDER BY stress tests', () => {
  
  it('ORDER BY single column ASC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    [5, 3, 1, 4, 2].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT id FROM t ORDER BY id ASC');
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3, 4, 5]);
  });

  it('ORDER BY single column DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    [5, 3, 1, 4, 2].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT id FROM t ORDER BY id DESC');
    assert.deepStrictEqual(r.rows.map(r => r.id), [5, 4, 3, 2, 1]);
  });

  it('ORDER BY multiple columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 3)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 2)');
    db.execute('INSERT INTO t VALUES (1, 2)');
    const r = db.execute('SELECT a, b FROM t ORDER BY a, b');
    assert.deepStrictEqual(r.rows.map(r => [r.a, r.b]), [[1,1],[1,2],[1,3],[2,2]]);
  });

  it('ORDER BY mixed ASC and DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 3)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 2)');
    db.execute('INSERT INTO t VALUES (1, 2)');
    const r = db.execute('SELECT a, b FROM t ORDER BY a ASC, b DESC');
    assert.deepStrictEqual(r.rows.map(r => [r.a, r.b]), [[1,3],[1,2],[1,1],[2,2]]);
  });

  it('ORDER BY with NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 5)');
    db.execute('INSERT INTO t VALUES (4, NULL)');
    const r = db.execute('SELECT id, val FROM t ORDER BY val');
    // NULLs typically sort first or last
    assert.strictEqual(r.rows.length, 4);
    // Non-null values should be in order
    const nonNull = r.rows.filter(r => r.val !== null);
    assert.strictEqual(nonNull[0].val, 5);
    assert.strictEqual(nonNull[1].val, 10);
  });

  it('ORDER BY string column', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    ['Charlie', 'Alice', 'Bob'].forEach(n => db.execute(`INSERT INTO t VALUES ('${n}')`));
    const r = db.execute('SELECT name FROM t ORDER BY name');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Bob', 'Charlie']);
  });

  it('LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT id FROM t ORDER BY id LIMIT 3');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('LIMIT with OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 5');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.id), [6, 7, 8]);
  });

  it('OFFSET beyond data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 100');
    assert.strictEqual(r.rows.length, 0);
  });

  it('LIMIT 0', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    const r = db.execute('SELECT id FROM t LIMIT 0');
    assert.strictEqual(r.rows.length, 0);
  });

  it('ORDER BY with duplicate values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [1, 3, 2, 3, 1, 2].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT val FROM t ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 1, 2, 2, 3, 3]);
  });

  it('ORDER BY alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT id, val as v FROM t ORDER BY v');
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3, 1]);
  });

  it('ORDER BY column not in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, hidden INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 3)');
    db.execute('INSERT INTO t VALUES (2, 20, 1)');
    db.execute('INSERT INTO t VALUES (3, 30, 2)');
    const r = db.execute('SELECT id, val FROM t ORDER BY hidden');
    // Should order by hidden: 1, 2, 3 → ids 2, 3, 1
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3, 1]);
  });

  it('ORDER BY aggregate in GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A', 10)");
    db.execute("INSERT INTO t VALUES ('B', 30)");
    db.execute("INSERT INTO t VALUES ('A', 20)");
    db.execute("INSERT INTO t VALUES ('B', 5)");
    const r = db.execute('SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY total');
    assert.strictEqual(r.rows[0].cat, 'A'); // A: 30
    assert.strictEqual(r.rows[1].cat, 'B'); // B: 35
  });

  it('large ORDER BY (10000 rows)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 10000; i >= 1; i--) db.execute(`INSERT INTO t VALUES (${i})`);
    const start = Date.now();
    const r = db.execute('SELECT id FROM t ORDER BY id LIMIT 5');
    const elapsed = Date.now() - start;
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3, 4, 5]);
    assert.ok(elapsed < 5000, `sorting 10K rows took ${elapsed}ms`);
  });

  it('ORDER BY with DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [3, 1, 2, 3, 1].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('ORDER BY with window function alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${6 - i})`);
    const r = db.execute('SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rn FROM t ORDER BY rn');
    assert.deepStrictEqual(r.rows.map(r => r.rn), [1, 2, 3, 4, 5]);
    assert.strictEqual(r.rows[0].val, 5); // Highest val first
  });
});
