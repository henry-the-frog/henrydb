// more-functions.test.js — IFNULL, IIF, TYPEOF + EXPLAIN comprehensive
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('IFNULL', () => {
  it('returns value when not null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT IFNULL(val, 0) AS result FROM t');
    assert.equal(r.rows[0].result, 42);
  });

  it('returns default when null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT IFNULL(val, 99) AS result FROM t');
    assert.equal(r.rows[0].result, 99);
  });

  it('IFNULL with string', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute("SELECT IFNULL(val, 'default') AS result FROM t");
    assert.equal(r.rows[0].result, 'default');
  });
});

describe('TYPEOF', () => {
  it('integer type', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT TYPEOF(val) AS t FROM t');
    assert.equal(r.rows[0].t, 'integer');
  });

  it('text type', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    const r = db.execute('SELECT TYPEOF(val) AS t FROM t');
    assert.equal(r.rows[0].t, 'text');
  });

  it('null type', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT TYPEOF(val) AS t FROM t');
    assert.equal(r.rows[0].t, 'null');
  });
});

describe('EXPLAIN comprehensive', () => {
  it('EXPLAIN shows full pipeline', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute('CREATE INDEX idx_cat ON data (cat)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO data VALUES (${i}, '${i % 3}', ${i})`);

    const plan = db.execute("EXPLAIN SELECT DISTINCT cat, COUNT(*) FROM data WHERE cat = '1' GROUP BY cat ORDER BY cat LIMIT 5");
    const ops = plan.plan.map(p => p.operation);
    assert.ok(ops.includes('INDEX_SCAN') || ops.includes('TABLE_SCAN'));
    assert.ok(ops.includes('HASH_GROUP_BY'));
    assert.ok(ops.includes('SORT'));
    assert.ok(ops.includes('DISTINCT'));
    assert.ok(ops.includes('LIMIT'));
  });

  it('EXPLAIN shows CTE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    const plan = db.execute('EXPLAIN WITH cte AS (SELECT * FROM t) SELECT * FROM cte');
    assert.ok(plan.plan.some(p => p.operation === 'CTE'));
  });

  it('EXPLAIN shows window function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const plan = db.execute('EXPLAIN SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t');
    assert.ok(plan.plan.some(p => p.operation === 'WINDOW_FUNCTION'));
  });

  it('EXPLAIN shows aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    const plan = db.execute('EXPLAIN SELECT COUNT(*) FROM t');
    assert.ok(plan.plan.some(p => p.operation === 'AGGREGATE'));
  });

  it('EXPLAIN shows JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    const plan = db.execute('EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id');
    assert.ok(plan.plan.some(p => p.operation === 'NESTED_LOOP_JOIN' || p.operation === 'HASH_JOIN'));
  });
});

describe('Combined function tests', () => {
  it('IFNULL + arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, bonus INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 50)');
    const r = db.execute('SELECT id, IFNULL(bonus, 0) + 100 AS total FROM t ORDER BY id');
    assert.equal(r.rows[0].total, 100);
    assert.equal(r.rows[1].total, 150);
  });

  it('TRIM + UPPER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '  hello  ')");
    const r = db.execute('SELECT UPPER(TRIM(val)) AS result FROM t');
    assert.equal(r.rows[0].result, 'HELLO');
  });

  it('ABS in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 5)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    // ABS(val - 12) <= 3 → val between 9 and 15
    const r = db.execute('SELECT * FROM t WHERE ABS(val - 12) <= 3');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 10);
  });

  it('REPLACE + LENGTH', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello world')");
    const r = db.execute("SELECT LENGTH(REPLACE(val, ' ', '')) AS no_spaces FROM t");
    assert.equal(r.rows[0].no_spaces, 10); // 'helloworld' = 10 chars
  });
});
