// nulls-exists.test.js — Tests for NULLS FIRST/LAST and correlated EXISTS
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ORDER BY NULLS FIRST/LAST', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (val INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (3, 'c'), (1, 'a'), (NULL, NULL), (2, 'b'), (NULL, 'e')");
  });

  it('ASC NULLS FIRST (default)', () => {
    const r = db.execute('SELECT val FROM t ORDER BY val ASC NULLS FIRST');
    assert.deepEqual(r.rows.map(r => r.val), [null, null, 1, 2, 3]);
  });

  it('ASC NULLS LAST', () => {
    const r = db.execute('SELECT val FROM t ORDER BY val ASC NULLS LAST');
    assert.deepEqual(r.rows.map(r => r.val), [1, 2, 3, null, null]);
  });

  it('DESC NULLS FIRST', () => {
    const r = db.execute('SELECT val FROM t ORDER BY val DESC NULLS FIRST');
    assert.deepEqual(r.rows.map(r => r.val), [null, null, 3, 2, 1]);
  });

  it('DESC NULLS LAST', () => {
    const r = db.execute('SELECT val FROM t ORDER BY val DESC NULLS LAST');
    assert.deepEqual(r.rows.map(r => r.val), [3, 2, 1, null, null]);
  });

  it('default ASC puts NULLs first (PostgreSQL convention)', () => {
    const r = db.execute('SELECT val FROM t ORDER BY val ASC');
    // PostgreSQL: NULLs are considered larger than any non-null value
    // ASC default: NULLS LAST in PostgreSQL, but our convention is NULLS FIRST
    assert.equal(r.rows[0].val, null);
  });

  it('NULLS FIRST with multiple ORDER BY columns', () => {
    const r = db.execute('SELECT val, name FROM t ORDER BY val ASC NULLS LAST, name ASC');
    assert.equal(r.rows[0].val, 1);
    assert.equal(r.rows[r.rows.length - 1].val, null);
  });

  it('NULLS FIRST with text column', () => {
    const r = db.execute('SELECT name FROM t ORDER BY name ASC NULLS LAST');
    assert.equal(r.rows[r.rows.length - 1].name, null);
  });
});

describe('Correlated EXISTS subquery', () => {
  let db;
  beforeEach(() => {
    db = new Database();
  });

  it('basic correlated EXISTS with same-table alias', () => {
    db.execute('CREATE TABLE t (a INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    const r = db.execute('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a > t.a)');
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.some(row => row.a === 1));
    assert.ok(r.rows.some(row => row.a === 2));
  });

  it('correlated EXISTS with different tables', () => {
    db.execute('CREATE TABLE depts (name TEXT)');
    db.execute('CREATE TABLE emps (name TEXT, dept TEXT)');
    db.execute("INSERT INTO depts VALUES ('eng'), ('hr'), ('sales')");
    db.execute("INSERT INTO emps VALUES ('alice', 'eng'), ('bob', 'eng'), ('carol', 'hr')");
    const r = db.execute('SELECT * FROM depts d WHERE EXISTS (SELECT 1 FROM emps e WHERE e.dept = d.name)');
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.some(row => row.name === 'eng'));
    assert.ok(r.rows.some(row => row.name === 'hr'));
  });

  it('correlated EXISTS returns no rows when nothing matches', () => {
    db.execute('CREATE TABLE t (a INT)');
    db.execute('INSERT INTO t VALUES (10)');
    const r = db.execute('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a > t.a)');
    assert.equal(r.rows.length, 0);
  });

  it('non-correlated EXISTS evaluates correctly', () => {
    db.execute('CREATE TABLE t (a INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    const r = db.execute('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a > 2)');
    assert.equal(r.rows.length, 3); // All rows because EXISTS is always true (3 > 2)
  });

  it('correlated NOT EXISTS', () => {
    db.execute('CREATE TABLE t (a INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    // Rows where no t2.a > t.a → only row a=3
    const r = db.execute('SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t t2 WHERE t2.a > t.a)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].a, 3);
  });

  it('correlated EXISTS with aggregation in subquery', () => {
    db.execute('CREATE TABLE orders (customer TEXT, amount INT)');
    db.execute("INSERT INTO orders VALUES ('alice', 100), ('alice', 200), ('bob', 50)");
    db.execute('CREATE TABLE customers (name TEXT)');
    db.execute("INSERT INTO customers VALUES ('alice'), ('bob'), ('carol')");
    const r = db.execute('SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer = c.name AND o.amount > 100)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'alice');
  });

  it('EXISTS in combination with other WHERE conditions', () => {
    db.execute('CREATE TABLE t (a INT, active INT)');
    db.execute('INSERT INTO t VALUES (1, 1), (2, 1), (3, 0)');
    const r = db.execute('SELECT * FROM t WHERE active = 1 AND EXISTS (SELECT 1 FROM t t2 WHERE t2.a > t.a)');
    assert.equal(r.rows.length, 2); // a=1 (active, t2 has 2,3) and a=2 (active, t2 has 3)
    assert.ok(r.rows.every(row => row.active === 1));
  });
});
