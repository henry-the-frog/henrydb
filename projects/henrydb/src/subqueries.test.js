// subqueries.test.js — Comprehensive subquery tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Subqueries', () => {
  it('scalar subquery in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT id, (SELECT MAX(val) FROM t) as max_val FROM t ORDER BY id');
    assert.equal(r.rows[0].max_val, 30);
    assert.equal(r.rows[2].max_val, 30);
  });

  it('correlated scalar subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, cust TEXT, amount INT)');
    db.execute("INSERT INTO orders VALUES (1,'alice',100),(2,'alice',200),(3,'bob',150)");
    const r = db.execute(`
      SELECT DISTINCT cust,
             (SELECT SUM(amount) FROM orders o2 WHERE o2.cust = o1.cust) as total
      FROM orders o1
      ORDER BY cust
    `);
    assert.equal(r.rows[0].total, 300); // alice: 100+200
    assert.equal(r.rows[1].total, 150); // bob: 150
  });

  it('IN subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')");
    db.execute('INSERT INTO t2 VALUES (1),(3)');
    const r = db.execute('SELECT val FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), ['a', 'c']);
  });

  it('NOT IN subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')");
    db.execute('INSERT INTO t2 VALUES (1),(3)');
    const r = db.execute('SELECT val FROM t1 WHERE id NOT IN (SELECT id FROM t2)');
    assert.deepEqual(r.rows.map(r => r.val), ['b']);
  });

  it('EXISTS subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (ref_id INT)');
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')");
    db.execute('INSERT INTO t2 VALUES (1),(1),(3)');
    const r = db.execute('SELECT val FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref_id = t1.id) ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), ['a', 'c']);
  });

  it('NOT EXISTS subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (ref_id INT)');
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')");
    db.execute('INSERT INTO t2 VALUES (1),(3)');
    const r = db.execute('SELECT val FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref_id = t1.id)');
    assert.deepEqual(r.rows.map(r => r.val), ['b']);
  });

  it('subquery in FROM (derived table)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT * FROM (SELECT id, val * 2 as doubled FROM t) sub WHERE doubled > 30 ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].doubled, 40);
  });

  it('subquery comparison operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 30);
  });
});
