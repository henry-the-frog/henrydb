import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL: CTE (Common Table Expressions)', () => {
  it('simple CTE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('WITH cte AS (SELECT * FROM t WHERE val > 15) SELECT * FROM cte');
    assert.equal(r.rows.length, 2);
  });

  it('recursive CTE: generate series', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 5
      )
      SELECT * FROM nums
    `);
    assert.equal(r.rows.length, 5);
    assert.deepEqual(r.rows.map(r => r.n), [1, 2, 3, 4, 5]);
  });

  it('recursive CTE: factorial', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fact(n, f) AS (
        SELECT 1, 1
        UNION ALL
        SELECT n + 1, f * (n + 1) FROM fact WHERE n < 5
      )
      SELECT * FROM fact
    `);
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[4].f, 120);
  });

  it('recursive CTE: fibonacci', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fib(n, a, b) AS (
        SELECT 0, 0, 1
        UNION ALL
        SELECT n + 1, b, a + b FROM fib WHERE n < 10
      )
      SELECT n, a as fib FROM fib
    `);
    assert.equal(r.rows.length, 11);
    assert.equal(r.rows[10].fib, 55);
  });
});

describe('SQL: Subqueries', () => {
  it('scalar subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT id, val, (SELECT MAX(val) FROM t) as max_val FROM t');
    assert(r.rows.every(row => row.max_val === 30));
  });

  it('IN subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, amount INT)');
    db.execute('CREATE TABLE big_orders (order_id INT)');
    db.execute('INSERT INTO orders VALUES (1,10),(2,50),(3,100),(4,5)');
    db.execute('INSERT INTO big_orders VALUES (2),(3)');
    const r = db.execute('SELECT * FROM orders WHERE id IN (SELECT order_id FROM big_orders)');
    assert.equal(r.rows.length, 2);
  });

  it('EXISTS subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (ref INT)');
    db.execute('INSERT INTO a VALUES (1),(2),(3)');
    db.execute('INSERT INTO b VALUES (1),(3)');
    const r = db.execute('SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.ref = a.id)');
    assert.equal(r.rows.length, 2);
  });

  it('correlated subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, dept TEXT, salary INT)');
    db.execute(`INSERT INTO emp VALUES (1,'A',100),(2,'A',120),(3,'B',90),(4,'B',110)`);
    const r = db.execute(`
      SELECT * FROM emp e WHERE salary > (
        SELECT AVG(salary) FROM emp WHERE dept = e.dept
      )
    `);
    assert.equal(r.rows.length, 2);
  });
});

describe('SQL: Aggregate Functions', () => {
  it('GROUP BY with HAVING', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (product TEXT, amount INT)');
    db.execute(`INSERT INTO sales VALUES ('A',10),('A',20),('B',5),('B',15),('C',100)`);
    const r = db.execute('SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING SUM(amount) > 20');
    assert.equal(r.rows.length, 2); // A=30, C=100
  });

  it('COUNT DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(1),(2),(2),(3)');
    const r = db.execute('SELECT COUNT(DISTINCT val) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 3);
  });
});

describe('SQL: Milestone Tests', () => {
  it('999th test: CASE expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    const r = db.execute("SELECT val, CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END as label FROM t");
    assert.equal(r.rows[0].label, 'one');
    assert.equal(r.rows[2].label, 'other');
  });

  it('1000th test: ORDER BY + LIMIT + OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (5),(3),(1),(4),(2)');
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1');
    assert.deepEqual(r.rows.map(r => r.id), [2, 3]);
  });
});
