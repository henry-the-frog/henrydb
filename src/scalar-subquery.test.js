// scalar-subquery.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Scalar Subqueries in SELECT', () => {
  it('returns single value from subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');

    const r = db.execute('SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t');
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows.every(r => r.max_val === 30));
  });

  it('correlated scalar subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'Eng', 100)");
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'Eng', 120)");
    db.execute("INSERT INTO emp VALUES (3, 'Carol', 'Sales', 90)");

    const r = db.execute(`
      SELECT name, salary,
        (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = e.dept) AS dept_avg
      FROM emp e
    `);
    assert.equal(r.rows.length, 3);
    // Eng avg = (100+120)/2 = 110, Sales avg = 90
    const alice = r.rows.find(r => r.name === 'Alice');
    assert.equal(alice.dept_avg, 110);
  });

  it('scalar subquery returns NULL for empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');

    const r = db.execute('SELECT id, (SELECT val FROM t WHERE id = 999) AS missing FROM t');
    assert.equal(r.rows[0].missing, null);
  });

  it('scalar subquery without FROM', () => {
    const db = new Database();
    const r = db.execute('SELECT (SELECT 42) AS answer');
    assert.equal(r.rows[0].answer, 42);
  });
});
