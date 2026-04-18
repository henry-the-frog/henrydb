// table-func.test.js — Table-returning function tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function q(db, sql) {
  const r = db.execute(sql);
  return r.rows || r || [];
}

function setupDB() {
  const db = new Database();
  db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
  db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Eng', 80000)");
  db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Eng', 90000)");
  db.execute("INSERT INTO employees VALUES (3, 'Carol', 'Sales', 70000)");
  db.execute("INSERT INTO employees VALUES (4, 'Dave', 'Sales', 75000)");
  db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Eng', 85000)");
  return db;
}

describe('Table-Returning Functions', () => {
  describe('basic usage', () => {
    it('returns rows from a parameterized query', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT * FROM get_dept('Eng')");
      assert.equal(r.length, 3);
    });

    it('returns empty result set when no matches', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT * FROM get_dept('HR')");
      assert.equal(r.length, 0);
    });

    it('supports WHERE on function result', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT name FROM get_dept('Eng') WHERE salary > 85000");
      assert.equal(r.length, 1);
      assert.equal(r[0].name, 'Bob');
    });

    it('supports ORDER BY on function result', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT * FROM get_dept('Eng') ORDER BY salary DESC");
      assert.equal(r[0].name, 'Bob');
      assert.equal(r[0].salary, 90000);
    });

    it('supports LIMIT on function result', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT * FROM get_dept('Eng') LIMIT 2");
      assert.equal(r.length, 2);
    });
  });

  describe('with aliases', () => {
    it('function result with AS alias', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT e.name, e.salary FROM get_dept('Sales') AS e ORDER BY e.name");
      assert.equal(r.length, 2);
      assert.equal(r[0].name, 'Carol');
    });
  });

  describe('multiple parameters', () => {
    it('function with salary range parameters', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION salary_range(min_sal INT, max_sal INT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE salary >= min_sal AND salary <= max_sal $$");
      
      const r = q(db, 'SELECT * FROM salary_range(75000, 85000) ORDER BY name');
      assert.equal(r.length, 3); // Dave(75k), Alice(80k), Eve(85k)
      assert.equal(r[0].name, 'Alice');
    });
  });

  describe('aggregation on function results', () => {
    it('COUNT on function result', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT COUNT(*) as cnt FROM get_dept('Eng')");
      assert.equal(r[0].cnt, 3);
    });

    it('SUM/AVG on function result', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      
      const r = q(db, "SELECT SUM(salary) as total, AVG(salary) as avg_sal FROM get_dept('Eng')");
      assert.equal(r[0].total, 255000); // 80k + 90k + 85k
    });
  });

  describe('error handling', () => {
    it('calling non-existent function throws', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      assert.throws(() => db.execute("SELECT * FROM nonexistent('x')"), /does not exist/);
    });

    it('calling scalar function in FROM throws', () => {
      const db = setupDB();
      db.execute('CREATE FUNCTION double_it(x INT) RETURNS INT AS $$ SELECT x * 2 $$');
      assert.throws(() => db.execute('SELECT * FROM double_it(5)'), /does not return TABLE/);
    });
  });

  describe('combining scalar and table functions', () => {
    it('scalar UDF on table function results', () => {
      const db = setupDB();
      db.execute("CREATE FUNCTION get_dept(d TEXT) RETURNS TABLE(name TEXT, salary INT) AS $$ SELECT name, salary FROM employees WHERE dept = d $$");
      db.execute("CREATE FUNCTION bonus(salary INT) RETURNS INT AS $$ SELECT salary / 10 $$");
      
      const r = q(db, "SELECT name, bonus(salary) as bonus FROM get_dept('Eng') ORDER BY name");
      assert.equal(r.length, 3);
      assert.equal(r[0].name, 'Alice');
      assert.equal(r[0].bonus, 8000);
    });
  });
});
