import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CTE Comprehensive Tests (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, dept_id INT, salary INT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1,'Alice',1,100000,NULL)");
    db.execute("INSERT INTO employees VALUES (2,'Bob',1,120000,1)");
    db.execute("INSERT INTO employees VALUES (3,'Carol',2,90000,1)");
    db.execute("INSERT INTO employees VALUES (4,'Dave',2,110000,3)");
    db.execute("INSERT INTO employees VALUES (5,'Eve',1,130000,2)");
    db.execute('CREATE TABLE departments (id INT, name TEXT)');
    db.execute("INSERT INTO departments VALUES (1,'Engineering'),(2,'Sales')");
    return db;
  }

  describe('Basic CTEs', () => {
    it('simple CTE', () => {
      setup();
      const r = db.execute(`
        WITH high_earners AS (SELECT * FROM employees WHERE salary > 100000)
        SELECT name FROM high_earners ORDER BY name
      `);
      assert.equal(r.rows.length, 3);
    });

    it('CTE with aggregation', () => {
      setup();
      const r = db.execute(`
        WITH dept_totals AS (
          SELECT dept_id, SUM(salary) AS total
          FROM employees GROUP BY dept_id
        )
        SELECT * FROM dept_totals ORDER BY total DESC
      `);
      assert.equal(r.rows.length, 2);
    });

    it('multiple CTEs', () => {
      setup();
      const r = db.execute(`
        WITH eng AS (SELECT * FROM employees WHERE dept_id = 1),
             sales AS (SELECT * FROM employees WHERE dept_id = 2)
        SELECT (SELECT COUNT(*) FROM eng) AS eng_cnt,
               (SELECT COUNT(*) FROM sales) AS sales_cnt
      `);
      assert.equal(r.rows[0].eng_cnt, 3);
      assert.equal(r.rows[0].sales_cnt, 2);
    });

    it('CTE referencing another CTE', () => {
      setup();
      const r = db.execute(`
        WITH totals AS (
          SELECT dept_id, SUM(salary) AS total FROM employees GROUP BY dept_id
        ),
        ranked AS (
          SELECT dept_id, total, RANK() OVER (ORDER BY total DESC) AS rank FROM totals
        )
        SELECT * FROM ranked
      `);
      assert.ok(r.rows.length === 2);
    });
  });

  describe('Recursive CTEs', () => {
    it('recursive counter', () => {
      setup();
      const r = db.execute(`
        WITH RECURSIVE nums(n) AS (
          SELECT 1
          UNION ALL
          SELECT n + 1 FROM nums WHERE n < 5
        )
        SELECT * FROM nums
      `);
      assert.equal(r.rows.length, 5);
      assert.equal(r.rows[0].n, 1);
      assert.equal(r.rows[4].n, 5);
    });

    it('Fibonacci sequence', () => {
      setup();
      const r = db.execute(`
        WITH RECURSIVE fib(n, a, b) AS (
          SELECT 1, 0, 1
          UNION ALL
          SELECT n + 1, b, a + b FROM fib WHERE n < 8
        )
        SELECT n, a AS value FROM fib
      `);
      assert.equal(r.rows.length, 8);
      assert.equal(r.rows[7].value, 13);  // fib(8) = 13
    });

    it('recursive tree traversal', () => {
      setup();
      const r = db.execute(`
        WITH RECURSIVE subordinates(id, name, depth) AS (
          SELECT id, name, 0 FROM employees WHERE manager_id IS NULL
          UNION ALL
          SELECT e.id, e.name, s.depth + 1
          FROM employees e JOIN subordinates s ON e.manager_id = s.id
        )
        SELECT * FROM subordinates ORDER BY depth, name
      `);
      assert.equal(r.rows.length, 5);  // all employees
      assert.equal(r.rows[0].depth, 0);  // root
    });
  });

  describe('CTE with Joins', () => {
    it('CTE + JOIN', () => {
      setup();
      const r = db.execute(`
        WITH high_earners AS (SELECT * FROM employees WHERE salary > 100000)
        SELECT h.name, d.name AS dept
        FROM high_earners h JOIN departments d ON h.dept_id = d.id
      `);
      assert.ok(r.rows.length >= 1);
    });
  });

  describe('CTE with DML', () => {
    it('CTE in UPDATE', () => {
      setup();
      db.execute(`
        WITH avg_salary AS (SELECT AVG(salary) AS avg FROM employees)
        UPDATE employees SET salary = salary + 1000
        WHERE salary < (SELECT avg FROM avg_salary)
      `);
      const r = db.execute('SELECT name, salary FROM employees ORDER BY salary');
      assert.ok(r.rows[0].salary >= 91000);  // Carol got raise
    });
  });

  describe('Keyword as column name', () => {
    it('RANK as column name', () => {
      db = new Database();
      db.execute('CREATE TABLE t (name TEXT, rank INT)');
      db.execute("INSERT INTO t VALUES ('Alice', 1), ('Bob', 2)");
      const r = db.execute('SELECT name, rank FROM t WHERE rank = 1');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].name, 'Alice');
    });

    it('CTE producing rank column used in outer query', () => {
      setup();
      const r = db.execute(`
        WITH ranked AS (
          SELECT name, RANK() OVER (ORDER BY salary DESC) AS rank
          FROM employees
        )
        SELECT name, rank FROM ranked WHERE rank <= 2
      `);
      assert.equal(r.rows.length, 2);
    });
  });
});
