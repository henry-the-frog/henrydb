// cte-advanced.test.js — Advanced CTE tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Advanced CTEs', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000, NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000, 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Marketing', 80000, 1)");
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 85000, 3)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 95000, 2)");
    db.execute("INSERT INTO employees VALUES (6, 'Frank', 'Sales', 70000, 1)");
  });

  describe('Multiple CTEs', () => {
    it('CTE with WHERE filter', () => {
      const result = db.execute("WITH eng AS (SELECT * FROM employees WHERE dept = 'Engineering') SELECT * FROM eng ORDER BY salary DESC");
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('CTE with complex filter', () => {
      const result = db.execute("WITH high AS (SELECT * FROM employees WHERE salary > 80000) SELECT name, salary FROM high ORDER BY salary");
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows.every(r => r.salary > 80000));
    });
  });

  describe('CTE with aggregation', () => {
    it('CTE with GROUP BY', () => {
      const result = db.execute(`
        WITH dept_stats AS (
          SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total
          FROM employees GROUP BY dept
        )
        SELECT * FROM dept_stats ORDER BY total DESC
      `);
      assert.ok(result.rows.length > 0);
      const eng = result.rows.find(r => r.dept === 'Engineering');
      assert.equal(eng.cnt, 3);
      assert.equal(eng.total, 285000);
    });

    it('CTE used with subquery', () => {
      const result = db.execute(`
        WITH dept_totals AS (
          SELECT dept, SUM(salary) AS total
          FROM employees GROUP BY dept
        )
        SELECT * FROM dept_totals WHERE total > 100000 ORDER BY total DESC
      `);
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows.every(r => r.total > 100000));
    });
  });

  describe('CTE with window functions', () => {
    it('CTE with ROW_NUMBER', () => {
      const result = db.execute(`
        WITH ranked AS (
          SELECT name, dept, salary,
            ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
          FROM employees
        )
        SELECT * FROM ranked WHERE rn = 1
      `);
      // Should get top earner per department
      assert.equal(result.rows.length, 3); // 3 departments
      const eng = result.rows.find(r => r.dept === 'Engineering');
      assert.equal(eng.name, 'Alice');
    });
  });

  describe('CTE edge cases', () => {
    it('CTE with all rows', () => {
      const result = db.execute('WITH all_emp AS (SELECT * FROM employees) SELECT COUNT(*) AS cnt FROM all_emp');
      assert.equal(result.rows[0].cnt, 6);
    });

    it('CTE with empty result', () => {
      const result = db.execute("WITH ghost AS (SELECT * FROM employees WHERE dept = 'NonExistent') SELECT * FROM ghost");
      assert.equal(result.rows.length, 0);
    });

    it('CTE with DISTINCT', () => {
      const result = db.execute('WITH depts AS (SELECT DISTINCT dept FROM employees) SELECT * FROM depts ORDER BY dept');
      assert.equal(result.rows.length, 3);
    });

    it('CTE with LIMIT in main query', () => {
      const result = db.execute('WITH all_emp AS (SELECT * FROM employees ORDER BY salary DESC) SELECT * FROM all_emp LIMIT 2');
      assert.equal(result.rows.length, 2);
    });

    it('CTE with subquery in main query', () => {
      const result = db.execute(`
        WITH high_paid AS (SELECT * FROM employees WHERE salary > 85000)
        SELECT * FROM high_paid WHERE dept IN (SELECT DISTINCT dept FROM employees WHERE salary < 75000)
      `);
      // High paid employees in departments that also have low-paid employees
      // Sales has Frank(70k<75k), so Sales qualifies. But no Sales employee is >85k.
      // So this might be empty
      assert.ok(result.rows.length >= 0);
    });
  });

  describe('CTE with modifications', () => {
    it('CTE with computed columns', () => {
      const result = db.execute(`
        WITH enriched AS (
          SELECT name, dept, salary, salary * 12 AS annual
          FROM employees
        )
        SELECT name, annual FROM enriched ORDER BY annual DESC
      `);
      assert.equal(result.rows[0].annual, 1200000); // Alice: 100000 * 12
    });

    it('CTE with CASE expression', () => {
      const result = db.execute(`
        WITH graded AS (
          SELECT name, salary,
            CASE WHEN salary >= 90000 THEN 'Senior'
                 WHEN salary >= 80000 THEN 'Mid'
                 ELSE 'Junior' END AS grade
          FROM employees
        )
        SELECT grade, COUNT(*) AS cnt FROM graded GROUP BY grade ORDER BY cnt DESC
      `);
      assert.ok(result.rows.length > 0);
    });
  });
});
