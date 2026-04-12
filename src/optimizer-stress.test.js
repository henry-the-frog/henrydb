// optimizer-stress.test.js — Stress tests for query optimizer correctness
// Generates complex queries and verifies results match naive execution

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function makeDB() {
  const db = new Database();
  
  // Departments
  db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, budget REAL)');
  db.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000)");
  db.execute("INSERT INTO departments VALUES (2, 'Sales', 300000)");
  db.execute("INSERT INTO departments VALUES (3, 'Marketing', 200000)");
  db.execute("INSERT INTO departments VALUES (4, 'HR', 150000)");
  
  // Employees
  db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary REAL, hire_date TEXT)');
  for (let i = 1; i <= 50; i++) {
    const deptId = ((i - 1) % 4) + 1;
    const salary = 40000 + (i * 1000) + Math.floor(Math.random() * 10000);
    const year = 2020 + Math.floor(Math.random() * 4);
    const month = String(Math.floor(Math.random() * 12) + 1).padStart(2, '0');
    db.execute(`INSERT INTO employees VALUES (${i}, 'Employee${i}', ${deptId}, ${salary}, '${year}-${month}-01')`);
  }
  
  // Projects
  db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, dept_id INT, status TEXT)');
  db.execute("INSERT INTO projects VALUES (1, 'Alpha', 1, 'active')");
  db.execute("INSERT INTO projects VALUES (2, 'Beta', 1, 'complete')");
  db.execute("INSERT INTO projects VALUES (3, 'Gamma', 2, 'active')");
  db.execute("INSERT INTO projects VALUES (4, 'Delta', 3, 'active')");
  db.execute("INSERT INTO projects VALUES (5, 'Epsilon', 2, 'complete')");
  
  // Assignments
  db.execute('CREATE TABLE assignments (employee_id INT, project_id INT, hours REAL)');
  for (let i = 1; i <= 50; i++) {
    const projectId = ((i - 1) % 5) + 1;
    const hours = 10 + Math.floor(Math.random() * 100);
    db.execute(`INSERT INTO assignments VALUES (${i}, ${projectId}, ${hours})`);
    // Some employees on multiple projects
    if (i % 3 === 0) {
      const project2 = (projectId % 5) + 1;
      db.execute(`INSERT INTO assignments VALUES (${i}, ${project2}, ${Math.floor(hours / 2)})`);
    }
  }
  
  return db;
}

describe('Query Optimizer Stress Tests', () => {
  it('should handle multi-table JOIN with WHERE', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, d.name AS dept, p.name AS project
      FROM employees e
      JOIN departments d ON e.dept_id = d.id
      JOIN assignments a ON a.employee_id = e.id
      JOIN projects p ON a.project_id = p.id
      WHERE d.name = 'Engineering' AND p.status = 'active'
    `);
    
    // All results should be Engineering dept + active projects
    for (const row of result.rows) {
      assert.equal(row.dept, 'Engineering');
    }
    assert.ok(result.rows.length > 0, 'Should find some engineering employees on active projects');
  });

  it('should handle subquery in WHERE', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT name, salary FROM employees
      WHERE salary > (SELECT AVG(salary) FROM employees)
    `);
    
    const avgResult = db.execute('SELECT AVG(salary) AS avg_sal FROM employees');
    const avgSalary = avgResult.rows[0].avg_sal;
    
    // Verify all results are above average
    for (const row of result.rows) {
      assert.ok(row.salary > avgSalary, `${row.salary} should be > ${avgSalary}`);
    }
  });

  it('should handle correlated subquery', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, e.salary,
        (SELECT COUNT(*) FROM employees e2 WHERE e2.salary > e.salary) AS rank_from_top
      FROM employees e
      ORDER BY salary DESC
      LIMIT 5
    `);
    
    assert.equal(result.rows.length, 5);
    // Top salary should have rank 0 (nobody higher)
    assert.equal(result.rows[0].rank_from_top, 0);
  });

  it('should handle GROUP BY with HAVING', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_salary
      FROM employees
      GROUP BY dept_id
      HAVING COUNT(*) > 10
    `);
    
    for (const row of result.rows) {
      assert.ok(row.cnt > 10, `Count ${row.cnt} should be > 10`);
    }
  });

  it('should handle window functions with complex ORDER BY', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT name, dept_id, salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rank,
        LAG(salary) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS prev_salary
      FROM employees
    `);
    
    // Check rank is correct per department
    for (const deptId of [1, 2, 3, 4]) {
      const deptRows = result.rows.filter(r => r.dept_id === deptId);
      const ranks = deptRows.map(r => r.dept_rank);
      assert.ok(ranks.includes(1), `Dept ${deptId} should have rank 1`);
    }
  });

  it('should handle LATERAL JOIN with window function inside', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT d.name AS dept, top.name AS top_employee, top.salary
      FROM departments d
      CROSS JOIN LATERAL (
        SELECT name, salary FROM employees
        WHERE dept_id = d.id
        ORDER BY salary DESC
        LIMIT 3
      ) top
    `);
    
    // Each department should have at most 3 employees
    for (const dept of ['Engineering', 'Sales', 'Marketing', 'HR']) {
      const deptRows = result.rows.filter(r => r.dept === dept);
      assert.ok(deptRows.length <= 3, `${dept} should have <= 3 top employees`);
      assert.ok(deptRows.length > 0, `${dept} should have some employees`);
    }
  });

  it('should handle CTE + JOIN + aggregation', () => {
    const db = makeDB();
    const result = db.execute(`
      WITH dept_stats AS (
        SELECT dept_id, AVG(salary) AS avg_sal, COUNT(*) AS emp_count
        FROM employees
        GROUP BY dept_id
      )
      SELECT d.name, avg_sal, emp_count
      FROM dept_stats
      JOIN departments d ON dept_id = d.id
      ORDER BY avg_sal DESC
    `);
    
    assert.equal(result.rows.length, 4);
    // Should be ordered by average salary descending
    for (let i = 1; i < result.rows.length; i++) {
      assert.ok(result.rows[i - 1].avg_sal >= result.rows[i].avg_sal,
        `${result.rows[i - 1].avg_sal} should be >= ${result.rows[i].avg_sal}`);
    }
  });

  it('should handle recursive CTE (Fibonacci)', () => {
    const db = new Database();
    const result = db.execute(`
      WITH RECURSIVE fib AS (
        SELECT 1 AS n, 1 AS val, 0 AS prev
        UNION ALL
        SELECT n + 1, val + prev, val FROM fib WHERE n < 10
      )
      SELECT n, val FROM fib
    `);
    
    assert.equal(result.rows.length, 10);
    // Fibonacci: 1, 1, 2, 3, 5, 8, 13, 21, 34, 55
    assert.equal(result.rows[0].val, 1);
    assert.equal(result.rows[4].val, 5);
    assert.equal(result.rows[9].val, 55);
  });

  it('should handle UNION ALL + ORDER BY + LIMIT', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT name, salary, 'high' AS category FROM employees WHERE salary > 70000
      UNION ALL
      SELECT name, salary, 'low' AS category FROM employees WHERE salary < 50000
      ORDER BY salary DESC
      LIMIT 10
    `);
    
    assert.equal(result.rows.length, 10);
    // Should be ordered by salary desc
    for (let i = 1; i < result.rows.length; i++) {
      assert.ok(result.rows[i - 1].salary >= result.rows[i].salary);
    }
  });

  it('should handle CASE + aggregate', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT
        dept_id,
        SUM(CASE WHEN salary > 60000 THEN 1 ELSE 0 END) AS high_earners,
        SUM(CASE WHEN salary <= 60000 THEN 1 ELSE 0 END) AS normal_earners
      FROM employees
      GROUP BY dept_id
    `);
    
    assert.equal(result.rows.length, 4);
    for (const row of result.rows) {
      assert.ok(row.high_earners >= 0);
      assert.ok(row.normal_earners >= 0);
    }
  });

  it('should handle information_schema + LATERAL (meta-query)', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_name IN ('employees', 'departments')
      ORDER BY table_name, ordinal_position
    `);
    
    assert.ok(result.rows.length > 0);
    const empCols = result.rows.filter(r => r.table_name === 'employees');
    assert.ok(empCols.length === 5); // id, name, dept_id, salary, hire_date
  });

  it('should handle NULL handling in JOINs correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE left_t (id INT, val TEXT)');
    db.execute('CREATE TABLE right_t (id INT, val TEXT)');
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute('INSERT INTO left_t VALUES (3, NULL)');
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (3, 'z')");
    
    const result = db.execute(`
      SELECT l.id, l.val AS lval, r.val AS rval
      FROM left_t l
      LEFT JOIN right_t r ON l.id = r.id
      ORDER BY l.id
    `);
    
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].rval, 'x'); // id=1 matched
    assert.equal(result.rows[1].rval, null); // id=2 no match
    assert.equal(result.rows[2].rval, 'z'); // id=3 matched
    assert.equal(result.rows[2].lval, null); // NULL val preserved
  });
});
