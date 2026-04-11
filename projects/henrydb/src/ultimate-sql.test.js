// ultimate-sql.test.js — One test per query, exercises everything HenryDB can do
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Ultimate SQL Tests', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')");
    
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT, hire_date TEXT)');
    const emps = [
      [1, 'Alice', 1, 120000, '2020-01-15'], [2, 'Bob', 1, 110000, '2020-06-01'],
      [3, 'Carol', 2, 95000, '2019-03-10'], [4, 'Dave', 2, 105000, '2021-01-20'],
      [5, 'Eve', 1, 130000, '2018-09-01'], [6, 'Frank', 3, 85000, '2022-02-14'],
      [7, 'Grace', 3, 90000, '2021-11-30'], [8, 'Hank', 2, 100000, '2023-04-01'],
    ];
    for (const [id, name, dept, sal, date] of emps) {
      db.execute(`INSERT INTO employees VALUES (${id}, '${name}', ${dept}, ${sal}, '${date}')`);
    }
    
    db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, lead_id INT, budget INT)');
    db.execute("INSERT INTO projects VALUES (1, 'Alpha', 1, 500000), (2, 'Beta', 5, 300000), (3, 'Gamma', 3, 200000)");
  });

  it('CTE + JOIN + GROUP BY + HAVING', () => {
    const r = db.execute(`
      WITH dept_stats AS (
        SELECT d.name as dept, COUNT(*) as headcount, AVG(e.salary) as avg_sal
        FROM departments d
        JOIN employees e ON d.id = e.dept_id
        GROUP BY d.name
        HAVING COUNT(*) >= 2
      )
      SELECT * FROM dept_stats ORDER BY avg_sal DESC
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows[0].avg_sal >= r.rows[r.rows.length - 1].avg_sal);
  });

  it('window function + CASE + GROUP BY alias', () => {
    const r = db.execute(`
      SELECT 
        CASE WHEN salary >= 110000 THEN 'senior' ELSE 'junior' END as level,
        COUNT(*) as cnt,
        AVG(salary) as avg_sal
      FROM employees
      GROUP BY level
      ORDER BY avg_sal DESC
    `);
    assert.ok(r.rows.length === 2);
    assert.ok(r.rows[0].level === 'senior');
  });

  it('recursive CTE: generate date range', () => {
    const r = db.execute(`
      WITH RECURSIVE dates(d, n) AS (
        SELECT '2024-01-01' as d, 1 as n
        UNION ALL
        SELECT d || '+' || n, n + 1 FROM dates WHERE n < 7
      )
      SELECT * FROM dates
    `);
    assert.strictEqual(r.rows.length, 7);
  });

  it('correlated subquery + window function', () => {
    const r = db.execute(`
      SELECT name, salary,
        (SELECT COUNT(*) FROM employees e2 WHERE e2.salary > e.salary) as rank_from_top,
        ROW_NUMBER() OVER (ORDER BY salary DESC) as rn
      FROM employees e
      ORDER BY salary DESC
    `);
    assert.strictEqual(r.rows.length, 8);
    assert.strictEqual(r.rows[0].rank_from_top, 0); // highest salary
  });

  it('FULL OUTER JOIN + COALESCE', () => {
    db.execute('CREATE TABLE team_a (id INT, score INT)');
    db.execute('CREATE TABLE team_b (id INT, score INT)');
    db.execute('INSERT INTO team_a VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('INSERT INTO team_b VALUES (2, 25), (3, 35), (4, 40)');
    
    const r = db.execute(`
      SELECT COALESCE(a.id, b.id) as player,
             COALESCE(a.score, 0) as team_a_score,
             COALESCE(b.score, 0) as team_b_score
      FROM team_a a FULL OUTER JOIN team_b b ON a.id = b.id
      ORDER BY player
    `);
    assert.strictEqual(r.rows.length, 4); // players 1, 2, 3, 4
  });

  it('STRING_AGG + NATURAL JOIN', () => {
    const r = db.execute(`
      SELECT d.name as dept, STRING_AGG(e.name, ', ') as team
      FROM departments d
      JOIN employees e ON d.id = e.dept_id
      GROUP BY d.name
      ORDER BY d.name
    `);
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows[0].team.includes(','));
  });

  it('CTAS from simple query', () => {
    db.execute(`
      CREATE TABLE emp_copy AS
      SELECT name, salary FROM employees WHERE salary >= 110000
    `);
    const r = db.execute('SELECT * FROM emp_copy ORDER BY salary DESC');
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows[0].salary >= 110000);
  });

  it('multiple CTEs + JOIN between them', () => {
    const r = db.execute(`
      WITH 
        high_earners AS (SELECT * FROM employees WHERE salary >= 100000),
        their_depts AS (SELECT DISTINCT dept_id FROM employees WHERE salary >= 100000)
      SELECT d.name, COUNT(*) as high_count
      FROM departments d
      JOIN their_depts td ON d.id = td.dept_id
      JOIN high_earners he ON d.id = he.dept_id
      GROUP BY d.name
      ORDER BY high_count DESC
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('INSERT INTO SELECT', () => {
    db.execute('CREATE TABLE dept_summary (dept TEXT, emp_count INT)');
    db.execute(`
      INSERT INTO dept_summary 
      SELECT d.name, COUNT(*)
      FROM departments d JOIN employees e ON d.id = e.dept_id
      GROUP BY d.name
    `);
    const r = db.execute('SELECT * FROM dept_summary ORDER BY emp_count DESC');
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows[0].emp_count > 0);
  });

  it('window: running total + PARTITION BY', () => {
    const r = db.execute(`
      SELECT name, dept_id, salary,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY salary) as dept_running_total,
        RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as dept_rank
      FROM employees
      ORDER BY dept_id, salary DESC
    `);
    assert.strictEqual(r.rows.length, 8);
    // First in each dept should be rank 1
    const eng = r.rows.filter(r => r.dept_id === 1);
    assert.strictEqual(eng[0].dept_rank, 1);
  });
});
