// complex-query-depth.test.js — Complex multi-table query depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-complex-'));
  db = TransactionalDatabase.open(dbDir);
  
  // 5-table schema: company database
  db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, budget INT)');
  db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, hire_date TEXT)');
  db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, dept_id INT, status TEXT)');
  db.execute('CREATE TABLE assignments (employee_id INT, project_id INT, hours INT)');
  db.execute('CREATE TABLE skills (employee_id INT, skill TEXT, level INT)');

  // Departments
  db.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000)");
  db.execute("INSERT INTO departments VALUES (2, 'Sales', 300000)");
  db.execute("INSERT INTO departments VALUES (3, 'HR', 100000)");

  // Employees
  db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, '2020-01-15')");
  db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, '2021-06-01')");
  db.execute("INSERT INTO employees VALUES (3, 'Carol', 2, '2019-03-20')");
  db.execute("INSERT INTO employees VALUES (4, 'Dave', 2, '2022-09-10')");
  db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, '2023-01-05')");

  // Projects
  db.execute("INSERT INTO projects VALUES (1, 'Alpha', 1, 'active')");
  db.execute("INSERT INTO projects VALUES (2, 'Beta', 1, 'completed')");
  db.execute("INSERT INTO projects VALUES (3, 'Gamma', 2, 'active')");

  // Assignments
  db.execute('INSERT INTO assignments VALUES (1, 1, 20)');
  db.execute('INSERT INTO assignments VALUES (1, 2, 10)');
  db.execute('INSERT INTO assignments VALUES (2, 1, 30)');
  db.execute('INSERT INTO assignments VALUES (3, 3, 25)');
  db.execute('INSERT INTO assignments VALUES (4, 3, 15)');

  // Skills
  db.execute("INSERT INTO skills VALUES (1, 'JS', 9)");
  db.execute("INSERT INTO skills VALUES (1, 'SQL', 8)");
  db.execute("INSERT INTO skills VALUES (2, 'JS', 7)");
  db.execute("INSERT INTO skills VALUES (2, 'Python', 6)");
  db.execute("INSERT INTO skills VALUES (3, 'Sales', 9)");
  db.execute("INSERT INTO skills VALUES (4, 'Sales', 5)");
  db.execute("INSERT INTO skills VALUES (5, 'HR', 8)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Multi-Table JOINs', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('4-table JOIN: employees + departments + projects + assignments', () => {
    const r = rows(db.execute(
      'SELECT e.name AS employee, d.name AS dept, p.name AS project, a.hours ' +
      'FROM employees e ' +
      'INNER JOIN departments d ON e.dept_id = d.id ' +
      'INNER JOIN assignments a ON e.id = a.employee_id ' +
      'INNER JOIN projects p ON a.project_id = p.id ' +
      'ORDER BY e.name, p.name'
    ));
    assert.equal(r.length, 5);
    assert.equal(r[0].employee, 'Alice');
    assert.equal(r[0].project, 'Alpha');
    assert.equal(r[0].hours, 20);
  });

  it('5-table query: employee skills on active projects', () => {
    const r = rows(db.execute(
      'SELECT DISTINCT e.name, s.skill, s.level ' +
      'FROM employees e ' +
      'INNER JOIN departments d ON e.dept_id = d.id ' +
      'INNER JOIN assignments a ON e.id = a.employee_id ' +
      'INNER JOIN projects p ON a.project_id = p.id ' +
      'INNER JOIN skills s ON e.id = s.employee_id ' +
      "WHERE p.status = 'active' " +
      'ORDER BY e.name, s.skill'
    ));
    // Alice on Alpha (active): JS, SQL
    // Bob on Alpha (active): JS, Python
    // Carol on Gamma (active): Sales
    // Dave on Gamma (active): Sales
    assert.ok(r.length >= 6);
  });
});

describe('Nested Subqueries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('3-level nested subquery', () => {
    // Find employees in departments that have active projects
    // with employees who have JS skill level > 7
    const r = rows(db.execute(
      'SELECT name FROM employees WHERE dept_id IN (' +
      '  SELECT dept_id FROM projects WHERE id IN (' +
      '    SELECT project_id FROM assignments WHERE employee_id IN (' +
      '      SELECT employee_id FROM skills WHERE skill = \'JS\' AND level > 7' +
      '    )' +
      '  )' +
      ') ORDER BY name'
    ));
    // Alice has JS level 9 → on project Alpha (dept 1) → dept 1 employees: Alice, Bob
    assert.ok(r.some(x => x.name === 'Alice'));
    assert.ok(r.some(x => x.name === 'Bob'));
  });
});

describe('CTE Chains', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('3 CTEs chained together', () => {
    const r = rows(db.execute(
      'WITH ' +
      'active_projects AS (SELECT * FROM projects WHERE status = \'active\'), ' +
      'project_hours AS (SELECT project_id, SUM(hours) AS total_hours FROM assignments GROUP BY project_id), ' +
      'dept_summary AS (SELECT d.name AS dept, SUM(ph.total_hours) AS dept_hours ' +
      '  FROM active_projects ap ' +
      '  INNER JOIN project_hours ph ON ap.id = ph.project_id ' +
      '  INNER JOIN departments d ON ap.dept_id = d.id ' +
      '  GROUP BY d.name) ' +
      'SELECT * FROM dept_summary ORDER BY dept_hours DESC'
    ));
    // Engineering: Alpha(20+30=50) = 50 hours
    // Sales: Gamma(25+15=40) = 40 hours
    assert.equal(r.length, 2);
    assert.equal(r[0].dept, 'Engineering');
    assert.equal(r[0].dept_hours, 50);
  });
});

describe('Complex WHERE Clauses', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('AND + OR + NOT combination', () => {
    const r = rows(db.execute(
      'SELECT name FROM employees WHERE ' +
      '(dept_id = 1 OR dept_id = 2) AND ' +
      "NOT (hire_date > '2022-01-01') " +
      'ORDER BY name'
    ));
    // Dept 1 or 2, hired before 2022
    // Alice (1, 2020), Bob (1, 2021), Carol (2, 2019)
    // Dave (2, 2022-09) is excluded by NOT
    assert.equal(r.length, 3);
    assert.deepEqual(r.map(x => x.name), ['Alice', 'Bob', 'Carol']);
  });

  it('BETWEEN + IN combination', () => {
    const r = rows(db.execute(
      'SELECT name FROM employees WHERE ' +
      'dept_id IN (1, 2) AND ' +
      "hire_date BETWEEN '2020-01-01' AND '2021-12-31' " +
      'ORDER BY name'
    ));
    // Alice (2020), Bob (2021)
    assert.equal(r.length, 2);
  });

  it('subquery + aggregate in WHERE', () => {
    const r = rows(db.execute(
      'SELECT e.name FROM employees e WHERE (' +
      '  SELECT COUNT(*) FROM assignments a WHERE a.employee_id = e.id' +
      ') > 1 ORDER BY e.name'
    ));
    // Alice has 2 assignments
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Alice');
  });
});

describe('Aggregate + Window + CTE Combined', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('department ranking with window function over CTE aggregate', () => {
    const r = rows(db.execute(
      'WITH dept_hours AS (' +
      '  SELECT d.name AS dept, SUM(a.hours) AS total ' +
      '  FROM departments d ' +
      '  INNER JOIN employees e ON d.id = e.dept_id ' +
      '  INNER JOIN assignments a ON e.id = a.employee_id ' +
      '  GROUP BY d.name' +
      ') ' +
      'SELECT dept, total, RANK() OVER (ORDER BY total DESC) AS rnk FROM dept_hours'
    ));
    assert.equal(r.length, 2); // Only Engineering and Sales have assignments
    assert.equal(r[0].rnk, 1);
  });

  it('employee productivity analysis', () => {
    const r = rows(db.execute(
      'WITH emp_projects AS (' +
      '  SELECT e.name, e.dept_id, COUNT(a.project_id) AS num_projects, SUM(a.hours) AS total_hours ' +
      '  FROM employees e ' +
      '  LEFT JOIN assignments a ON e.id = a.employee_id ' +
      '  GROUP BY e.name, e.dept_id' +
      ') ' +
      'SELECT name, num_projects, total_hours, ' +
      '  AVG(total_hours) OVER (PARTITION BY dept_id) AS dept_avg_hours ' +
      'FROM emp_projects ORDER BY name'
    ));
    assert.equal(r.length, 5);
    const alice = r.find(x => x.name === 'Alice');
    assert.equal(alice.num_projects, 2);
    assert.equal(alice.total_hours, 30);
  });
});
