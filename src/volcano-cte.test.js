// volcano-cte.test.js — CTE support in volcano planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano CTE', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000, NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 80000, 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 90000, NULL)");
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 70000, 3)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 95000, 1)");
  });

  function volcanoQuery(sql) {
    const ast = parse(sql);
    const plan = buildPlan(ast, db.tables);
    return plan.toArray();
  }

  // ===== Non-recursive CTEs =====

  it('simple CTE with filter', () => {
    const rows = volcanoQuery(
      'WITH high_earners AS (SELECT id, name, salary FROM employees WHERE salary > 85000) SELECT name, salary FROM high_earners'
    );
    assert.equal(rows.length, 3);
    const names = rows.map(r => r.name);
    assert.ok(names.includes('Alice'));
    assert.ok(names.includes('Charlie'));
    assert.ok(names.includes('Eve'));
  });

  it('CTE with column rename', () => {
    const rows = volcanoQuery(
      'WITH dept_stats(department, headcount) AS (SELECT dept, COUNT(*) FROM employees GROUP BY dept) SELECT department, headcount FROM dept_stats'
    );
    assert.equal(rows.length, 2);
    const eng = rows.find(r => r.department === 'Engineering');
    assert.ok(eng);
    assert.equal(eng.headcount, 3);
  });

  it('CTE used in JOIN', () => {
    const rows = volcanoQuery(
      'WITH eng AS (SELECT id, name FROM employees WHERE dept = \'Engineering\') SELECT e.name, emp.salary FROM eng e JOIN employees emp ON e.id = emp.id'
    );
    assert.equal(rows.length, 3);
    const alice = rows.find(r => r['e.name'] === 'Alice' || r.name === 'Alice');
    assert.ok(alice);
  });

  it('CTE with ORDER BY and LIMIT', () => {
    const rows = volcanoQuery(
      'WITH ranked AS (SELECT name, salary FROM employees) SELECT name, salary FROM ranked ORDER BY salary DESC LIMIT 3'
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].salary, 100000);
  });

  it('CTE with aggregation', () => {
    const rows = volcanoQuery(
      'WITH dept_avg AS (SELECT dept, AVG(salary) as avg_sal FROM employees GROUP BY dept) SELECT dept, avg_sal FROM dept_avg ORDER BY avg_sal DESC'
    );
    assert.equal(rows.length, 2);
  });

  // ===== Recursive CTEs =====

  it('recursive CTE — counting', () => {
    const rows = volcanoQuery(
      'WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT n FROM nums'
    );
    assert.equal(rows.length, 10);
    assert.deepEqual(rows.map(r => r.n), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('recursive CTE — powers of 2', () => {
    const rows = volcanoQuery(
      'WITH RECURSIVE powers(val) AS (SELECT 1 UNION ALL SELECT val*2 FROM powers WHERE val < 100) SELECT val FROM powers'
    );
    assert.deepEqual(rows.map(r => r.val), [1, 2, 4, 8, 16, 32, 64, 128]);
  });

  it('recursive CTE — fibonacci-like', () => {
    // Generate numbers doubling: 1, 3, 7, 15, 31
    const rows = volcanoQuery(
      'WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x*2+1 FROM seq WHERE x < 20) SELECT x FROM seq'
    );
    assert.deepEqual(rows.map(r => r.x), [1, 3, 7, 15, 31]);
  });

  it('recursive CTE stops at max depth', () => {
    // No WHERE clause termination — should stop at depth 100
    const rows = volcanoQuery(
      'WITH RECURSIVE inf(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM inf) SELECT x FROM inf'
    );
    assert.equal(rows.length, 101); // base(1) + 100 recursive steps
  });

  // ===== Multiple CTEs =====

  it('multiple CTEs', () => {
    const rows = volcanoQuery(
      `WITH 
        eng AS (SELECT name, salary FROM employees WHERE dept = 'Engineering'),
        sales AS (SELECT name, salary FROM employees WHERE dept = 'Sales')
      SELECT name, salary FROM eng ORDER BY salary DESC`
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, 'Alice');
  });

  // ===== EXPLAIN =====

  it('CTE plan describes correctly', () => {
    const ast = parse('WITH high AS (SELECT id FROM employees WHERE salary > 90000) SELECT id FROM high');
    const plan = buildPlan(ast, db.tables);
    const desc = plan.describe();
    assert.ok(desc); // Just verify it doesn't crash
  });
});
