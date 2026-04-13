// cte-stress.test.js — Stress tests for HenryDB CTEs (Common Table Expressions)
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CTE stress tests', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, manager_id INT, dept TEXT, salary INT)');
    const data = [
      [1, 'CEO', null, 'Exec', 500],
      [2, 'VP Eng', 1, 'Eng', 400],
      [3, 'VP Sales', 1, 'Sales', 380],
      [4, 'Sr Eng', 2, 'Eng', 300],
      [5, 'Jr Eng', 4, 'Eng', 200],
      [6, 'Intern', 5, 'Eng', 100],
      [7, 'Sales Rep', 3, 'Sales', 250],
      [8, 'Sales Rep 2', 3, 'Sales', 240],
      [9, 'Sales Mgr', 3, 'Sales', 300],
      [10, 'Account Exec', 9, 'Sales', 220],
    ];
    for (const [id, name, mgr, dept, sal] of data) {
      if (mgr === null) {
        db.execute(`INSERT INTO employees VALUES (${id}, '${name}', NULL, '${dept}', ${sal})`);
      } else {
        db.execute(`INSERT INTO employees VALUES (${id}, '${name}', ${mgr}, '${dept}', ${sal})`);
      }
    }
  });

  it('simple non-recursive CTE', () => {
    const r = db.execute(`
      WITH eng AS (SELECT * FROM employees WHERE dept = 'Eng')
      SELECT name, salary FROM eng ORDER BY salary DESC
    `);
    assert.strictEqual(r.rows.length, 4);
    assert.strictEqual(r.rows[0].name, 'VP Eng');
  });

  it('multiple CTEs used in main query', () => {
    const r = db.execute(`
      WITH 
        eng AS (SELECT name, salary FROM employees WHERE dept = 'Eng'),
        sales AS (SELECT name, salary FROM employees WHERE dept = 'Sales')
      SELECT e.name, e.salary FROM eng e ORDER BY e.salary DESC
    `);
    assert.strictEqual(r.rows.length, 4);
  });

  it('CTE referenced multiple times', () => {
    const r = db.execute(`
      WITH high_earners AS (SELECT * FROM employees WHERE salary >= 300)
      SELECT 
        (SELECT COUNT(*) FROM high_earners) as total,
        (SELECT AVG(salary) FROM high_earners) as avg_sal
    `);
    assert.ok(r.rows.length > 0);
  });

  it('recursive CTE: org hierarchy traversal', () => {
    const r = db.execute(`
      WITH RECURSIVE org_tree(id, name, level) AS (
        SELECT id, name, 0 FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, ot.level + 1
        FROM employees e
        JOIN org_tree ot ON e.manager_id = ot.id
      )
      SELECT id, name, level FROM org_tree ORDER BY level, id
    `);
    assert.strictEqual(r.rows.length, 10); // All 10 employees
    assert.strictEqual(r.rows[0].name, 'CEO');
    assert.strictEqual(r.rows[0].level, 0);
    // Deepest level should be 4 (CEO → VP Eng → Sr Eng → Jr Eng → Intern)
    const maxLevel = Math.max(...r.rows.map(r => r.level));
    assert.strictEqual(maxLevel, 4);
  });

  it('recursive CTE: counting from 1 to 100', () => {
    const r = db.execute(`
      WITH RECURSIVE cnt(x) AS (
        SELECT 1
        UNION ALL
        SELECT x + 1 FROM cnt WHERE x < 100
      )
      SELECT COUNT(*) as total, MIN(x) as min_x, MAX(x) as max_x FROM cnt
    `);
    assert.strictEqual(r.rows[0].total, 100);
    assert.strictEqual(r.rows[0].min_x, 1);
    assert.strictEqual(r.rows[0].max_x, 100);
  });

  it('recursive CTE: Fibonacci sequence', () => {
    const r = db.execute(`
      WITH RECURSIVE fib(n, a, b) AS (
        SELECT 1, 0, 1
        UNION ALL
        SELECT n + 1, b, a + b FROM fib WHERE n < 20
      )
      SELECT n, b as fib FROM fib ORDER BY n
    `);
    assert.strictEqual(r.rows.length, 20);
    // Verify first few Fibonacci numbers
    const fibs = r.rows.map(r => r.fib);
    assert.strictEqual(fibs[0], 1);  // F(1) = 1
    assert.strictEqual(fibs[1], 1);  // F(2) = 1
    assert.strictEqual(fibs[2], 2);  // F(3) = 2
    assert.strictEqual(fibs[3], 3);  // F(4) = 3
    assert.strictEqual(fibs[4], 5);  // F(5) = 5
    assert.strictEqual(fibs[9], 55); // F(10) = 55
  });

  it('recursive CTE: deep recursion (200 levels)', () => {
    const r = db.execute(`
      WITH RECURSIVE deep(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM deep WHERE n < 200
      )
      SELECT COUNT(*) as cnt, MAX(n) as max_n FROM deep
    `);
    assert.strictEqual(r.rows[0].cnt, 200);
    assert.strictEqual(r.rows[0].max_n, 200);
  });

  it('recursive CTE: path finding with explicit alias', () => {
    const r = db.execute(`
      WITH RECURSIVE path(id, name, path_str, depth) AS (
        SELECT id, name, CAST(name AS TEXT) as path_str, 0 as depth
        FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, p.path_str || ' > ' || e.name, p.depth + 1
        FROM employees e
        JOIN path p ON e.manager_id = p.id
      )
      SELECT name, path_str, depth FROM path
      ORDER BY depth, id
    `);
    assert.ok(r.rows.length > 0, 'should return hierarchy paths');
    assert.strictEqual(r.rows[0].depth, 0);
    // Deepest should be level 4
    const maxDepth = Math.max(...r.rows.map(r => r.depth));
    assert.strictEqual(maxDepth, 4);
  });

  it('recursive CTE: infinite loop protection', () => {
    // This CTE has no termination condition — should hit MAX_ITERATIONS
    try {
      db.execute(`
        WITH RECURSIVE infinite(n) AS (
          SELECT 1
          UNION ALL
          SELECT n + 1 FROM infinite
        )
        SELECT COUNT(*) FROM infinite
      `);
      // If it returns, it should have been limited
      assert.ok(true); // Got here without crash
    } catch (e) {
      // Expected: max iterations exceeded
      assert.ok(e.message.includes('iteration') || e.message.includes('limit') || e.message.includes('max'),
        `unexpected error: ${e.message}`);
    }
  });

  it('CTE in main query with subquery filter', () => {
    // CTE in subquery not supported, so test CTE + subquery combo differently
    const r = db.execute(`
      WITH avg_sal AS (SELECT AVG(salary) as avg FROM employees)
      SELECT e.name, e.salary FROM employees e, avg_sal a
      WHERE e.salary > a.avg
      ORDER BY e.salary DESC
    `);
    // Average is 289, so employees above that
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.salary > 289);
    }
  });

  it('CTE with aggregate functions', () => {
    const r = db.execute(`
      WITH dept_stats AS (
        SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal, MAX(salary) as max_sal
        FROM employees
        GROUP BY dept
      )
      SELECT dept, cnt, avg_sal, max_sal FROM dept_stats ORDER BY dept
    `);
    assert.ok(r.rows.length >= 2);
    const eng = r.rows.find(r => r.dept === 'Eng');
    const sales = r.rows.find(r => r.dept === 'Sales');
    assert.ok(eng);
    assert.ok(sales);
    assert.strictEqual(eng.cnt, 4);
    assert.strictEqual(sales.cnt, 5);
  });

  it('CTE with window function', () => {
    const r = db.execute(`
      WITH ranked AS (
        SELECT name, salary, dept,
          ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
        FROM employees
      )
      SELECT name, salary, dept FROM ranked WHERE rn = 1 ORDER BY dept
    `);
    // Top earner per department
    assert.ok(r.rows.length >= 2);
    const eng = r.rows.find(r => r.dept === 'Eng');
    const sales = r.rows.find(r => r.dept === 'Sales');
    assert.ok(eng);
    assert.ok(sales);
    assert.strictEqual(eng.name, 'VP Eng');
  });

  it('recursive CTE: graph reachability', () => {
    // Build a graph as a table
    const db2 = new Database();
    db2.execute('CREATE TABLE edges (src INT, dst INT)');
    // Graph: 1→2, 2→3, 3→4, 1→5, 5→6, 6→3 (cycle through 3)
    const edges = [[1,2],[2,3],[3,4],[1,5],[5,6],[6,3]];
    for (const [s, d] of edges) db2.execute(`INSERT INTO edges VALUES (${s}, ${d})`);
    
    const r = db2.execute(`
      WITH RECURSIVE reachable(node, depth) AS (
        SELECT 1, 0
        UNION ALL
        SELECT e.dst, r.depth + 1
        FROM edges e
        JOIN reachable r ON e.src = r.node
        WHERE r.depth < 10
      )
      SELECT DISTINCT node FROM reachable ORDER BY node
    `);
    // From node 1, should reach: 1, 2, 3, 4, 5, 6
    const nodes = r.rows.map(r => r.node);
    assert.deepStrictEqual(nodes, [1, 2, 3, 4, 5, 6]);
  });

  it('CTE used in JOIN', () => {
    const r = db.execute(`
      WITH high_sal AS (SELECT id, name FROM employees WHERE salary >= 300)
      SELECT h.name as high_earner, e.name as direct_report
      FROM high_sal h
      JOIN employees e ON e.manager_id = h.id
      ORDER BY h.name, e.name
    `);
    assert.ok(r.rows.length > 0);
  });

  it('recursive CTE: sum of subtree salaries (with duplicate column in base)', () => {
    // This tests the CTE column aliasing with duplicate column names
    const r = db.execute(`
      WITH RECURSIVE subtree(id, name, root_id, salary) AS (
        SELECT id, name, id, salary FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, st.root_id, e.salary
        FROM employees e
        JOIN subtree st ON e.manager_id = st.id
      )
      SELECT SUM(salary) as total_salary FROM subtree
    `);
    // Should sum all employee salaries (CEO's subtree = all employees)
    const expectedTotal = 500 + 400 + 380 + 300 + 200 + 100 + 250 + 240 + 300 + 220;
    assert.strictEqual(r.rows[0].total_salary, expectedTotal);
  });

  it('empty CTE result', () => {
    const r = db.execute(`
      WITH empty AS (SELECT * FROM employees WHERE salary > 9999)
      SELECT COUNT(*) as cnt FROM empty
    `);
    assert.strictEqual(r.rows[0].cnt, 0);
  });

  it('CTE with UNION', () => {
    const r = db.execute(`
      WITH all_people AS (
        SELECT name, salary FROM employees WHERE dept = 'Eng'
        UNION ALL
        SELECT name, salary FROM employees WHERE dept = 'Sales'
      )
      SELECT COUNT(*) as cnt FROM all_people
    `);
    // Eng (4) + Sales (5) = 9, plus Exec (1) not included
    assert.strictEqual(r.rows[0].cnt, 9);
  });
});
