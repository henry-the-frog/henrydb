// recursive-cte.test.js — Tests for WITH RECURSIVE
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Recursive CTEs', () => {
  let db;
  before(() => {
    db = new Database();
    
    // Org chart
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'CEO', NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'VP Eng', 1)");
    db.execute("INSERT INTO employees VALUES (3, 'VP Sales', 1)");
    db.execute("INSERT INTO employees VALUES (4, 'Dev Lead', 2)");
    db.execute("INSERT INTO employees VALUES (5, 'Dev', 4)");
    db.execute("INSERT INTO employees VALUES (6, 'QA', 2)");
    db.execute("INSERT INTO employees VALUES (7, 'Sales Rep', 3)");
    
    // Graph
    db.execute('CREATE TABLE edges (src INT, dst INT)');
    db.execute('INSERT INTO edges VALUES (1, 2), (2, 3), (3, 4), (1, 5), (5, 6)');
  });

  it('simple counter', () => {
    const r = db.execute(`
      WITH RECURSIVE cnt(x) AS (
        SELECT 1
        UNION ALL
        SELECT x + 1 FROM cnt WHERE x < 5
      )
      SELECT * FROM cnt
    `);
    assert.strictEqual(r.rows.length, 5);
    assert.deepStrictEqual(r.rows.map(r => r.x), [1, 2, 3, 4, 5]);
  });

  it('factorial', () => {
    const r = db.execute(`
      WITH RECURSIVE fact(n, f) AS (
        SELECT 1 as n, 1 as f
        UNION ALL
        SELECT n + 1, f * (n + 1) FROM fact WHERE n < 10
      )
      SELECT * FROM fact
    `);
    assert.strictEqual(r.rows.length, 10);
    assert.strictEqual(r.rows[0].f, 1);   // 1! = 1
    assert.strictEqual(r.rows[4].f, 120); // 5! = 120
    assert.strictEqual(r.rows[9].f, 3628800); // 10! = 3628800
  });

  it('fibonacci', () => {
    const r = db.execute(`
      WITH RECURSIVE fib(n, a, b) AS (
        SELECT 1 as n, 0 as a, 1 as b
        UNION ALL
        SELECT n + 1, b, a + b FROM fib WHERE n < 10
      )
      SELECT n, a as fib_n FROM fib
    `);
    assert.strictEqual(r.rows.length, 10);
    assert.deepStrictEqual(
      r.rows.map(r => r.fib_n),
      [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    );
  });

  it('org chart traversal (tree walk)', () => {
    const r = db.execute(`
      WITH RECURSIVE org(id, name, level) AS (
        SELECT id, name, 0 as level FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, org.level + 1
        FROM employees e JOIN org ON e.manager_id = org.id
      )
      SELECT * FROM org ORDER BY level, name
    `);
    assert.strictEqual(r.rows.length, 7);
    assert.strictEqual(r.rows[0].name, 'CEO');
    assert.strictEqual(r.rows[0].level, 0);
    assert.ok(r.rows.some(r => r.level === 3)); // Dev is 3 levels deep
  });

  it('org chart with path', () => {
    const r = db.execute(`
      WITH RECURSIVE org(id, name, path) AS (
        SELECT id, name, name as path FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, org.path || ' > ' || e.name
        FROM employees e JOIN org ON e.manager_id = org.id
      )
      SELECT * FROM org ORDER BY path
    `);
    assert.ok(r.rows.some(r => r.path.includes('>')));
    // Dev should have the longest path
    const devRow = r.rows.find(r => r.name === 'Dev');
    assert.ok(devRow.path.includes('CEO > VP Eng > Dev Lead > Dev'));
  });

  it('graph reachability', () => {
    const r = db.execute(`
      WITH RECURSIVE reach(node) AS (
        SELECT 1 as node
        UNION ALL
        SELECT e.dst FROM edges e JOIN reach r ON e.src = r.node
      )
      SELECT DISTINCT node FROM reach ORDER BY node
    `);
    assert.ok(r.rows.length >= 4); // 1 → 2 → 3 → 4, 1 → 5 → 6
  });

  it('powers of 2', () => {
    const r = db.execute(`
      WITH RECURSIVE pow2(n, val) AS (
        SELECT 0 as n, 1 as val
        UNION ALL
        SELECT n + 1, val * 2 FROM pow2 WHERE n < 10
      )
      SELECT * FROM pow2
    `);
    assert.strictEqual(r.rows.length, 11);
    assert.strictEqual(r.rows[10].val, 1024); // 2^10
  });

  it('string building', () => {
    const r = db.execute(`
      WITH RECURSIVE build(n, s) AS (
        SELECT 1 as n, 'A' as s
        UNION ALL
        SELECT n + 1, s || 'A' FROM build WHERE n < 5
      )
      SELECT * FROM build
    `);
    assert.strictEqual(r.rows.length, 5);
    assert.strictEqual(r.rows[4].s, 'AAAAA');
  });
});
