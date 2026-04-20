// recursive-cte.test.js — Recursive CTE tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Recursive CTEs', () => {
  it('simple counting', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE cnt AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM cnt WHERE n < 5
      )
      SELECT n FROM cnt
    `);
    assert.deepEqual(r.rows.map(r => r.n), [1, 2, 3, 4, 5]);
  });

  it('Fibonacci sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fib AS (
        SELECT 1 as n, 0 as a, 1 as b
        UNION ALL
        SELECT n + 1, b, a + b FROM fib WHERE n < 10
      )
      SELECT n, a as fib_val FROM fib
    `);
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].fib_val, 0);
    assert.equal(r.rows[1].fib_val, 1);
    assert.equal(r.rows[5].fib_val, 5);
    assert.equal(r.rows[9].fib_val, 34);
  });

  it('org chart / tree traversal', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO employees VALUES (1,'CEO',NULL),(2,'VP1',1),(3,'VP2',1),(4,'Dir',2),(5,'Mgr',4)");
    
    const r = db.execute(`
      WITH RECURSIVE org AS (
        SELECT id, name, mgr_id, 0 as depth
        FROM employees WHERE mgr_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.mgr_id, o.depth + 1
        FROM employees e JOIN org o ON e.mgr_id = o.id
      )
      SELECT name, depth FROM org ORDER BY depth, name
    `);
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].name, 'CEO');
    assert.equal(r.rows[0].depth, 0);
    assert.equal(r.rows[4].name, 'Mgr');
    assert.equal(r.rows[4].depth, 3);
  });

  it('graph path finding', () => {
    const db = new Database();
    db.execute('CREATE TABLE edges (src INT, dst INT, weight INT)');
    db.execute('INSERT INTO edges VALUES (1,2,10),(2,3,20),(3,4,30),(1,3,25)');
    
    const r = db.execute(`
      WITH RECURSIVE paths AS (
        SELECT src, dst, weight, src || '->' || dst as path
        FROM edges WHERE src = 1
        UNION ALL
        SELECT p.src, e.dst, p.weight + e.weight, p.path || '->' || e.dst
        FROM paths p JOIN edges e ON p.dst = e.src
        WHERE p.weight + e.weight < 100
      )
      SELECT path, weight FROM paths ORDER BY weight
    `);
    assert.ok(r.rows.length >= 2);
    // Should find: 1->2 (10), 1->3 (25), 1->2->3 (30), 1->3->4 (55), 1->2->3->4 (60)
  });

  it('recursive with aggregate on result', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 100
      )
      SELECT SUM(n) as total, COUNT(*) as cnt, AVG(n) as avg_n
      FROM nums
    `);
    assert.equal(r.rows[0].total, 5050);
    assert.equal(r.rows[0].cnt, 100);
    assert.equal(r.rows[0].avg_n, 50.5);
  });

  it('recursive CTE + window function', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 5
      )
      SELECT n, SUM(n) OVER (ORDER BY n) as running_sum
      FROM nums
    `);
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[4].running_sum, 15);
  });
});
