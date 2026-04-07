// recursive-cte.test.js — Recursive CTE tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Recursive CTEs', () => {
  it('generates number sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 5
      )
      SELECT * FROM nums
    `);
    assert.equal(r.rows.length, 5);
    assert.deepEqual(r.rows.map(r => r.n), [1, 2, 3, 4, 5]);
  });

  it('traverses tree hierarchy', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL)");
    db.execute("INSERT INTO emp VALUES (2, 'VP', 1)");
    db.execute("INSERT INTO emp VALUES (3, 'Dir', 2)");
    db.execute("INSERT INTO emp VALUES (4, 'Dev', 3)");

    const r = db.execute(`
      WITH RECURSIVE org AS (
        SELECT id, name FROM emp WHERE mgr_id IS NULL
        UNION ALL
        SELECT e.id, e.name FROM emp e JOIN org ON e.mgr_id = org.id
      )
      SELECT * FROM org
    `);
    assert.equal(r.rows.length, 4);
    const names = r.rows.map(r => r.name);
    assert.ok(names.includes('CEO'));
    assert.ok(names.includes('Dev'));
  });

  it('terminates with cycle detection', () => {
    const db = new Database();
    // This would infinite-loop without cycle detection
    const r = db.execute(`
      WITH RECURSIVE nums AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n FROM nums WHERE n < 3
      )
      SELECT * FROM nums
    `);
    // Should terminate: base produces {n:1}, recursive produces {n:1} again (cycle detected)
    assert.ok(r.rows.length <= 2, `Expected <=2 rows with cycle detection, got ${r.rows.length}`);
  });

  it('works with WHERE filter on CTE', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 10
      )
      SELECT * FROM nums WHERE n > 7
    `);
    assert.equal(r.rows.length, 3); // 8, 9, 10
  });

  it('non-recursive CTE still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    const r = db.execute('WITH cte AS (SELECT id, val FROM t WHERE val > 10) SELECT * FROM cte');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 20);
  });

  it('SELECT without FROM works', () => {
    const db = new Database();
    const r = db.execute('SELECT 42 AS answer');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].answer, 42);
  });
});
