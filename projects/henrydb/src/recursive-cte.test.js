// recursive-cte.test.js — WITH RECURSIVE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('WITH RECURSIVE', () => {
  it('generates number sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 5
      ) SELECT n FROM nums
    `);
    assert.deepEqual(r.rows.map(r => r.n), [1, 2, 3, 4, 5]);
  });

  it('hierarchical org chart traversal', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dir', 2), (4, 'Mgr', 3)");
    
    const r = db.execute(`
      WITH RECURSIVE org(id, name, lvl) AS (
        SELECT id, name, 0 FROM emp WHERE id = 1
        UNION ALL
        SELECT e.id, e.name, org.lvl + 1 FROM emp e JOIN org ON e.mgr_id = org.id
      ) SELECT name, lvl FROM org ORDER BY lvl
    `);
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].name, 'CEO');
    assert.equal(r.rows[0].lvl, 0);
    assert.equal(r.rows[3].name, 'Mgr');
    assert.equal(r.rows[3].lvl, 3);
  });

  it('fibonacci sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fib(a, b) AS (
        SELECT 0, 1
        UNION ALL
        SELECT b, a + b FROM fib WHERE b < 100
      ) SELECT a FROM fib
    `);
    assert.ok(r.rows.length >= 10);
    assert.deepEqual(r.rows.slice(0, 8).map(r => r.a), [0, 1, 1, 2, 3, 5, 8, 13]);
  });
});
