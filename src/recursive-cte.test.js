// recursive-cte.test.js — Tests for WITH RECURSIVE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Recursive CTE', () => {
  it('basic org hierarchy', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Mgr', 2), (4, 'Dev', 3)");
    
    const r = db.execute(`
      WITH RECURSIVE org AS (
        SELECT id, name, mgr_id, 1 as level FROM emp WHERE mgr_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.mgr_id, o.level + 1 FROM emp e JOIN org o ON e.mgr_id = o.id
      )
      SELECT * FROM org ORDER BY level
    `);
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].name, 'CEO');
    assert.equal(r.rows[0].level, 1);
    assert.equal(r.rows[3].name, 'Dev');
    assert.equal(r.rows[3].level, 4);
  });

  it('fibonacci sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fib AS (
        SELECT 1 as n, 1 as val, 0 as prev
        UNION ALL
        SELECT n + 1, val + prev, val FROM fib WHERE n < 10
      )
      SELECT n, val FROM fib ORDER BY n
    `);
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].val, 1);
    assert.equal(r.rows[1].val, 1);
    assert.equal(r.rows[2].val, 2);
    assert.equal(r.rows[3].val, 3);
    assert.equal(r.rows[4].val, 5);
    assert.equal(r.rows[5].val, 8);
  });

  it('counting with recursive CTE', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE cnt AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM cnt WHERE n < 20
      )
      SELECT * FROM cnt
    `);
    assert.equal(r.rows.length, 20);
    assert.equal(r.rows[19].n, 20);
  });

  it('non-recursive CTE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    
    const r = db.execute(`
      WITH summary AS (
        SELECT SUM(val) as total, AVG(val) as avg_val FROM t
      )
      SELECT * FROM summary
    `);
    assert.equal(r.rows[0].total, 60);
  });

  it('graph traversal with tree structure', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'c'), (5, 2, 'd'), (6, 3, 'e')");
    
    const r = db.execute(`
      WITH RECURSIVE descendants AS (
        SELECT id, name, 0 as depth FROM tree WHERE parent_id IS NULL
        UNION ALL
        SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id
      )
      SELECT * FROM descendants ORDER BY depth, id
    `);
    assert.equal(r.rows.length, 6);
    assert.equal(r.rows[0].name, 'root');
    assert.equal(r.rows[0].depth, 0);
    assert.equal(r.rows[5].depth, 2);
  });
});
