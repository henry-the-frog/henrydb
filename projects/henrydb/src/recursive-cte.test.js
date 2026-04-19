// recursive-cte.test.js — Recursive CTE edge cases

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Recursive CTE — Basic', () => {
  it('counting to N', () => {
    const db = new Database();
    const r = db.execute('WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT * FROM cnt');
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].n, 1);
    assert.equal(r.rows[9].n, 10);
  });

  it('Fibonacci sequence', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE fib(n, a, b) AS (
        SELECT 1, 0, 1
        UNION ALL
        SELECT n + 1, b, a + b FROM fib WHERE n < 10
      )
      SELECT n, a as fib FROM fib ORDER BY n
    `);
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].fib, 0);
    assert.equal(r.rows[1].fib, 1);
    assert.equal(r.rows[9].fib, 34);
  });

  it('generate_series equivalent', () => {
    const db = new Database();
    const r = db.execute('WITH RECURSIVE s(n) AS (SELECT 5 UNION ALL SELECT n + 5 FROM s WHERE n < 50) SELECT * FROM s');
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].n, 5);
    assert.equal(r.rows[9].n, 50);
  });
});

describe('Recursive CTE — Tree Traversal', () => {
  it('all descendants from root', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'A'), (3, 1, 'B'), (4, 2, 'C'), (5, 2, 'D'), (6, 3, 'E')");
    
    const r = db.execute(`
      WITH RECURSIVE desc AS (
        SELECT id, name, 0 as depth FROM tree WHERE parent_id IS NULL
        UNION ALL
        SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN desc d ON t.parent_id = d.id
      )
      SELECT * FROM desc ORDER BY depth, id
    `);
    assert.equal(r.rows.length, 6);
    assert.equal(r.rows[0].depth, 0);
    assert.equal(r.rows[5].depth, 2);
  });

  it('ancestors of a node', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'A'), (3, 2, 'B'), (4, 3, 'C')");
    
    const r = db.execute(`
      WITH RECURSIVE anc AS (
        SELECT id, parent_id, name FROM tree WHERE id = 4
        UNION ALL
        SELECT t.id, t.parent_id, t.name FROM tree t JOIN anc a ON t.id = a.parent_id
      )
      SELECT * FROM anc ORDER BY id
    `);
    assert.equal(r.rows.length, 4); // 4, 3, 2, 1
    assert.equal(r.rows[0].name, 'root');
    assert.equal(r.rows[3].name, 'C');
  });

  it('path from root to each node', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'A'), (2, 1, 'B'), (3, 2, 'C')");
    
    const r = db.execute(`
      WITH RECURSIVE path AS (
        SELECT id, name, name as full_path FROM tree WHERE parent_id IS NULL
        UNION ALL
        SELECT t.id, t.name, p.full_path || '/' || t.name FROM tree t JOIN path p ON t.parent_id = p.id
      )
      SELECT * FROM path ORDER BY id
    `);
    assert.equal(r.rows[0].full_path, 'A');
    assert.equal(r.rows[1].full_path, 'A/B');
    assert.equal(r.rows[2].full_path, 'A/B/C');
  });

  it('subtree sum', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, value INT)');
    db.execute('INSERT INTO tree VALUES (1, NULL, 100), (2, 1, 50), (3, 1, 30), (4, 2, 20), (5, 2, 10)');
    
    const r = db.execute(`
      WITH RECURSIVE subtree AS (
        SELECT id, value FROM tree WHERE id = 2
        UNION ALL
        SELECT t.id, t.value FROM tree t JOIN subtree s ON t.parent_id = s.id
      )
      SELECT SUM(value) as total FROM subtree
    `);
    assert.equal(r.rows[0].total, 80); // 50 + 20 + 10
  });
});

describe('Recursive CTE — Depth Limits', () => {
  it('depth limit with JOIN stops correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE graph (id INT PRIMARY KEY, next_id INT)');
    db.execute('INSERT INTO graph VALUES (1, 2), (2, 3), (3, 1)');
    
    const r = db.execute(`
      WITH RECURSIVE path AS (
        SELECT id, next_id, 1 as depth FROM graph WHERE id = 1
        UNION ALL
        SELECT g.id, g.next_id, p.depth + 1 FROM graph g JOIN path p ON g.id = p.next_id WHERE p.depth < 5
      )
      SELECT * FROM path
    `);
    assert.equal(r.rows.length, 5, 'Should stop at depth 5');
    assert.equal(r.rows[4].depth, 5);
  });

  it('linear chain terminates naturally', () => {
    const db = new Database();
    db.execute('CREATE TABLE chain (id INT PRIMARY KEY, next_id INT)');
    db.execute('INSERT INTO chain VALUES (1, 2), (2, 3), (3, 4), (4, NULL)');
    
    const r = db.execute(`
      WITH RECURSIVE path AS (
        SELECT id, next_id FROM chain WHERE id = 1
        UNION ALL
        SELECT c.id, c.next_id FROM chain c JOIN path p ON c.id = p.next_id WHERE p.next_id IS NOT NULL
      )
      SELECT * FROM path ORDER BY id
    `);
    assert.equal(r.rows.length, 4);
  });
});

describe('Recursive CTE — Cycle Detection', () => {
  it('CYCLE clause detects and marks cycles', () => {
    const db = new Database();
    db.execute('CREATE TABLE graph (id INT PRIMARY KEY, next_id INT)');
    db.execute('INSERT INTO graph VALUES (1, 2), (2, 3), (3, 1)');
    
    const r = db.execute(`
      WITH RECURSIVE path AS (
        SELECT id, next_id FROM graph WHERE id = 1
        UNION ALL
        SELECT g.id, g.next_id FROM graph g JOIN path p ON g.id = p.next_id
      )
      CYCLE id SET is_cycle USING cycle_path
      SELECT * FROM path
    `);
    // Should include cycle row marked as is_cycle
    const cycleRows = r.rows.filter(row => row.is_cycle === true);
    assert.ok(cycleRows.length > 0, 'Should detect cycle');
  });

  it('UNION (not UNION ALL) deduplicates rows', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1
        UNION
        SELECT CASE WHEN n < 5 THEN n + 1 ELSE 1 END FROM nums WHERE n <= 5
      )
      SELECT * FROM nums ORDER BY n
    `);
    // UNION deduplicates, so we get exactly 1-5
    assert.equal(r.rows.length, 5);
  });
});
