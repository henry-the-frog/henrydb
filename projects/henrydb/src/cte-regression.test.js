import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CTE Column Renaming', () => {
  it('basic column rename with WITH a(x)', () => {
    const db = new Database();
    const r = db.execute('WITH a(x) AS (SELECT 1) SELECT x FROM a');
    assert.deepStrictEqual(r.rows, [{ x: 1 }]);
  });

  it('multiple column rename', () => {
    const db = new Database();
    const r = db.execute('WITH a(x, y) AS (SELECT 1, 2) SELECT x, y FROM a');
    assert.deepStrictEqual(r.rows, [{ x: 1, y: 2 }]);
  });

  it('multi-CTE with column reference', () => {
    const db = new Database();
    const r = db.execute(`
      WITH 
        a(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3),
        b(y) AS (SELECT x * 10 FROM a)
      SELECT y FROM b ORDER BY y
    `);
    assert.deepStrictEqual(r.rows, [{ y: 10 }, { y: 20 }, { y: 30 }]);
  });

  it('CTE without column rename still works', () => {
    const db = new Database();
    const r = db.execute('WITH a AS (SELECT 1 as val) SELECT val FROM a');
    assert.deepStrictEqual(r.rows, [{ val: 1 }]);
  });
});

describe('Recursive CTEs', () => {
  it('basic countdown', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE cnt(x) AS (
        SELECT 5
        UNION ALL
        SELECT x - 1 FROM cnt WHERE x > 0
      )
      SELECT x FROM cnt
    `);
    assert.deepStrictEqual(r.rows.map(row => row.x), [5, 4, 3, 2, 1, 0]);
  });

  it('recursive CTE with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL)");
    db.execute("INSERT INTO emp VALUES (2, 'VP', 1)");
    db.execute("INSERT INTO emp VALUES (3, 'Dir', 2)");

    const r = db.execute(`
      WITH RECURSIVE chain(id, name, mgr_id, lvl) AS (
        SELECT id, name, mgr_id, 0 FROM emp WHERE id = 3
        UNION ALL
        SELECT e.id, e.name, e.mgr_id, c.lvl + 1
        FROM chain c JOIN emp e ON c.mgr_id = e.id
      )
      SELECT name, lvl FROM chain ORDER BY lvl
    `);
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].name, 'Dir');
    assert.strictEqual(r.rows[2].name, 'CEO');
  });

  it('UNION deduplicates in recursive CTE', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE dup(n) AS (
        SELECT 1
        UNION
        SELECT CASE WHEN n < 5 THEN n + 1 ELSE 1 END FROM dup WHERE n < 10
      )
      SELECT COUNT(*) as cnt FROM dup
    `);
    assert.strictEqual(r.rows[0].cnt, 5);
  });

  it('deep recursion (1000)', () => {
    const db = new Database();
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 1000
      )
      SELECT COUNT(*) as cnt FROM nums
    `);
    assert.strictEqual(r.rows[0].cnt, 1000);
  });
});

describe('Boolean/Integer Coercion', () => {
  it('comparisons return 1/0 not true/false', () => {
    const db = new Database();
    const val = (sql) => Object.values(db.execute(sql).rows[0])[0];
    assert.strictEqual(val('SELECT 1 = 1'), 1);
    assert.strictEqual(val('SELECT 1 = 2'), 0);
    assert.strictEqual(val('SELECT 3 > 2'), 1);
    assert.strictEqual(val('SELECT 2 < 3'), 1);
  });

  it('NULL propagates in comparisons', () => {
    const db = new Database();
    const val = (sql) => Object.values(db.execute(sql).rows[0])[0];
    assert.strictEqual(val('SELECT 1 = NULL'), null);
  });

  it('IS returns 1/0', () => {
    const db = new Database();
    const val = (sql) => Object.values(db.execute(sql).rows[0])[0];
    assert.strictEqual(val('SELECT NULL IS NULL'), 1);
    assert.strictEqual(val('SELECT 1 IS NOT NULL'), 1);
  });

  it('cross-type numeric coercion', () => {
    const db = new Database();
    const val = (sql) => Object.values(db.execute(sql).rows[0])[0];
    assert.strictEqual(val("SELECT 1 = '1'"), 1);
    assert.strictEqual(val("SELECT 10 > '9'"), 1);
  });
});
