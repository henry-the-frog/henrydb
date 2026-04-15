import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Session B Bug Fix Regression Tests', () => {
  // LATERAL WHERE fix
  it('LATERAL JOIN respects WHERE on lateral column', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr INT)');
    db.execute("INSERT INTO emp VALUES (1,'A',NULL),(2,'B',1),(3,'C',1),(4,'D',2)");
    
    const r = db.execute(`
      SELECT e.name, sub.cnt
      FROM emp e,
      LATERAL (SELECT COUNT(*) as cnt FROM emp WHERE mgr = e.id) sub
      WHERE sub.cnt > 0
    `);
    assert.ok(r.rows.length <= 3, 'Should filter out employees with no subordinates');
  });

  // RANGE BETWEEN fix
  it('RANGE BETWEEN uses value-based bounds', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');
    
    const r = db.execute(`
      SELECT val, SUM(val) OVER (ORDER BY val RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) as rs
      FROM t
    `);
    // val=30: range [20,40] → 20+30+40 = 90
    const row30 = r.rows.find(r => r.val === 30);
    assert.equal(row30.rs, 90);
  });

  // FIRST_VALUE/LAST_VALUE fix
  it('LAST_VALUE respects frame spec', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');
    
    const r = db.execute(`
      SELECT val, LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
      FROM t
    `);
    assert.ok(r.rows.every(row => row.lv === 30), 'All rows should see 30 as last value');
  });

  // UNION ORDER BY + LIMIT fix
  it('UNION ALL with ORDER BY and LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    
    const r = db.execute(`
      SELECT id FROM t WHERE id <= 3
      UNION ALL
      SELECT id FROM t WHERE id > 3
      ORDER BY id
      LIMIT 3
    `);
    assert.equal(r.rows.length, 3);
    assert.deepEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  // Boolean expressions in SELECT
  it('IS NULL in SELECT returns true/false', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('x'),(NULL)");
    
    const r = db.execute('SELECT val IS NULL as is_null FROM t ORDER BY val');
    assert.ok(r.rows.some(r => r.is_null === true), 'Should have true for NULL row');
    assert.ok(r.rows.some(r => r.is_null === false), 'Should have false for non-NULL row');
  });

  // ARRAY_AGG ORDER BY fix
  it('ARRAY_AGG with ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(2)');
    
    const r = db.execute('SELECT ARRAY_AGG(id ORDER BY id) as sorted FROM t');
    assert.deepEqual(r.rows[0].sorted, [1, 2, 3]);
  });

  // GROUP BY duplicate column fix
  it('GROUP BY with qualified names - no duplicate columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE p (id INT, name TEXT)');
    db.execute('CREATE TABLE c (pid INT, val INT)');
    db.execute("INSERT INTO p VALUES (1,'x'),(2,'y')");
    db.execute('INSERT INTO c VALUES (1,10),(1,20),(2,30)');
    
    const r = db.execute('SELECT p.name, SUM(c.val) as total FROM p JOIN c ON p.id = c.pid GROUP BY p.name');
    const keys = Object.keys(r.rows[0]);
    assert.ok(!keys.includes('p.name'), 'Should not have qualified column p.name');
    assert.ok(keys.includes('name'), 'Should have unqualified column name');
  });
});
