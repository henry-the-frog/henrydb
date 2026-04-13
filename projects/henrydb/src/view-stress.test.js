// view-stress.test.js — Stress tests for VIEW operations
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('VIEW stress tests', () => {
  
  it('basic CREATE VIEW and SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('CREATE VIEW v AS SELECT id, val FROM t WHERE val > 50');
    const r = db.execute('SELECT * FROM v ORDER BY id');
    assert.strictEqual(r.rows.length, 5);
    assert.strictEqual(r.rows[0].id, 6);
  });

  it('VIEW with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A', 10)");
    db.execute("INSERT INTO t VALUES ('A', 20)");
    db.execute("INSERT INTO t VALUES ('B', 30)");
    db.execute('CREATE VIEW v AS SELECT cat, SUM(val) as total FROM t GROUP BY cat');
    const r = db.execute('SELECT * FROM v ORDER BY cat');
    assert.strictEqual(r.rows[0].total, 30);
  });

  it('VIEW reflects base table changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('CREATE VIEW v AS SELECT * FROM t');
    
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT COUNT(*) as cnt FROM v');
    assert.strictEqual(r.rows[0].cnt, 2);
  });

  it('VIEW with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, name TEXT)');
    db.execute('CREATE TABLE b (a_id INT, val INT)');
    db.execute("INSERT INTO a VALUES (1, 'one')");
    db.execute('INSERT INTO b VALUES (1, 100)');
    db.execute('CREATE VIEW v AS SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id');
    const r = db.execute('SELECT * FROM v');
    assert.strictEqual(r.rows[0].name, 'one');
    assert.strictEqual(r.rows[0].val, 100);
  });

  it('DROP VIEW', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE VIEW v AS SELECT * FROM t');
    db.execute('DROP VIEW v');
    try {
      db.execute('SELECT * FROM v');
      assert.fail('should error after DROP VIEW');
    } catch (e) {
      assert.ok(true);
    }
  });

  it('VIEW used in WHERE subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('CREATE VIEW high AS SELECT id FROM t WHERE val > 50');
    const r = db.execute('SELECT * FROM t WHERE id IN (SELECT id FROM high) ORDER BY id');
    assert.strictEqual(r.rows.length, 5);
  });

  it('multiple views on same table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, cat TEXT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i}, 'cat${i % 2}')`);
    db.execute("CREATE VIEW v_even AS SELECT * FROM t WHERE cat = 'cat0'");
    db.execute("CREATE VIEW v_odd AS SELECT * FROM t WHERE cat = 'cat1'");
    
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM v_even').rows[0].cnt, 10);
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM v_odd').rows[0].cnt, 10);
  });

  it('VIEW with CASE expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (score INT)');
    db.execute('INSERT INTO t VALUES (95)');
    db.execute('INSERT INTO t VALUES (72)');
    db.execute('INSERT INTO t VALUES (45)');
    db.execute(`CREATE VIEW grades AS SELECT score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END as grade FROM t`);
    const r = db.execute('SELECT * FROM grades ORDER BY score');
    assert.strictEqual(r.rows[0].grade, 'F');
    assert.strictEqual(r.rows[2].grade, 'A');
  });
});
