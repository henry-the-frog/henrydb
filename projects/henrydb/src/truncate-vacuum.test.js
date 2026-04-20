// truncate-vacuum.test.js — TRUNCATE and VACUUM tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('TRUNCATE TABLE', () => {
  it('removes all rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 100);
    db.execute('TRUNCATE TABLE t');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 0);
  });

  it('table still usable after TRUNCATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'before')");
    db.execute('TRUNCATE TABLE t');
    db.execute("INSERT INTO t VALUES (2, 'after')");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'after');
  });
});

describe('VACUUM', () => {
  it('VACUUM runs without error', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute('VACUUM'); // Should not throw
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
  });

  it('VACUUM specific table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (1)');
    db.execute('DELETE FROM t1 WHERE id = 1');
    db.execute('VACUUM t1');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t2').rows[0].c, 1);
  });

  it('ANALYZE collects statistics', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('ANALYZE t'); // Should not throw
    // After ANALYZE, estimates should be more accurate
    const explain = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 50');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('actual=50'));
  });
});
