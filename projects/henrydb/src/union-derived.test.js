// union-derived.test.js — Test UNION/INTERSECT/EXCEPT in derived table subqueries
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UNION in derived table subqueries', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT, val TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'a')");
    db.execute("INSERT INTO t1 VALUES (2, 'b')");
    db.execute("INSERT INTO t2 VALUES (2, 'b')");
    db.execute("INSERT INTO t2 VALUES (3, 'c')");
    return db;
  }

  it('UNION in derived table', () => {
    const db = makeDb();
    const r = db.execute('SELECT id, val FROM (SELECT id, val FROM t1 UNION SELECT id, val FROM t2) sub ORDER BY id');
    assert.strictEqual(r.rows.length, 3); // 1,2,3 (deduped)
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('UNION ALL in derived table', () => {
    const db = makeDb();
    const r = db.execute('SELECT id FROM (SELECT id FROM t1 UNION ALL SELECT id FROM t2) sub ORDER BY id');
    assert.strictEqual(r.rows.length, 4); // 1,2,2,3
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 2, 3]);
  });

  it('INTERSECT in derived table', () => {
    const db = makeDb();
    const r = db.execute('SELECT id FROM (SELECT id FROM t1 INTERSECT SELECT id FROM t2) sub');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 2);
  });

  it('EXCEPT in derived table', () => {
    const db = makeDb();
    const r = db.execute('SELECT id FROM (SELECT id FROM t1 EXCEPT SELECT id FROM t2) sub');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
  });

  it('UNION derived table with WHERE on outer query', () => {
    const db = makeDb();
    const r = db.execute('SELECT id, val FROM (SELECT id, val FROM t1 UNION SELECT id, val FROM t2) sub WHERE id >= 2 ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3]);
  });

  it('UNION derived table with aggregation on outer query', () => {
    const db = makeDb();
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(id) as total FROM (SELECT id FROM t1 UNION ALL SELECT id FROM t2) sub');
    assert.strictEqual(r.rows[0].cnt, 4);
    assert.strictEqual(r.rows[0].total, 8); // 1+2+2+3
  });

  it('UNION derived table joined with another table', () => {
    const db = new Database();
    db.execute('CREATE TABLE names (id INT, name TEXT)');
    db.execute('CREATE TABLE nums1 (id INT)');
    db.execute('CREATE TABLE nums2 (id INT)');
    db.execute("INSERT INTO names VALUES (1, 'one')");
    db.execute("INSERT INTO names VALUES (2, 'two')");
    db.execute('INSERT INTO nums1 VALUES (1)');
    db.execute('INSERT INTO nums2 VALUES (2)');
    
    const r = db.execute(`
      SELECT n.name, sub.id
      FROM (SELECT id FROM nums1 UNION SELECT id FROM nums2) sub
      JOIN names n ON n.id = sub.id
      ORDER BY sub.id
    `);
    assert.strictEqual(r.rows.length, 2);
    assert.ok(r.rows[0].name === 'one' || r.rows[0]['n.name'] === 'one');
    assert.ok(r.rows[1].name === 'two' || r.rows[1]['n.name'] === 'two');
  });
});
