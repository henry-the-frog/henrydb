import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UNION/INTERSECT/EXCEPT with ORDER BY and LIMIT', () => {
  it('UNION ALL with ORDER BY and LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')");

    const r = db.execute(`
      SELECT * FROM t WHERE id <= 3
      UNION ALL
      SELECT * FROM t WHERE id > 3
      ORDER BY id
      LIMIT 3
    `);
    assert.equal(r.rows.length, 3);
    assert.deepEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('UNION with ORDER BY and LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');

    const r = db.execute(`
      SELECT id FROM t WHERE id <= 3
      UNION
      SELECT id FROM t WHERE id > 2
      ORDER BY id
      LIMIT 3
    `);
    assert.equal(r.rows.length, 3);
    assert.deepEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('UNION ALL with ORDER BY DESC and OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');

    const r = db.execute(`
      SELECT * FROM t WHERE id <= 3
      UNION ALL
      SELECT * FROM t WHERE id > 3
      ORDER BY id DESC
      LIMIT 2
      OFFSET 1
    `);
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(r => r.id), [4, 3]);
  });

  it('INTERSECT with ORDER BY and LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');

    const r = db.execute(`
      SELECT id FROM t WHERE id <= 4
      INTERSECT
      SELECT id FROM t WHERE id >= 2
      ORDER BY id
      LIMIT 2
    `);
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(r => r.id), [2, 3]);
  });

  it('EXCEPT with ORDER BY and LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');

    const r = db.execute(`
      SELECT id FROM t
      EXCEPT
      SELECT id FROM t WHERE id > 3
      ORDER BY id DESC
      LIMIT 2
    `);
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(r => r.id), [3, 2]);
  });

  it('UNION ALL without ORDER BY but with LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');

    const r = db.execute(`
      SELECT id FROM t
      UNION ALL
      SELECT id FROM t
      LIMIT 4
    `);
    assert.equal(r.rows.length, 4);
  });
});
