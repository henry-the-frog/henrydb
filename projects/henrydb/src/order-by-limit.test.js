// order-by-limit.test.js — ORDER BY and LIMIT/OFFSET tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ORDER BY and LIMIT', () => {
  it('ORDER BY ASC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(2)');
    const r = db.execute('SELECT val FROM t ORDER BY val ASC');
    assert.deepEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('ORDER BY DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(2)');
    const r = db.execute('SELECT val FROM t ORDER BY val DESC');
    assert.deepEqual(r.rows.map(r => r.val), [3, 2, 1]);
  });

  it('ORDER BY multiple columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1,3),(1,1),(2,2),(1,2)');
    const r = db.execute('SELECT a, b FROM t ORDER BY a ASC, b DESC');
    assert.equal(r.rows[0].a, 1); assert.equal(r.rows[0].b, 3);
    assert.equal(r.rows[1].a, 1); assert.equal(r.rows[1].b, 2);
    assert.equal(r.rows[2].a, 1); assert.equal(r.rows[2].b, 1);
    assert.equal(r.rows[3].a, 2);
  });

  it('LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT val FROM t ORDER BY val LIMIT 3');
    assert.deepEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT val FROM t ORDER BY val OFFSET 2');
    assert.deepEqual(r.rows.map(r => r.val), [3, 4, 5]);
  });

  it('LIMIT + OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT val FROM t ORDER BY val LIMIT 2 OFFSET 2');
    assert.deepEqual(r.rows.map(r => r.val), [3, 4]);
  });

  it('NULLS FIRST', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)');
    const r = db.execute('SELECT val FROM t ORDER BY val NULLS FIRST');
    assert.equal(r.rows[0].val, null);
    assert.equal(r.rows[1].val, null);
    assert.equal(r.rows[2].val, 1);
  });

  it('NULLS LAST', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)');
    const r = db.execute('SELECT val FROM t ORDER BY val NULLS LAST');
    assert.ok(r.rows[3].val === null || r.rows[4].val === null);
  });

  it('ORDER BY expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES ('alice',90),('bob',85),('charlie',95)");
    const r = db.execute('SELECT name FROM t ORDER BY score DESC');
    assert.deepEqual(r.rows.map(r => r.name), ['charlie', 'alice', 'bob']);
  });

  it('ORDER BY ordinal position', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (3,1),(1,3),(2,2)');
    const r = db.execute('SELECT a, b FROM t ORDER BY 2 DESC');
    assert.equal(r.rows[0].b, 3);
    assert.equal(r.rows[2].b, 1);
  });
});
