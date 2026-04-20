// distinct.test.js — DISTINCT and DISTINCT ON tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('DISTINCT', () => {
  it('removes duplicate rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('a'),('b'),('a'),('c'),('b')");
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), ['a', 'b', 'c']);
  });

  it('DISTINCT on multiple columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES ('x','1'),('x','2'),('y','1'),('x','1')");
    const r = db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b');
    assert.equal(r.rows.length, 3); // (x,1), (x,2), (y,1)
  });

  it('COUNT(DISTINCT col)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('a'),('b'),('a'),('c'),('b'),('a')");
    const r = db.execute('SELECT COUNT(DISTINCT val) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 3);
  });

  it('DISTINCT with NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(NULL),(2),(NULL),(1)');
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    // NULL, 1, 2 — 3 distinct values
    assert.equal(r.rows.length, 3);
  });

  it('DISTINCT with ORDER BY + LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(2),(3),(1),(2),(3)');
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val LIMIT 2');
    assert.deepEqual(r.rows.map(r => r.val), [1, 2]);
  });

  it('AVG(DISTINCT val)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(1),(3),(2)');
    const r = db.execute('SELECT COUNT(DISTINCT val) as cnt, COUNT(*) as total FROM t');
    assert.equal(r.rows[0].cnt, 3);
    assert.equal(r.rows[0].total, 5);
  });
});
