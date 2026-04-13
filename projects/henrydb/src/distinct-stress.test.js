// distinct-stress.test.js — Stress tests for DISTINCT
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('DISTINCT stress tests', () => {
  
  it('basic DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [1, 2, 2, 3, 3, 3].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('DISTINCT multiple columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (1, 2)');
    db.execute('INSERT INTO t VALUES (2, 1)');
    const r = db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b');
    assert.strictEqual(r.rows.length, 3);
  });

  it('DISTINCT with NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.strictEqual(r.rows.length, 2); // 1 and NULL
  });

  it('DISTINCT with ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [5, 3, 1, 3, 5, 2, 1].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val DESC');
    assert.deepStrictEqual(r.rows.map(r => r.val), [5, 3, 2, 1]);
  });

  it('COUNT(DISTINCT)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [1, 1, 2, 2, 3, 3, 3].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT COUNT(DISTINCT val) as cnt FROM t');
    assert.strictEqual(r.rows[0].cnt, 3);
  });

  it('DISTINCT strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    ['a', 'b', 'a', 'c', 'b'].forEach(n => db.execute(`INSERT INTO t VALUES ('${n}')`));
    const r = db.execute('SELECT DISTINCT name FROM t ORDER BY name');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['a', 'b', 'c']);
  });

  it('DISTINCT with LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i % 10})`);
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val LIMIT 5');
    assert.strictEqual(r.rows.length, 5);
    assert.deepStrictEqual(r.rows.map(r => r.val), [0, 1, 2, 3, 4]);
  });

  it('DISTINCT on all-same values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 0; i < 100; i++) db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT DISTINCT val FROM t');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 42);
  });
});
