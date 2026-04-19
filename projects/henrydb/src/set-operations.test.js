// set-operations.test.js — UNION, INTERSECT, EXCEPT tests

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UNION', () => {
  it('UNION removes duplicates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'alpha'), (2, 'beta')");
    db.execute("INSERT INTO t2 VALUES (3, 'gamma'), (4, 'alpha')"); // alpha is duplicate
    
    const r = db.execute('SELECT name FROM t1 UNION SELECT name FROM t2');
    assert.equal(r.rows.length, 3); // alpha, beta, gamma
  });

  it('UNION ALL keeps duplicates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'alpha'), (2, 'beta')");
    db.execute("INSERT INTO t2 VALUES (3, 'gamma'), (4, 'alpha')");
    
    const r = db.execute('SELECT name FROM t1 UNION ALL SELECT name FROM t2');
    assert.equal(r.rows.length, 4); // alpha, beta, gamma, alpha
  });

  it('UNION with ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (3), (1)');
    db.execute('INSERT INTO t2 VALUES (4), (2)');
    
    const r = db.execute('SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3, 4]);
  });

  it('UNION with LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2), (3)');
    db.execute('INSERT INTO t2 VALUES (4), (5), (6)');
    
    const r = db.execute('SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('UNION with different column aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (a INT)');
    db.execute('CREATE TABLE t2 (b INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2)');
    db.execute('INSERT INTO t2 VALUES (3), (4)');
    
    const r = db.execute('SELECT a as val FROM t1 UNION SELECT b as val FROM t2 ORDER BY val');
    assert.equal(r.rows.length, 4);
  });

  it('three-way UNION', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val TEXT)');
    db.execute('CREATE TABLE t2 (val TEXT)');
    db.execute('CREATE TABLE t3 (val TEXT)');
    db.execute("INSERT INTO t1 VALUES ('a')");
    db.execute("INSERT INTO t2 VALUES ('b')");
    db.execute("INSERT INTO t3 VALUES ('c')");
    
    const r = db.execute("SELECT val FROM t1 UNION SELECT val FROM t2 UNION SELECT val FROM t3 ORDER BY val");
    assert.deepStrictEqual(r.rows.map(r => r.val), ['a', 'b', 'c']);
  });
});

describe('INTERSECT', () => {
  it('returns common rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2), (3)');
    db.execute('INSERT INTO t2 VALUES (2), (3), (4)');
    
    const r = db.execute('SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [2, 3]);
  });

  it('empty INTERSECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2)');
    db.execute('INSERT INTO t2 VALUES (3), (4)');
    
    const r = db.execute('SELECT val FROM t1 INTERSECT SELECT val FROM t2');
    assert.equal(r.rows.length, 0);
  });
});

describe('EXCEPT', () => {
  it('returns rows in first but not second', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2), (3)');
    db.execute('INSERT INTO t2 VALUES (2), (3), (4)');
    
    const r = db.execute('SELECT val FROM t1 EXCEPT SELECT val FROM t2');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 1);
  });

  it('EXCEPT with empty second set', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2), (3)');
    
    const r = db.execute('SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('EXCEPT reversal', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2), (3)');
    db.execute('INSERT INTO t2 VALUES (2), (3), (4)');
    
    // Reversed: rows in t2 but not t1
    const r = db.execute('SELECT val FROM t2 EXCEPT SELECT val FROM t1');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 4);
  });
});

describe('Set Operations — Edge Cases', () => {
  it('UNION with NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (NULL)');
    db.execute('INSERT INTO t2 VALUES (2), (NULL)');
    
    const r = db.execute('SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val');
    assert.equal(r.rows.length, 4);
  });

  it('UNION with expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20)');
    
    const r = db.execute('SELECT val * 2 as result FROM t UNION SELECT val + 100 as result FROM t ORDER BY result');
    assert.ok(r.rows.length > 0);
  });

  it('UNION in subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (1), (2)');
    db.execute('INSERT INTO t2 VALUES (3), (4)');
    
    const r = db.execute('SELECT * FROM (SELECT val FROM t1 UNION SELECT val FROM t2) sub ORDER BY val');
    assert.equal(r.rows.length, 4);
  });
});
