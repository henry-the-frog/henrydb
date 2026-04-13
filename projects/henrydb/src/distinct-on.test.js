// distinct-on.test.js — DISTINCT ON (PostgreSQL extension)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('DISTINCT ON', () => {
  it('keeps first row per distinct key', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (dept TEXT, name TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('sales', 'Carol', 90), ('sales', 'Dave', 95), ('hr', 'Eve', 80)");
    
    const r = db.execute('SELECT DISTINCT ON (dept) dept, name, salary FROM t ORDER BY dept, salary DESC');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].dept, 'eng');
    assert.equal(r.rows[0].name, 'Bob');  // highest salary in eng
    assert.equal(r.rows[1].dept, 'hr');
    assert.equal(r.rows[2].dept, 'sales');
    assert.equal(r.rows[2].name, 'Dave'); // highest salary in sales
  });

  it('works with multiple DISTINCT ON columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a TEXT, b TEXT, c INT)');
    db.execute("INSERT INTO t VALUES ('x', 'y', 1), ('x', 'y', 2), ('x', 'z', 3), ('w', 'y', 4)");
    
    const r = db.execute('SELECT DISTINCT ON (a, b) a, b, c FROM t ORDER BY a, b, c DESC');
    assert.equal(r.rows.length, 3);
    // (w,y,4), (x,y,2), (x,z,3)
    assert.deepEqual(r.rows.map(r => r.c), [4, 2, 3]);
  });

  it('respects LIMIT after DISTINCT ON', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5)");
    
    const r = db.execute('SELECT DISTINCT ON (cat) cat, val FROM t ORDER BY cat, val DESC LIMIT 2');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].cat, 'a');
    assert.equal(r.rows[1].cat, 'b');
  });

  it('plain DISTINCT still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (2), (3), (3), (3)');
    
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });
});
