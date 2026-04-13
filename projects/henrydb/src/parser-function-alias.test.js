// parser-function-alias.test.js — Test that SQL function names can be used as column aliases
// Regression test for: parser treating 'mod', 'length', etc. as function calls in ORDER BY
// even when not followed by '(' (they should be treated as identifier references).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Parser: Function names as identifiers', () => {
  it('mod alias in ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, num INT)');
    for (let i = 0; i < 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT DISTINCT num % 10 as mod FROM t ORDER BY mod');
    assert.strictEqual(r.rows[0].mod, 0);
  });

  it('length alias in ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hi')");
    db.execute("INSERT INTO t VALUES (2, 'hello')");
    const r = db.execute('SELECT name, LENGTH(name) as length FROM t ORDER BY length');
    assert.strictEqual(r.rows[0].length, 2);
    assert.strictEqual(r.rows[1].length, 5);
  });

  it('upper/lower as aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    const r = db.execute('SELECT id as upper, id as lower FROM t ORDER BY upper');
    assert.strictEqual(r.rows[0].upper, 1);
  });

  it('round as alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('SELECT val as round FROM t ORDER BY round');
    assert.strictEqual(r.rows[0].round, 10);
  });

  it('function calls still work', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, num INT)');
    db.execute("INSERT INTO t VALUES (1, 'hello', 17)");
    assert.strictEqual(db.execute('SELECT LENGTH(name) as l FROM t').rows[0].l, 5);
    assert.strictEqual(db.execute('SELECT UPPER(name) as u FROM t').rows[0].u, 'HELLO');
    assert.strictEqual(db.execute('SELECT MOD(num, 5) as m FROM t').rows[0].m, 2);
    assert.strictEqual(db.execute('SELECT ROUND(num) as r FROM t').rows[0].r, 17);
  });

  it('function in ORDER BY expression still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'abc')");
    db.execute("INSERT INTO t VALUES (2, 'ab')");
    const r = db.execute('SELECT * FROM t ORDER BY LENGTH(name)');
    assert.strictEqual(r.rows[0].id, 2); // shorter first
  });

  it('mixed function calls and aliases in same query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, num INT)');
    db.execute('INSERT INTO t VALUES (1, 7)');
    db.execute('INSERT INTO t VALUES (2, 13)');
    const r = db.execute('SELECT MOD(num, 5) as mod, ABS(num - 10) as abs FROM t ORDER BY mod');
    assert.strictEqual(r.rows[0].mod, 2); // 7%5=2
    assert.strictEqual(r.rows[0].abs, 3); // |7-10|=3
    assert.strictEqual(r.rows[1].mod, 3); // 13%5=3
  });
});
