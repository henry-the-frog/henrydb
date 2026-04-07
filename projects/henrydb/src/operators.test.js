// operators.test.js — Operator tests and DML edge cases
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Comparison Operators', () => {
  it('<> operator (not equal)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE val <> 20');
    assert.equal(r.rows.length, 2);
  });

  it('!= operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT * FROM t WHERE val != 10');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 20);
  });

  it('> operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE val > 15');
    assert.equal(r.rows.length, 2);
  });

  it('>= operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT * FROM t WHERE val >= 20');
    assert.equal(r.rows.length, 1);
  });

  it('< operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT * FROM t WHERE val < 15');
    assert.equal(r.rows.length, 1);
  });

  it('<= operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT * FROM t WHERE val <= 10');
    assert.equal(r.rows.length, 1);
  });

  it('string comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    const r = db.execute("SELECT * FROM t WHERE name > 'B'");
    assert.ok(r.rows.length >= 2); // Bob and Charlie
  });
});

describe('DML edge cases', () => {
  it('UPDATE with no matching WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = 99 WHERE id = 999');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].val, 10); // unchanged
  });

  it('DELETE with no matching WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('DELETE FROM t WHERE id = 999');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1); // unchanged
  });

  it('INSERT into non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('INSERT INTO ghost VALUES (1)'), /not found/);
  });

  it('SELECT from non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('SELECT * FROM ghost'), /not found/);
  });

  it('UPDATE non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('UPDATE ghost SET val = 1'), /not found/);
  });

  it('DELETE from non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('DELETE FROM ghost'), /not found/);
  });

  it('DROP non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('DROP TABLE ghost'), /not found/);
  });

  it('CREATE duplicate table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    assert.throws(() => db.execute('CREATE TABLE t (id INT PRIMARY KEY)'), /exists/);
  });
});
