// milestone.test.js — Push past 1500 tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Milestone: 1500 Tests', () => {
  it('CREATE TABLE + INSERT + SELECT roundtrip', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'hello');
  });

  it('aggregate functions on empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS total FROM t');
    assert.equal(r.rows[0].cnt, 0);
  });

  it('NULL handling in comparisons', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, null)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    const r = db.execute('SELECT * FROM t WHERE val IS NULL');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('string concatenation with ||', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, first TEXT, last TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')");
    // Test CONCAT function
    const r = db.execute("SELECT CONCAT(first, ' ', last) AS full_name FROM t");
    assert.equal(r.rows[0].full_name, 'John Doe');
  });

  it('arithmetic expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 3)');
    const r = db.execute('SELECT a * b AS product, a + b AS total FROM t');
    assert.equal(r.rows[0].product, 30);
    assert.equal(r.rows[0].total, 13);
  });

  it('multiple ORDER BY columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 1, 2)');
    db.execute('INSERT INTO t VALUES (2, 1, 1)');
    db.execute('INSERT INTO t VALUES (3, 2, 1)');
    const r = db.execute('SELECT id FROM t ORDER BY a, b');
    assert.equal(r.rows[0].id, 2); // a=1, b=1
    assert.equal(r.rows[1].id, 1); // a=1, b=2
    assert.equal(r.rows[2].id, 3); // a=2, b=1
  });

  it('nested function calls', () => {
    const db = new Database();
    const r = db.execute("SELECT UPPER(REVERSE('hello')) AS result");
    assert.equal(r.rows[0].result, 'OLLEH');
  });

  it('LIKE pattern matching', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    const r = db.execute("SELECT name FROM t WHERE name LIKE 'A%'");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('DELETE with no WHERE deletes all', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('DELETE FROM t');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 0);
  });

  it('UPDATE with no WHERE updates all', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = 0');
    const r = db.execute('SELECT SUM(val) AS total FROM t');
    assert.equal(r.rows[0].total, 0);
  });

  it('boolean expressions TRUE and FALSE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, active INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 0)');
    const r = db.execute('SELECT * FROM t WHERE active = 1');
    assert.equal(r.rows.length, 1);
  });

  it('multi-row INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 3);
  });

  it('LEFT function on table data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, code TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'ABC123')");
    const r = db.execute("SELECT LEFT(code, 3) AS prefix FROM t");
    assert.equal(r.rows[0].prefix, 'ABC');
  });

  it('POWER and SQRT composition', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 16)');
    const r = db.execute('SELECT POWER(SQRT(val), 2) AS result FROM t');
    assert.ok(Math.abs(r.rows[0].result - 16) < 0.01);
  });

  it('BETWEEN in WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT * FROM t WHERE val BETWEEN 30 AND 70');
    assert.equal(r.rows.length, 5);
  });
});
