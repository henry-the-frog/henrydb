// between-in-stress.test.js — Stress tests for BETWEEN, IN, NOT IN
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('BETWEEN/IN stress tests', () => {
  
  it('BETWEEN inclusive range', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val BETWEEN 3 AND 7 ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [3, 4, 5, 6, 7]);
  });

  it('NOT BETWEEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val NOT BETWEEN 3 AND 7 ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 8, 9, 10]);
  });

  it('BETWEEN with strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    ['apple', 'banana', 'cherry', 'date', 'elderberry'].forEach(n => db.execute(`INSERT INTO t VALUES ('${n}')`));
    const r = db.execute("SELECT name FROM t WHERE name BETWEEN 'b' AND 'd' ORDER BY name");
    assert.deepStrictEqual(r.rows.map(r => r.name), ['banana', 'cherry']); // 'date' > 'd' lexicographically
  });

  it('IN with integer list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val IN (2, 4, 6, 8) ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [2, 4, 6, 8]);
  });

  it('NOT IN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val NOT IN (2, 4) ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 3, 5]);
  });

  it('IN with single value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val IN (3)');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 3);
  });

  it('IN with no matching values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val IN (99, 100)');
    assert.strictEqual(r.rows.length, 0);
  });

  it('IN with subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (a_id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO a VALUES (${i})`);
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (4)');
    const r = db.execute('SELECT id FROM a WHERE id IN (SELECT a_id FROM b) ORDER BY id');
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 4]);
  });

  it('BETWEEN with dates (as strings)', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT, dt TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-01-01')");
    db.execute("INSERT INTO events VALUES (2, '2024-03-15')");
    db.execute("INSERT INTO events VALUES (3, '2024-06-30')");
    db.execute("INSERT INTO events VALUES (4, '2024-12-31')");
    const r = db.execute("SELECT id FROM events WHERE dt BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id");
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('BETWEEN combined with AND', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 5)");
    db.execute("INSERT INTO t VALUES (2, 'B', 15)");
    db.execute("INSERT INTO t VALUES (3, 'A', 25)");
    const r = db.execute("SELECT id FROM t WHERE cat = 'A' AND val BETWEEN 1 AND 10 ORDER BY id");
    assert.deepStrictEqual(r.rows.map(r => r.id), [1]);
  });

  it('IN combined with OR', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('SELECT val FROM t WHERE val IN (1, 2) OR val IN (9, 10) ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 9, 10]);
  });
});
