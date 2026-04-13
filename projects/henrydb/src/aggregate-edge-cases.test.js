// aggregate-edge-cases.test.js — Edge cases for aggregate functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Aggregate edge cases', () => {
  
  it('COUNT(*) vs COUNT(col)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (3)');
    const r = db.execute('SELECT COUNT(*) as all_rows, COUNT(val) as non_null FROM t');
    assert.strictEqual(r.rows[0].all_rows, 3);
    assert.strictEqual(r.rows[0].non_null, 2);
  });

  it('SUM of empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    const r = db.execute('SELECT SUM(val) as total FROM t');
    assert.strictEqual(r.rows[0].total, null);
  });

  it('AVG of single row', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT AVG(val) as avg FROM t');
    assert.strictEqual(r.rows[0].avg, 42);
  });

  it('MIN/MAX of single value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (7)');
    const r = db.execute('SELECT MIN(val) as mn, MAX(val) as mx FROM t');
    assert.strictEqual(r.rows[0].mn, 7);
    assert.strictEqual(r.rows[0].mx, 7);
  });

  it('MIN/MAX of strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    ['cherry', 'apple', 'banana'].forEach(n => db.execute(`INSERT INTO t VALUES ('${n}')`));
    const r = db.execute('SELECT MIN(name) as mn, MAX(name) as mx FROM t');
    assert.strictEqual(r.rows[0].mn, 'apple');
    assert.strictEqual(r.rows[0].mx, 'cherry');
  });

  it('aggregate with WHERE returning no rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(val) as total FROM t WHERE val > 999');
    assert.strictEqual(r.rows[0].cnt, 0);
    assert.strictEqual(r.rows[0].total, null);
  });

  it('multiple aggregates on same column', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [1, 2, 3, 4, 5].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT COUNT(val) as cnt, SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM t');
    assert.strictEqual(r.rows[0].cnt, 5);
    assert.strictEqual(r.rows[0].s, 15);
    assert.strictEqual(r.rows[0].a, 3);
    assert.strictEqual(r.rows[0].mn, 1);
    assert.strictEqual(r.rows[0].mx, 5);
  });

  it('GROUP BY with single row per group', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT id, SUM(val) as total FROM t GROUP BY id ORDER BY id');
    assert.strictEqual(r.rows.length, 5);
    assert.strictEqual(r.rows[0].total, 10);
  });

  it('aggregate with all NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (NULL)');
    const r = db.execute('SELECT COUNT(*) as cnt, COUNT(val) as cnt_val, SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM t');
    assert.strictEqual(r.rows[0].cnt, 2);
    assert.strictEqual(r.rows[0].cnt_val, 0);
    assert.strictEqual(r.rows[0].s, null);
  });
});
