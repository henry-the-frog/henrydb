// returning-limit-offset.test.js — Tests for RETURNING expressions and LIMIT/OFFSET expressions

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RETURNING with expressions', () => {
  it('UPDATE RETURNING with expression and alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('UPDATE t SET val = val + 5 WHERE id = 1 RETURNING id, val, val * 2 as doubled');
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].val, 15);
    assert.strictEqual(r.rows[0].doubled, 30);
  });

  it('INSERT RETURNING with expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('INSERT INTO t VALUES (1, 10) RETURNING id, val + 1 as inc');
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].inc, 11);
  });

  it('DELETE RETURNING *', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('DELETE FROM t WHERE id = 1 RETURNING *');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 10);
  });

  it('DELETE RETURNING with expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('DELETE FROM t WHERE id = 1 RETURNING id, val * 10 as big');
    assert.strictEqual(r.rows[0].big, 100);
  });

  it('RETURNING with multiple expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    const r = db.execute('UPDATE t SET a = a + 1 WHERE id = 1 RETURNING a + b as sum, a * b as product');
    assert.ok(r.rows[0].SUM === 31 || r.rows[0].sum === 31, 'sum should be 31');
    assert.strictEqual(r.rows[0].product, 220);  // 11 * 20
  });

  it('RETURNING with string concatenation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, fname TEXT, lname TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')");
    const r = db.execute("UPDATE t SET fname = 'Jane' WHERE id = 1 RETURNING fname || ' ' || lname as fullname");
    assert.ok(r.rows[0].fullname || r.rows[0].FULLNAME, 'Should have fullname');
    assert.strictEqual(r.rows[0].fullname || r.rows[0].FULLNAME, 'Jane Doe');
  });
});

describe('LIMIT/OFFSET with expressions', () => {
  let db;

  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
  });

  it('LIMIT with addition', () => {
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 2 + 1');
    assert.strictEqual(r.rows.length, 3);
  });

  it('LIMIT with multiplication', () => {
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 2 * 3');
    assert.strictEqual(r.rows.length, 6);
  });

  it('OFFSET with addition', () => {
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 1 + 1');
    assert.strictEqual(r.rows[0].id, 3); // Skip 2 rows
  });

  it('OFFSET with subtraction', () => {
    const r = db.execute('SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 5 - 2');
    assert.strictEqual(r.rows[0].id, 4); // Skip 3 rows
  });

  it('simple LIMIT still works', () => {
    assert.strictEqual(db.execute('SELECT * FROM t LIMIT 5').rows.length, 5);
  });

  it('LIMIT 0 returns empty', () => {
    assert.strictEqual(db.execute('SELECT * FROM t LIMIT 0').rows.length, 0);
  });
});
