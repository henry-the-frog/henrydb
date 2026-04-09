// returning.test.js — Tests for RETURNING clause
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RETURNING clause', () => {
  it('INSERT RETURNING *', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'hello', 42) RETURNING *");
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].name, 'hello');
    assert.strictEqual(r.rows[0].val, 42);
  });

  it('INSERT RETURNING specific columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'test', 99) RETURNING id, val");
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].val, 99);
    assert.strictEqual(r.rows[0].name, undefined);
  });

  it('UPDATE RETURNING *', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    const r = db.execute('UPDATE t SET val = val + 50 WHERE id = 1 RETURNING *');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].val, 150);
  });

  it('UPDATE RETURNING multiple rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('UPDATE t SET val = 999 WHERE id > 3 RETURNING id, val');
    assert.strictEqual(r.rows.length, 2);
    assert.ok(r.rows.every(row => row.val === 999));
  });

  it('DELETE RETURNING *', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'keep')");
    db.execute("INSERT INTO t VALUES (2, 'delete_me')");
    db.execute("INSERT INTO t VALUES (3, 'delete_me')");
    const r = db.execute("DELETE FROM t WHERE name = 'delete_me' RETURNING *");
    assert.strictEqual(r.rows.length, 2);
    assert.ok(r.rows.every(row => row.name === 'delete_me'));
  });

  it('DELETE RETURNING specific columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    const r = db.execute('DELETE FROM t WHERE id = 1 RETURNING id');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].val, undefined);
  });

  it('INSERT RETURNING with ON CONFLICT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    const r = db.execute('INSERT INTO t VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = 200 RETURNING *');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 200);
  });
});
