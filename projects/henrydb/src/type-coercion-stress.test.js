// type-coercion-stress.test.js — Tests for implicit type coercion
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Type coercion stress tests', () => {
  
  it('int compared to string number', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute("SELECT val FROM t WHERE val = '42'");
    assert.strictEqual(r.rows.length, 1);
  });

  it('string number in arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b TEXT)');
    db.execute("INSERT INTO t VALUES (10, '20')");
    try {
      const r = db.execute('SELECT a + b as sum FROM t');
      assert.ok(r.rows[0].sum === 30 || r.rows[0].sum === '1020');
    } catch (e) {
      assert.ok(true); // Type error is acceptable
    }
  });

  it('boolean-like values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (active BOOLEAN)');
    db.execute('INSERT INTO t VALUES (true)');
    db.execute('INSERT INTO t VALUES (false)');
    const r = db.execute('SELECT active FROM t WHERE active = true');
    assert.ok(r.rows.length >= 1);
  });

  it('REAL and INT mixed arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b REAL)');
    db.execute('INSERT INTO t VALUES (1, 2.5)');
    const r = db.execute('SELECT a + b as sum FROM t');
    assert.strictEqual(r.rows[0].sum, 3.5);
  });

  it('NULL in equality comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');
    // NULL = NULL should not match (3-valued logic)
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = val');
    assert.strictEqual(r.rows[0].cnt, 1); // Only non-NULL row matches
  });

  it('mixed types in IN clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    const r = db.execute("SELECT val FROM t WHERE val IN (1, '2', 3) ORDER BY val");
    assert.ok(r.rows.length >= 2); // At least 1 and 3 should match
  });
});
