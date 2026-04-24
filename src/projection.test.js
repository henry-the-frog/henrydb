// projection.test.js — Column projection, aliases, table.column references
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Column Projection', () => {
  it('SELECT * returns all columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c INT)');
    db.execute("INSERT INTO t VALUES (1, 10, 'hello', 30)");
    const r = db.execute('SELECT * FROM t');
    assert.ok(r.rows[0].id !== undefined);
    assert.ok(r.rows[0].a !== undefined);
    assert.ok(r.rows[0].b !== undefined);
    assert.ok(r.rows[0].c !== undefined);
  });

  it('SELECT specific columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    const r = db.execute('SELECT a, b FROM t');
    assert.equal(r.rows[0].a, 10);
    assert.equal(r.rows[0].b, 20);
    assert.equal(r.rows[0].id, undefined);
  });

  it('column aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT id AS identifier, val AS value FROM t');
    assert.equal(r.rows[0].identifier, 1);
    assert.equal(r.rows[0].value, 42);
  });

  it('duplicate column names via aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT val AS x, val AS y FROM t');
    assert.equal(r.rows[0].x, 42);
    assert.equal(r.rows[0].y, 42);
  });
});

describe('Query with empty results', () => {
  it('WHERE matches nothing', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('SELECT * FROM t WHERE val = 999');
    assert.equal(r.rows.length, 0);
  });

  it('empty table COUNT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.equal(r.rows[0].cnt, 0);
  });

  it('empty table SUM', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT SUM(val) AS total FROM t');
    assert.equal(r.rows[0].total, null);
  });

  it('ORDER BY on empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT * FROM t ORDER BY val DESC');
    assert.equal(r.rows.length, 0);
  });
});

describe('Multi-table column references', () => {
  it('table.column in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, label TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO b VALUES (1, 1, 'y')");
    const r = db.execute('SELECT a.val, b.label FROM a JOIN b ON a.id = b.a_id');
    assert.ok(r.rows[0].val !== undefined || r.rows[0]['a.val'] !== undefined);
  });

  it('table.column in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)');
    db.execute('INSERT INTO a VALUES (1, 100)');
    db.execute('INSERT INTO b VALUES (1, 1, 200)');
    const r = db.execute('SELECT * FROM a JOIN b ON a.id = b.a_id WHERE a.val = 100');
    assert.equal(r.rows.length, 1);
  });
});

describe('Comprehensive data types', () => {
  it('stores and retrieves integers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 42);
    assert.equal(typeof r.rows[0].val, 'number');
  });

  it('stores and retrieves text', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello world')");
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 'hello world');
  });

  it('stores and retrieves boolean', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN)');
    db.execute('INSERT INTO t VALUES (1, TRUE)');
    db.execute('INSERT INTO t VALUES (2, FALSE)');
    const r1 = db.execute('SELECT * FROM t WHERE active = TRUE');
    assert.equal(r1.rows.length, 1);
  });

  it('stores and retrieves NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, null);
  });

  it('large integer values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 2147483647)');
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 2147483647);
  });
});
