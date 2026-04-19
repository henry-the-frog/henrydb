import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('INSERT SELECT column mapping (2026-04-19)', () => {
  it('maps columns by position with explicit column list', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (a INT, b TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'hello')");
    db.execute('CREATE TABLE dst (x INT, y TEXT)');
    db.execute('INSERT INTO dst (x, y) SELECT a, b FROM src');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows[0].x, 1);
    assert.equal(r.rows[0].y, 'hello');
  });

  it('handles column reordering', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (a INT, b TEXT, c FLOAT)');
    db.execute("INSERT INTO src VALUES (1, 'hi', 3.14)");
    db.execute('CREATE TABLE dst (c_col FLOAT, a_col INT, b_col TEXT)');
    db.execute('INSERT INTO dst (a_col, b_col, c_col) SELECT a, b, c FROM src');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows[0].a_col, 1);
    assert.equal(r.rows[0].b_col, 'hi');
    assert.equal(r.rows[0].c_col, 3.14);
  });

  it('handles partial column list with defaults', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (a INT, b TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'test')");
    db.execute('CREATE TABLE dst (id INT, name TEXT, score INT DEFAULT 0)');
    db.execute('INSERT INTO dst (id, name) SELECT a, b FROM src');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'test');
    assert.equal(r.rows[0].score, 0);
  });

  it('handles multiple rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (a INT, b TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    db.execute('CREATE TABLE dst (x INT, y TEXT)');
    db.execute('INSERT INTO dst (x, y) SELECT a, b FROM src');
    const r = db.execute('SELECT * FROM dst ORDER BY x');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].x, 1);
    assert.equal(r.rows[2].y, 'c');
  });
});

describe('ALTER TABLE ADD COLUMN NOT NULL DEFAULT backfill (2026-04-19)', () => {
  it('backfills existing rows with NOT NULL DEFAULT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute("ALTER TABLE t ADD COLUMN status TEXT NOT NULL DEFAULT 'active'");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].status, 'active');
    assert.equal(r.rows[1].status, 'active');
  });

  it('backfills with DEFAULT only (no NOT NULL)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN score INT DEFAULT 100');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].score, 100);
  });

  it('multiple ALTER ADD COLUMN operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute("ALTER TABLE t ADD COLUMN name TEXT DEFAULT 'unknown'");
    db.execute('ALTER TABLE t ADD COLUMN age INT DEFAULT 0');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'unknown');
    assert.equal(r.rows[0].age, 0);
  });

  it('new rows after ALTER get the default', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute("ALTER TABLE t ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'");
    db.execute('INSERT INTO t VALUES (2)');  // should get status='pending'
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].status, 'pending');
    assert.equal(r.rows[1].status, 'pending');
  });

  it('NOT NULL without DEFAULT adds column with null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    // NOT NULL without DEFAULT — existing rows get null (PostgreSQL behavior)
    db.execute('ALTER TABLE t ADD COLUMN val INT');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].val, null);
  });
});
