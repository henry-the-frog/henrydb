// generated-columns.test.js — Computed/Generated Columns
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Generated Columns', () => {
  it('auto-computes on INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)');
    db.execute('INSERT INTO t (a, b) VALUES (3, 4)');
    db.execute('INSERT INTO t (a, b) VALUES (10, 20)');
    
    const r = db.execute('SELECT * FROM t ORDER BY a');
    assert.equal(r.rows[0].c, 7);
    assert.equal(r.rows[1].c, 30);
  });

  it('recomputes on UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a * b) STORED)');
    db.execute('INSERT INTO t (a, b) VALUES (3, 4)');
    assert.equal(db.execute('SELECT c FROM t').rows[0].c, 12);
    
    db.execute('UPDATE t SET a = 5 WHERE a = 3');
    assert.equal(db.execute('SELECT c FROM t').rows[0].c, 20);
  });

  it('works with string concatenation', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)");
    db.execute("INSERT INTO t (first_name, last_name) VALUES ('Alice', 'Smith')");
    
    const r = db.execute('SELECT full_name FROM t');
    assert.equal(r.rows[0].full_name, 'Alice Smith');
  });

  it('multiple generated columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (price INT, qty INT, subtotal INT GENERATED ALWAYS AS (price * qty) STORED, tax INT GENERATED ALWAYS AS (price * qty / 10) STORED)');
    db.execute('INSERT INTO t (price, qty) VALUES (100, 5)');
    
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].subtotal, 500);
    assert.equal(r.rows[0].tax, 50);
  });
});
