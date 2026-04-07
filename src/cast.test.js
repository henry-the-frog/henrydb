// cast.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CAST Expressions', () => {
  it('CAST text to INT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '42')");
    const r = db.execute('SELECT CAST(val AS INT) AS num FROM t');
    assert.equal(r.rows[0].num, 42);
    assert.equal(typeof r.rows[0].num, 'number');
  });

  it('CAST INT to TEXT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT CAST(val AS TEXT) AS str FROM t');
    assert.equal(r.rows[0].str, '42');
    assert.equal(typeof r.rows[0].str, 'string');
  });

  it('CAST to FLOAT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '3.14')");
    const r = db.execute('SELECT CAST(val AS FLOAT) AS num FROM t');
    assert.ok(Math.abs(r.rows[0].num - 3.14) < 0.001);
  });

  it('CAST NULL returns NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT CAST(val AS INT) AS num FROM t');
    assert.equal(r.rows[0].num, null);
  });
});
