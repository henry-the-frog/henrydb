// matview.test.js — Materialized Views
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Materialized Views', () => {
  it('creates and queries materialized view', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a', 10), ('b', 20), ('a', 30)");
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT cat, SUM(val) AS total FROM t GROUP BY cat');
    const r = db.execute('SELECT * FROM mv ORDER BY cat');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].total, 40); // a: 10+30
    assert.equal(r.rows[1].total, 20); // b: 20
  });

  it('does not change until refresh', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT SUM(val) AS total FROM t');
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 6);
    
    db.execute('INSERT INTO t VALUES (4)');
    // Still shows old data
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 6);
  });

  it('refresh updates materialized view', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT SUM(val) AS total FROM t');
    db.execute('INSERT INTO t VALUES (4)');
    db.execute('REFRESH MATERIALIZED VIEW mv');
    
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 10);
  });

  it('materialized view with complex query', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (product TEXT, qty INT, price INT)');
    db.execute("INSERT INTO orders VALUES ('A', 2, 10), ('B', 1, 20), ('A', 3, 10)");
    
    db.execute('CREATE MATERIALIZED VIEW revenue AS SELECT product, SUM(qty * price) AS total FROM orders GROUP BY product');
    const r = db.execute('SELECT * FROM revenue ORDER BY product');
    assert.equal(r.rows[0].total, 50); // A: 2*10 + 3*10
    assert.equal(r.rows[1].total, 20); // B: 1*20
  });
});
