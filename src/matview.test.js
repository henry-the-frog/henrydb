// matview.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Materialized Views', () => {
  it('creates and queries materialized view', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");

    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp');
    const r = db.execute('SELECT * FROM mv ORDER BY grp');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows.find(r => r.grp === 'A').total, 30);
  });

  it('REFRESH updates with new data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT SUM(val) AS total FROM t');
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 10);

    // Add data and refresh
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('REFRESH MATERIALIZED VIEW mv');
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 30);
  });

  it('materialized view is queryable with WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, cat TEXT, price INT)');
    db.execute("INSERT INTO products VALUES (1, 'A', 100)");
    db.execute("INSERT INTO products VALUES (2, 'B', 200)");
    db.execute("INSERT INTO products VALUES (3, 'A', 300)");

    db.execute('CREATE MATERIALIZED VIEW cat_stats AS SELECT cat, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY cat');
    const r = db.execute("SELECT * FROM cat_stats WHERE cnt > 1");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].cat, 'A');
  });
});
