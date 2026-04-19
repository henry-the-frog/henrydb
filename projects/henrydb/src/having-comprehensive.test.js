import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('HAVING Clause Comprehensive Tests (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT, product TEXT, amount INT, discount INT, region TEXT)');
    db.execute("INSERT INTO orders VALUES (1,'Widget',100,NULL,'East')");
    db.execute("INSERT INTO orders VALUES (2,'Widget',200,10,'West')");
    db.execute("INSERT INTO orders VALUES (3,'Gadget',150,NULL,'East')");
    db.execute("INSERT INTO orders VALUES (4,'Gadget',300,20,'East')");
    db.execute("INSERT INTO orders VALUES (5,'Widget',50,NULL,'East')");
    db.execute("INSERT INTO orders VALUES (6,'Doohickey',80,5,'West')");
    return db;
  }

  it('simple HAVING with SUM', () => {
    setup();
    const r = db.execute('SELECT product, SUM(amount) AS total FROM orders GROUP BY product HAVING SUM(amount) > 200');
    assert.ok(r.rows.length >= 1);
    assert.ok(r.rows.every(row => row.total > 200));
  });

  it('HAVING with COUNT', () => {
    setup();
    const r = db.execute('SELECT product, COUNT(*) AS cnt FROM orders GROUP BY product HAVING COUNT(*) > 1');
    assert.ok(r.rows.every(row => row.cnt > 1));
  });

  it('HAVING with AVG', () => {
    setup();
    const r = db.execute('SELECT product, AVG(amount) AS avg FROM orders GROUP BY product HAVING AVG(amount) > 100');
    assert.ok(r.rows.every(row => row.avg > 100));
  });

  it('HAVING with COALESCE-wrapped aggregate', () => {
    setup();
    // Product A (Widget): discount sum = 10, Product B (Gadget): = 20
    // Doohickey: discount = 5
    const r = db.execute('SELECT product, SUM(amount) AS total FROM orders GROUP BY product HAVING COALESCE(SUM(discount), 0) > 0');
    assert.ok(r.rows.length >= 2);
  });

  it('HAVING with expression using multiple aggregates', () => {
    setup();
    const r = db.execute('SELECT product, SUM(amount) AS total FROM orders GROUP BY product HAVING SUM(amount) > COUNT(*) * 100');
    assert.ok(r.rows.length >= 1);
  });

  it('HAVING NOT IN subquery', () => {
    setup();
    const r = db.execute(`
      SELECT product, SUM(amount) AS total 
      FROM orders 
      GROUP BY product 
      HAVING product NOT IN ('Doohickey')
    `);
    assert.ok(r.rows.every(row => row.product !== 'Doohickey'));
  });

  it('HAVING with BETWEEN', () => {
    setup();
    const r = db.execute('SELECT product, SUM(amount) AS total FROM orders GROUP BY product HAVING SUM(amount) BETWEEN 100 AND 400');
    assert.ok(r.rows.every(row => row.total >= 100 && row.total <= 400));
  });

  it('HAVING with complex expression', () => {
    setup();
    const r = db.execute(`
      SELECT region, COUNT(*) AS cnt, SUM(amount) AS total 
      FROM orders 
      GROUP BY region 
      HAVING COUNT(*) >= 2 AND SUM(amount) > 200
    `);
    assert.ok(r.rows.length >= 1);
    assert.ok(r.rows.every(row => row.cnt >= 2 && row.total > 200));
  });

  it('HAVING without matching aggregate in SELECT', () => {
    setup();
    // HAVING references MAX(amount) which is not in SELECT
    const r = db.execute('SELECT product FROM orders GROUP BY product HAVING MAX(amount) > 100');
    assert.ok(r.rows.length >= 1);
  });

  it('HAVING with CASE expression', () => {
    setup();
    const r = db.execute(`
      SELECT product, SUM(amount) AS total,
        CASE WHEN SUM(amount) > 200 THEN 'high' ELSE 'low' END AS level
      FROM orders 
      GROUP BY product
      HAVING CASE WHEN SUM(amount) > 200 THEN 'high' ELSE 'low' END = 'high'
    `);
    assert.ok(r.rows.every(row => row.level === 'high'));
  });
});
