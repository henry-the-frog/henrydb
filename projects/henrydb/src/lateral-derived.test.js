// lateral-derived.test.js — LATERAL JOIN and derived table tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Derived Tables (Subquery in FROM)', () => {
  it('basic derived table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    
    const r = db.execute(`
      SELECT * FROM (SELECT id, val * 2 as doubled FROM t) sub
      ORDER BY id
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].doubled, 20);
    assert.equal(r.rows[2].doubled, 60);
  });

  it('derived table with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('N',100),('N',200),('S',150),('S',250)");
    
    const r = db.execute(`
      SELECT region, total FROM (
        SELECT region, SUM(amount) as total FROM sales GROUP BY region
      ) sub
      ORDER BY total DESC
    `);
    assert.equal(r.rows[0].region, 'S');
    assert.equal(r.rows[0].total, 400);
  });

  it('derived table with window function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');
    
    const r = db.execute(`
      SELECT * FROM (
        SELECT id, val, RANK() OVER (ORDER BY val DESC) as rnk FROM t
      ) ranked
      WHERE rnk <= 3
      ORDER BY rnk
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].val, 50); // rank 1
    assert.equal(r.rows[2].val, 30); // rank 3
  });
});

describe('LATERAL JOIN', () => {
  it('LATERAL with correlated subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    
    // LATERAL subquery that references outer row
    const r = db.execute(`
      SELECT t.id, sub.doubled
      FROM t, LATERAL (SELECT t.val * 2 as doubled) sub
      ORDER BY t.id
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].doubled, 20);
    assert.equal(r.rows[2].doubled, 60);
  });

  it('LATERAL subquery referencing outer table', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer TEXT, amount INT)');
    db.execute("INSERT INTO orders VALUES (1,'alice',100),(2,'alice',200),(3,'bob',150)");
    
    const r = db.execute(`
      SELECT DISTINCT customer, top_order.max_amount
      FROM orders o,
           LATERAL (SELECT MAX(amount) as max_amount FROM orders WHERE customer = o.customer) top_order
      ORDER BY customer
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].customer, 'alice');
    assert.equal(r.rows[0].max_amount, 200);
    assert.equal(r.rows[1].customer, 'bob');
    assert.equal(r.rows[1].max_amount, 150);
  });
});
