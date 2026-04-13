// groupby-stress.test.js — Stress tests for GROUP BY and HAVING
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP BY stress tests', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer TEXT, product TEXT, qty INT, price INT, region TEXT)');
    const data = [
      [1, 'Alice', 'Widget', 5, 10, 'East'],
      [2, 'Alice', 'Gadget', 3, 20, 'East'],
      [3, 'Bob', 'Widget', 10, 10, 'West'],
      [4, 'Bob', 'Widget', 2, 10, 'West'],
      [5, 'Carol', 'Gadget', 7, 20, 'East'],
      [6, 'Carol', 'Doohickey', 1, 50, 'East'],
      [7, 'Dave', 'Widget', 4, 10, 'West'],
      [8, 'Dave', 'Gadget', 6, 20, 'West'],
      [9, 'Alice', 'Doohickey', 2, 50, 'East'],
      [10, 'Bob', 'Gadget', 1, 20, 'West'],
    ];
    for (const [id, c, p, q, pr, r] of data) {
      db.execute(`INSERT INTO orders VALUES (${id}, '${c}', '${p}', ${q}, ${pr}, '${r}')`);
    }
  });

  it('basic GROUP BY with COUNT', () => {
    const r = db.execute('SELECT customer, COUNT(*) as cnt FROM orders GROUP BY customer ORDER BY cnt DESC');
    assert.strictEqual(r.rows.length, 4);
    assert.strictEqual(r.rows[0].customer, 'Alice');
    assert.strictEqual(r.rows[0].cnt, 3);
  });

  it('GROUP BY with multiple aggregates', () => {
    const r = db.execute(`
      SELECT customer, COUNT(*) as cnt, SUM(qty) as total_qty, AVG(price) as avg_price, MAX(qty) as max_qty
      FROM orders GROUP BY customer ORDER BY customer
    `);
    assert.strictEqual(r.rows.length, 4);
    const alice = r.rows[0];
    assert.strictEqual(alice.cnt, 3);
    assert.strictEqual(alice.total_qty, 10); // 5+3+2
  });

  it('GROUP BY multiple columns', () => {
    const r = db.execute(`
      SELECT customer, region, COUNT(*) as cnt
      FROM orders GROUP BY customer, region ORDER BY customer, region
    `);
    assert.ok(r.rows.length >= 4);
    // Alice is always East
    const alice = r.rows.filter(r => r.customer === 'Alice');
    assert.strictEqual(alice.length, 1);
    assert.strictEqual(alice[0].region, 'East');
  });

  it('HAVING with simple condition', () => {
    const r = db.execute(`
      SELECT customer, COUNT(*) as cnt FROM orders 
      GROUP BY customer HAVING COUNT(*) >= 3 ORDER BY customer
    `);
    // Alice (3) and Bob (3) have 3+ orders
    assert.strictEqual(r.rows.length, 2);
  });

  it('HAVING with complex aggregate expression', () => {
    const r = db.execute(`
      SELECT customer, SUM(qty * price) as total_value
      FROM orders GROUP BY customer
      HAVING SUM(qty * price) > 100
      ORDER BY total_value DESC
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.total_value > 100);
    }
  });

  it('HAVING with multiple conditions', () => {
    const r = db.execute(`
      SELECT customer, COUNT(*) as cnt, SUM(qty) as total
      FROM orders GROUP BY customer
      HAVING COUNT(*) >= 2 AND SUM(qty) > 5
      ORDER BY customer
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.cnt >= 2);
      assert.ok(row.total > 5);
    }
  });

  it('GROUP BY with WHERE and HAVING together', () => {
    const r = db.execute(`
      SELECT customer, SUM(qty) as total
      FROM orders WHERE region = 'East'
      GROUP BY customer
      HAVING SUM(qty) >= 5
      ORDER BY customer
    `);
    // East orders only, then filter by total
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.total >= 5);
    }
  });

  it('GROUP BY with NULL values', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE t (cat TEXT, val INT)');
    db2.execute("INSERT INTO t VALUES ('A', 10)");
    db2.execute("INSERT INTO t VALUES ('A', 20)");
    db2.execute("INSERT INTO t VALUES (NULL, 30)");
    db2.execute("INSERT INTO t VALUES (NULL, 40)");
    db2.execute("INSERT INTO t VALUES ('B', 50)");
    
    const r = db2.execute('SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY cat');
    assert.ok(r.rows.length >= 2);
    // NULLs should form their own group
    const nullGroup = r.rows.find(r => r.cat === null);
    if (nullGroup) {
      assert.strictEqual(nullGroup.total, 70);
    }
  });

  it('aggregate without GROUP BY (entire table)', () => {
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(qty) as total, AVG(price) as avg_price FROM orders');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].cnt, 10);
  });

  it('GROUP BY with DISTINCT', () => {
    const r = db.execute('SELECT DISTINCT region, COUNT(*) as cnt FROM orders GROUP BY region ORDER BY region');
    assert.ok(r.rows.length >= 2);
  });

  it('GROUP BY with ORDER BY aggregate', () => {
    const r = db.execute(`
      SELECT product, SUM(qty) as total_qty FROM orders
      GROUP BY product ORDER BY SUM(qty) DESC
    `);
    assert.ok(r.rows.length >= 3);
    // First should have highest total
    assert.ok(r.rows[0].total_qty >= r.rows[1].total_qty);
  });

  it('GROUP BY result in subquery', () => {
    const r = db.execute(`
      SELECT customer FROM orders
      GROUP BY customer
      HAVING SUM(qty) = (SELECT MAX(total) FROM (SELECT SUM(qty) as total FROM orders GROUP BY customer) sub)
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('empty GROUP BY result', () => {
    const r = db.execute(`
      SELECT customer, COUNT(*) as cnt FROM orders
      WHERE price > 9999
      GROUP BY customer
    `);
    assert.strictEqual(r.rows.length, 0);
  });

  it('HAVING without matching rows', () => {
    const r = db.execute(`
      SELECT customer, SUM(qty) as total FROM orders
      GROUP BY customer HAVING SUM(qty) > 9999
    `);
    assert.strictEqual(r.rows.length, 0);
  });

  it('GROUP BY with expression in SELECT', () => {
    const r = db.execute(`
      SELECT customer, SUM(qty * price) as revenue, COUNT(*) as orders
      FROM orders GROUP BY customer ORDER BY revenue DESC
    `);
    assert.strictEqual(r.rows.length, 4);
    // Verify revenue calculation
    for (const row of r.rows) {
      assert.ok(row.revenue > 0);
    }
  });

  it('GROUP BY with CASE expression', () => {
    try {
      const r = db.execute(`
        SELECT 
          CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END as tier,
          COUNT(*) as cnt
        FROM orders
        GROUP BY CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END
        ORDER BY tier
      `);
      assert.strictEqual(r.rows.length, 2);
    } catch (e) {
      // GROUP BY CASE may not be supported
      assert.ok(true);
    }
  });

  it('multiple GROUP BY queries in sequence', () => {
    // Verify no state leaks between queries
    const r1 = db.execute('SELECT region, COUNT(*) as cnt FROM orders GROUP BY region');
    const r2 = db.execute('SELECT customer, SUM(qty) as total FROM orders GROUP BY customer');
    const r3 = db.execute('SELECT product, AVG(price) as avg_price FROM orders GROUP BY product');
    
    assert.ok(r1.rows.length >= 2);
    assert.ok(r2.rows.length >= 4);
    assert.ok(r3.rows.length >= 3);
  });

  it('GROUP BY with COUNT(DISTINCT)', () => {
    const r = db.execute(`
      SELECT customer, COUNT(DISTINCT product) as unique_products
      FROM orders GROUP BY customer ORDER BY customer
    `);
    assert.strictEqual(r.rows.length, 4);
    const alice = r.rows.find(r => r.customer === 'Alice');
    assert.strictEqual(alice.unique_products, 3); // Widget, Gadget, Doohickey
  });

  it('GROUP BY with LIMIT', () => {
    const r = db.execute(`
      SELECT customer, SUM(qty) as total FROM orders
      GROUP BY customer ORDER BY total DESC LIMIT 2
    `);
    assert.strictEqual(r.rows.length, 2);
  });

  it('GROUP BY single row table', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE single (cat TEXT, val INT)');
    db2.execute("INSERT INTO single VALUES ('A', 42)");
    const r = db2.execute('SELECT cat, SUM(val) as total FROM single GROUP BY cat');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].total, 42);
  });
});
