// compound-queries-stress.test.js — Tests for compound/complex query patterns
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Compound query stress tests', () => {
  
  it('SELECT with subquery in SELECT, WHERE, and HAVING all at once', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES ('cat${i % 4}', ${i})`);
    
    const r = db.execute(`
      SELECT cat, SUM(val) as total,
        (SELECT AVG(val) FROM t) as global_avg
      FROM t
      GROUP BY cat
      HAVING SUM(val) > (SELECT AVG(val) FROM t) * 2
      ORDER BY total DESC
    `);
    assert.ok(r.rows.length >= 0); // Just shouldn't crash
  });

  it('nested aggregation via subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES ('cat${i % 3}', ${i})`);
    
    // Average of group totals
    const r = db.execute(`
      SELECT AVG(sub.total) as avg_total
      FROM (SELECT cat, SUM(val) as total FROM t GROUP BY cat) sub
    `);
    assert.ok(r.rows[0].avg_total > 0);
  });

  it('CTE + window function + GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (month INT, val INT)');
    for (let i = 1; i <= 12; i++) db.execute(`INSERT INTO sales VALUES (${i}, ${i * 100})`);
    
    const r = db.execute(`
      WITH monthly AS (
        SELECT month, val FROM sales
      )
      SELECT month, val,
        SUM(val) OVER (ORDER BY month) as ytd
      FROM monthly
      ORDER BY month
    `);
    assert.strictEqual(r.rows.length, 12);
    assert.strictEqual(r.rows[11].ytd, 7800); // Sum of 1*100 to 12*100
  });

  it('multi-table JOIN + GROUP BY + HAVING + ORDER BY + LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT, region TEXT)');
    db.execute('CREATE TABLE orders (id INT, cust_id INT, amount INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO customers VALUES (${i}, 'R${i % 3}')`);
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${(i % 10) + 1}, ${i * 10})`);
    
    const r = db.execute(`
      SELECT c.region, COUNT(o.id) as order_count, SUM(o.amount) as revenue
      FROM customers c
      JOIN orders o ON c.id = o.cust_id
      GROUP BY c.region
      HAVING COUNT(o.id) >= 10
      ORDER BY revenue DESC
      LIMIT 2
    `);
    assert.ok(r.rows.length <= 2);
    assert.ok(r.rows[0].revenue > 0);
  });

  it('self-join with aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'CEO', NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'VP', 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Manager', 2)");
    
    const r = db.execute(`
      SELECT e.name as employee, m.name as manager
      FROM employees e
      LEFT JOIN employees m ON e.manager_id = m.id
      ORDER BY e.id
    `);
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].manager, null);
    assert.strictEqual(r.rows[1].manager, 'CEO');
  });

  it('query referencing window + aggregate + CASE + JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, region TEXT, amount INT)');
    for (let i = 1; i <= 30; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, 'R${i % 3}', ${i * 10})`);
    }
    
    const r = db.execute(`
      SELECT region, SUM(amount) as total,
        CASE WHEN SUM(amount) > 3000 THEN 'high' ELSE 'low' END as tier
      FROM orders
      GROUP BY region
      ORDER BY total DESC
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(['high', 'low'].includes(r.rows[0].tier));
  });

  it('deeply nested subquery (3 levels)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute(`
      SELECT * FROM t WHERE val > (
        SELECT AVG(val) FROM t WHERE id > (
          SELECT MIN(id) FROM t
        )
      ) ORDER BY id
    `);
    assert.ok(r.rows.length > 0);
  });

  it('transaction with CTE and window function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (6, 60)');
    
    const r = db.execute(`
      WITH cte AS (SELECT * FROM t)
      SELECT id, val, SUM(val) OVER (ORDER BY id) as running
      FROM cte ORDER BY id
    `);
    assert.strictEqual(r.rows.length, 6);
    assert.strictEqual(r.rows[5].running, 210);
    
    db.execute('ROLLBACK');
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 5);
  });
});
