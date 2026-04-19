import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Sales Analytics Dashboard (2026-04-19)', () => {
  let db;

  before(() => {
    db = new Database();
    
    // Schema
    db.execute(`CREATE TABLE customers (
      id INT PRIMARY KEY, name TEXT NOT NULL, region TEXT, tier TEXT DEFAULT 'standard'
    )`);
    db.execute(`CREATE TABLE products (
      id INT PRIMARY KEY, name TEXT NOT NULL, category TEXT, price FLOAT
    )`);
    db.execute(`CREATE TABLE orders (
      id INT PRIMARY KEY, customer_id INT, product_id INT, quantity INT, 
      order_date TEXT, status TEXT DEFAULT 'pending',
      CHECK (quantity > 0)
    )`);
    db.execute('CREATE INDEX idx_orders_customer ON orders (customer_id)');
    db.execute('CREATE INDEX idx_orders_product ON orders (product_id)');
    
    // Seed data
    db.execute("INSERT INTO customers VALUES (1,'Alice','East','premium'),(2,'Bob','West','standard'),(3,'Carol','East','premium'),(4,'Dave','West','basic'),(5,'Eve','East','standard')");
    db.execute("INSERT INTO products VALUES (1,'Widget','Electronics',29.99),(2,'Gadget','Electronics',49.99),(3,'Doohickey','Tools',19.99),(4,'Thingamajig','Tools',9.99)");
    db.execute("INSERT INTO orders VALUES (1,1,1,5,'2026-01-15','shipped')");
    db.execute("INSERT INTO orders VALUES (2,1,2,2,'2026-01-20','delivered')");
    db.execute("INSERT INTO orders VALUES (3,2,1,10,'2026-02-01','shipped')");
    db.execute("INSERT INTO orders VALUES (4,3,3,3,'2026-02-15','pending')");
    db.execute("INSERT INTO orders VALUES (5,4,4,20,'2026-03-01','delivered')");
    db.execute("INSERT INTO orders VALUES (6,5,2,1,'2026-03-10','shipped')");
    db.execute("INSERT INTO orders VALUES (7,1,3,8,'2026-03-15','delivered')");
    db.execute("INSERT INTO orders VALUES (8,2,4,15,'2026-03-20','pending')");
    db.execute("INSERT INTO orders VALUES (9,3,1,4,'2026-04-01','shipped')");
    db.execute("INSERT INTO orders VALUES (10,5,2,6,'2026-04-10','delivered')");
  });

  it('Q1: Revenue by category', () => {
    const r = db.execute(`
      SELECT p.category, SUM(o.quantity * p.price) AS revenue
      FROM orders o JOIN products p ON o.product_id = p.id
      GROUP BY p.category
      ORDER BY revenue DESC
    `);
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows[0].revenue > r.rows[1].revenue);
  });

  it('Q2: Top customers by total spend', () => {
    const r = db.execute(`
      SELECT c.name, c.tier, SUM(o.quantity * p.price) AS total_spent
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN products p ON o.product_id = p.id
      GROUP BY c.name, c.tier
      ORDER BY total_spent DESC
      LIMIT 3
    `);
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows[0].total_spent >= r.rows[1].total_spent);
  });

  it('Q3: Monthly revenue trend', () => {
    const r = db.execute(`
      WITH monthly AS (
        SELECT SUBSTR(order_date, 1, 7) AS month,
               SUM(o.quantity * p.price) AS revenue
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY SUBSTR(order_date, 1, 7)
      )
      SELECT month, revenue,
        revenue - LAG(revenue) OVER (ORDER BY month) AS growth
      FROM monthly
      ORDER BY month
    `);
    assert.ok(r.rows.length >= 3);
    assert.equal(r.rows[0].growth, null);  // first month has no lag
  });

  it('Q4: Region comparison with CASE', () => {
    const r = db.execute(`
      SELECT c.region,
        COUNT(DISTINCT o.customer_id) AS customers,
        SUM(o.quantity * p.price) AS revenue,
        CASE WHEN SUM(o.quantity * p.price) > 500 THEN 'strong' ELSE 'growing' END AS status
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN products p ON o.product_id = p.id
      GROUP BY c.region
    `);
    assert.equal(r.rows.length, 2);
  });

  it('Q5: Customer ranking within region', () => {
    const r = db.execute(`
      WITH customer_spend AS (
        SELECT c.id, c.name, c.region,
          SUM(o.quantity * p.price) AS total_spent
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN products p ON o.product_id = p.id
        GROUP BY c.id, c.name, c.region
      )
      SELECT name, region, total_spent,
        RANK() OVER (PARTITION BY region ORDER BY total_spent DESC) AS rank
      FROM customer_spend
      ORDER BY region, rank
    `);
    assert.ok(r.rows.length >= 4);
  });

  it('Q6: Products never ordered', () => {
    const r = db.execute(`
      SELECT p.name 
      FROM products p
      WHERE p.id NOT IN (SELECT DISTINCT product_id FROM orders)
    `);
    // All products have been ordered in our seed data
    assert.equal(r.rows.length, 0);
  });

  it('Q7: Order status summary with COALESCE', () => {
    const r = db.execute(`
      SELECT status,
        COUNT(*) AS order_count,
        COALESCE(SUM(quantity), 0) AS total_items
      FROM orders
      GROUP BY status
      ORDER BY order_count DESC
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows.every(row => row.total_items > 0));
  });

  it('Q8: Running total for a customer', () => {
    const r = db.execute(`
      SELECT o.id, o.order_date, o.quantity * p.price AS order_value,
        SUM(o.quantity * p.price) OVER (ORDER BY o.order_date) AS running_total
      FROM orders o
      JOIN products p ON o.product_id = p.id
      WHERE o.customer_id = 1
      ORDER BY o.order_date
    `);
    assert.ok(r.rows.length >= 2);
    // Running total should be non-decreasing
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].running_total >= r.rows[i - 1].running_total);
    }
  });

  it('Q9: Premium tier analysis with CTE + HAVING', () => {
    const r = db.execute(`
      WITH tier_stats AS (
        SELECT c.tier, COUNT(DISTINCT c.id) AS customers,
          SUM(o.quantity * p.price) AS revenue
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN products p ON o.product_id = p.id
        GROUP BY c.tier
      )
      SELECT tier, customers, revenue,
        CAST(revenue AS FLOAT) / customers AS revenue_per_customer
      FROM tier_stats
      WHERE revenue > 100
      ORDER BY revenue_per_customer DESC
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('Q10: INSERT with CHECK constraint', () => {
    assert.throws(() => db.execute("INSERT INTO orders VALUES (100, 1, 1, 0, '2026-05-01', 'test')"),
      /CHECK/i);  // quantity must be > 0
    
    db.execute("INSERT INTO orders VALUES (100, 1, 1, 5, '2026-05-01', 'test')");
    const r = db.execute('SELECT COUNT(*) AS cnt FROM orders');
    assert.equal(r.rows[0].cnt, 11);
  });
});
