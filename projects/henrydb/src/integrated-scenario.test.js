// integrated-scenario.test.js — A realistic database scenario exercising many features
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Integrated Scenario: E-Commerce Analytics', () => {
  let db;
  before(() => {
    db = new Database();
    
    // Schema
    db.execute(`CREATE TABLE products (
      id INT PRIMARY KEY, name TEXT, price INT, category TEXT, in_stock INT
    )`);
    db.execute(`CREATE TABLE customers (
      id INT PRIMARY KEY, name TEXT, email TEXT, region TEXT, tier TEXT
    )`);
    db.execute(`CREATE TABLE orders (
      id INT PRIMARY KEY, customer_id INT, product_id INT, qty INT, 
      total INT, status TEXT, order_date TEXT
    )`);
    
    // Products
    const products = [
      [1, 'Laptop', 1200, 'electronics', 50],
      [2, 'Phone', 800, 'electronics', 100],
      [3, 'Headphones', 150, 'electronics', 200],
      [4, 'Desk', 350, 'furniture', 30],
      [5, 'Chair', 250, 'furniture', 75],
      [6, 'Notebook', 5, 'office', 500],
      [7, 'Pen', 2, 'office', 1000],
      [8, 'Monitor', 400, 'electronics', 40],
    ];
    for (const [id, name, price, cat, stock] of products) {
      db.execute(`INSERT INTO products VALUES (${id}, '${name}', ${price}, '${cat}', ${stock})`);
    }
    
    // Customers
    for (let i = 1; i <= 20; i++) {
      const region = ['US', 'EU', 'Asia', 'Other'][i % 4];
      const tier = i % 5 === 0 ? 'premium' : 'standard';
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', 'c${i}@example.com', '${region}', '${tier}')`);
    }
    
    // Orders
    for (let i = 1; i <= 100; i++) {
      const cust = (i % 20) + 1;
      const prod = (i % 8) + 1;
      const qty = 1 + (i % 5);
      const price = products.find(p => p[0] === prod)[2];
      const total = price * qty;
      const status = i % 10 === 0 ? 'cancelled' : i % 3 === 0 ? 'shipped' : 'delivered';
      const month = String(1 + (i % 12)).padStart(2, '0');
      db.execute(`INSERT INTO orders VALUES (${i}, ${cust}, ${prod}, ${qty}, ${total}, '${status}', '2024-${month}-15')`);
    }
  });

  it('total revenue excluding cancelled orders', () => {
    const r = db.execute("SELECT SUM(total) as revenue FROM orders WHERE status != 'cancelled'");
    assert.ok(r.rows[0].revenue > 0);
  });

  it('revenue by category', () => {
    const r = db.execute(`
      SELECT p.category, SUM(o.total) as revenue, COUNT(*) as order_count
      FROM orders o JOIN products p ON o.product_id = p.id
      WHERE o.status != 'cancelled'
      GROUP BY p.category
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows[0].revenue >= r.rows[r.rows.length - 1].revenue);
  });

  it('top 5 customers by total spend', () => {
    const r = db.execute(`
      SELECT c.name, SUM(o.total) as total_spend, COUNT(*) as orders
      FROM customers c JOIN orders o ON c.id = o.customer_id
      WHERE o.status != 'cancelled'
      GROUP BY c.name
      ORDER BY total_spend DESC
      LIMIT 5
    `);
    assert.strictEqual(r.rows.length, 5);
    assert.ok(r.rows[0].total_spend >= r.rows[4].total_spend);
  });

  it('customer ranking with window function', () => {
    const r = db.execute(`
      WITH spend AS (
        SELECT c.name, SUM(o.total) as total_spend
        FROM customers c JOIN orders o ON c.id = o.customer_id
        WHERE o.status != 'cancelled'
        GROUP BY c.name
      )
      SELECT name, total_spend,
        RANK() OVER (ORDER BY total_spend DESC) as spend_rank
      FROM spend
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.some(r => r.spend_rank === 1));
  });

  it('CTE: premium customer analysis', () => {
    const r = db.execute(`
      WITH customer_stats AS (
        SELECT c.id, c.name, c.tier,
          COUNT(*) as order_count,
          SUM(o.total) as total_spend,
          AVG(o.total) as avg_order
        FROM customers c 
        JOIN orders o ON c.id = o.customer_id
        WHERE o.status != 'cancelled'
        GROUP BY c.id, c.name, c.tier
      )
      SELECT tier, COUNT(*) as customers, 
        SUM(total_spend) as tier_revenue,
        AVG(avg_order) as avg_order_value
      FROM customer_stats
      GROUP BY tier
      ORDER BY tier_revenue DESC
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('STRING_AGG: products per category', () => {
    const r = db.execute(`
      SELECT category, STRING_AGG(name, ', ') as products, COUNT(*) as count
      FROM products
      GROUP BY category
      ORDER BY count DESC
    `);
    assert.ok(r.rows[0].products.includes(','));
  });

  it('subquery: customers who bought electronics', () => {
    const r = db.execute(`
      SELECT DISTINCT c.name FROM customers c
      WHERE c.id IN (
        SELECT o.customer_id FROM orders o
        JOIN products p ON o.product_id = p.id
        WHERE p.category = 'electronics' AND o.status != 'cancelled'
      )
      ORDER BY c.name
    `);
    assert.ok(r.rows.length > 0);
  });

  it('EXISTS: products with orders', () => {
    const r = db.execute(`
      SELECT name, price FROM products p
      WHERE EXISTS (SELECT 1 FROM orders WHERE product_id = p.id)
      ORDER BY price DESC
    `);
    assert.ok(r.rows.length > 0);
  });

  it('CASE in SELECT: order status classification', () => {
    const r = db.execute(`
      SELECT 
        CASE 
          WHEN status = 'delivered' THEN 'completed'
          WHEN status = 'shipped' THEN 'in_transit'
          ELSE 'other'
        END as classification,
        COUNT(*) as count
      FROM orders
      GROUP BY CASE 
          WHEN status = 'delivered' THEN 'completed'
          WHEN status = 'shipped' THEN 'in_transit'
          ELSE 'other'
        END
      ORDER BY count DESC
    `);
    assert.ok(r.rows.length >= 2);
  });

  it('HAVING with aggregate expression', () => {
    const r = db.execute(`
      SELECT p.name, SUM(o.qty) as total_qty
      FROM orders o JOIN products p ON o.product_id = p.id
      GROUP BY p.name
      HAVING SUM(o.qty) > 20
      ORDER BY total_qty DESC
    `);
    assert.ok(r.rows.every(row => row.total_qty > 20));
  });

  it('running total by date', () => {
    const r = db.execute(`
      SELECT order_date, total,
        SUM(total) OVER (ORDER BY order_date) as running_total
      FROM orders
      WHERE status != 'cancelled'
      ORDER BY order_date
      LIMIT 10
    `);
    assert.strictEqual(r.rows.length, 10);
    // Running total should be non-decreasing
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].running_total >= r.rows[i-1].running_total);
    }
  });

  it('COALESCE with NULLs', () => {
    db.execute('ALTER TABLE products ADD COLUMN discount INT');
    const r = db.execute('SELECT name, COALESCE(discount, 0) as disc FROM products ORDER BY name');
    assert.ok(r.rows.every(row => row.disc === 0));
  });

  it('monthly revenue with CTE workaround', () => {
    const r = db.execute(`
      WITH monthly AS (
        SELECT SUBSTR(order_date, 6, 2) as month, total
        FROM orders WHERE status != 'cancelled'
      )
      SELECT month, SUM(total) as revenue
      FROM monthly
      GROUP BY month
      ORDER BY month
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.every(row => row.revenue > 0));
  });
});
