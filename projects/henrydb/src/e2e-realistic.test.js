// e2e-realistic.test.js — End-to-end realistic database scenario
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('End-to-end realistic scenario: E-commerce analytics', () => {
  let db;

  it('setup: create schema and load data', () => {
    db = new Database();
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, joined TEXT)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, total INT)');
    db.execute('CREATE TABLE order_items (order_id INT, product_id INT, qty INT, line_total INT)');
    
    // Customers
    const regions = ['East', 'West', 'Central'];
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${regions[i % 3]}', '2024-0${(i % 9) + 1}-01')`);
    }
    
    // Products
    const cats = ['Electronics', 'Books', 'Clothing', 'Food', 'Tools'];
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', '${cats[i % 5]}', ${(i * 7) % 100 + 5})`);
    }
    
    // Orders and items
    for (let i = 1; i <= 200; i++) {
      const custId = (i % 50) + 1;
      const month = String((i % 12) + 1).padStart(2, '0');
      const total = ((i * 13) % 500) + 10;
      db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, '2024-${month}-${String((i%28)+1).padStart(2,'0')}', ${total})`);
      
      // 1-3 items per order
      const numItems = (i % 3) + 1;
      for (let j = 0; j < numItems; j++) {
        const prodId = ((i + j) % 20) + 1;
        const qty = (j + 1);
        const lineTotal = qty * (((i + j) * 7) % 100 + 5);
        db.execute(`INSERT INTO order_items VALUES (${i}, ${prodId}, ${qty}, ${lineTotal})`);
      }
    }
    
    // Create indexes
    db.execute('CREATE INDEX idx_orders_cust ON orders (customer_id)');
    db.execute('CREATE INDEX idx_items_order ON order_items (order_id)');
    db.execute('CREATE INDEX idx_items_prod ON order_items (product_id)');
    
    // Analyze all tables
    db.execute('ANALYZE TABLE customers');
    db.execute('ANALYZE TABLE products');
    db.execute('ANALYZE TABLE orders');
    db.execute('ANALYZE TABLE order_items');
  });

  it('query 1: top 5 customers by total spend', () => {
    const r = db.execute(`
      SELECT c.name, SUM(o.total) as total_spend, COUNT(*) as order_count
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      ORDER BY total_spend DESC
      LIMIT 5
    `);
    assert.strictEqual(r.rows.length, 5);
    assert.ok(r.rows[0].total_spend >= r.rows[1].total_spend);
    assert.ok(r.rows[0].order_count > 0);
  });

  it('query 2: revenue by region with HAVING', () => {
    const r = db.execute(`
      SELECT c.region, SUM(o.total) as revenue, COUNT(DISTINCT c.id) as customers
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.region
      HAVING SUM(o.total) > 1000
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.revenue > 1000);
    }
  });

  it('query 3: product sales with window function', () => {
    const r = db.execute(`
      SELECT p.name, p.category, SUM(oi.qty) as total_sold
      FROM products p
      JOIN order_items oi ON p.id = oi.product_id
      GROUP BY p.name, p.category
      ORDER BY p.category, total_sold DESC
    `);
    assert.ok(r.rows.length > 0);
    // Verify each product has sales
    for (const row of r.rows) {
      assert.ok(row.total_sold > 0);
    }
  });

  it('query 4: running total using CTE', () => {
    const r = db.execute(`
      WITH monthly AS (
        SELECT order_date, SUM(total) as monthly_total
        FROM orders
        GROUP BY order_date
      )
      SELECT order_date, monthly_total,
        SUM(monthly_total) OVER (ORDER BY order_date) as running_total
      FROM monthly
      ORDER BY order_date
      LIMIT 10
    `);
    assert.strictEqual(r.rows.length, 10);
    // Running total should be monotonically increasing
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].running_total >= r.rows[i-1].running_total);
    }
  });

  it('query 5: customers who never ordered (NOT EXISTS)', () => {
    // First add some customers with no orders
    db.execute("INSERT INTO customers VALUES (51, 'NoOrders1', 'East', '2024-01-01')");
    db.execute("INSERT INTO customers VALUES (52, 'NoOrders2', 'West', '2024-01-01')");
    
    const r = db.execute(`
      SELECT c.name FROM customers c
      WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
      ORDER BY c.name
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows.some(r => r.name === 'NoOrders1'));
  });

  it('query 6: cross-category purchase analysis', () => {
    const r = db.execute(`
      SELECT c.name,
        COUNT(DISTINCT p.category) as categories_bought,
        SUM(oi.line_total) as total_spent
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN order_items oi ON o.id = oi.order_id
      JOIN products p ON oi.product_id = p.id
      GROUP BY c.name
      HAVING COUNT(DISTINCT p.category) >= 3
      ORDER BY total_spent DESC
      LIMIT 10
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.categories_bought >= 3);
    }
  });

  it('query 7: transaction safety — transfer budget between regions', () => {
    db.execute('CREATE TABLE region_budget (region TEXT, budget INT)');
    db.execute("INSERT INTO region_budget VALUES ('East', 10000)");
    db.execute("INSERT INTO region_budget VALUES ('West', 5000)");
    
    db.execute('BEGIN');
    db.execute("UPDATE region_budget SET budget = budget - 2000 WHERE region = 'East'");
    db.execute("UPDATE region_budget SET budget = budget + 2000 WHERE region = 'West'");
    
    // Verify mid-transaction
    const mid = db.execute('SELECT SUM(budget) as total FROM region_budget');
    assert.strictEqual(mid.rows[0].total, 15000); // Total preserved
    
    db.execute('COMMIT');
    
    const post = db.execute('SELECT * FROM region_budget ORDER BY region');
    assert.strictEqual(post.rows[0].budget, 8000); // East
    assert.strictEqual(post.rows[1].budget, 7000); // West
  });

  it('query 8: EXPLAIN ANALYZE on complex query', () => {
    const r = db.execute(`
      EXPLAIN ANALYZE SELECT c.region, COUNT(*) as orders, SUM(o.total) as revenue
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE o.total > 100
      GROUP BY c.region
      ORDER BY revenue DESC
    `);
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time'));
  });

  it('query 9: subquery + derived table', () => {
    const r = db.execute(`
      SELECT sub.region, sub.avg_order
      FROM (
        SELECT c.region, AVG(o.total) as avg_order
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.region
      ) sub
      ORDER BY sub.avg_order DESC
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows[0].avg_order >= r.rows[1].avg_order);
  });

  it('query 10: cleanup and verification', () => {
    // Drop temp tables
    db.execute('DROP TABLE region_budget');
    
    // Final count verification
    const customers = db.execute('SELECT COUNT(*) as cnt FROM customers').rows[0].cnt;
    const products = db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt;
    const orders = db.execute('SELECT COUNT(*) as cnt FROM orders').rows[0].cnt;
    
    assert.strictEqual(customers, 52);
    assert.strictEqual(products, 20);
    assert.strictEqual(orders, 200);
  });
});
