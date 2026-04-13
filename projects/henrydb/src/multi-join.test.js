// multi-join.test.js — Multi-table JOIN stress tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Multi-table JOINs', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    // Star schema: fact + dimensions
    db.execute('CREATE TABLE customers (id INT, name TEXT, region TEXT)');
    db.execute('CREATE TABLE products (id INT, name TEXT, category TEXT, price REAL)');
    db.execute('CREATE TABLE orders (id INT, customer_id INT, order_date TEXT)');
    db.execute('CREATE TABLE order_items (order_id INT, product_id INT, quantity INT)');

    // Customers
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'North')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob', 'South')");
    db.execute("INSERT INTO customers VALUES (3, 'Carol', 'North')");

    // Products
    db.execute("INSERT INTO products VALUES (1, 'Widget', 'A', 10.00)");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 'B', 25.00)");
    db.execute("INSERT INTO products VALUES (3, 'Doohickey', 'A', 5.00)");

    // Orders
    db.execute("INSERT INTO orders VALUES (1, 1, '2024-01-01')");
    db.execute("INSERT INTO orders VALUES (2, 1, '2024-02-01')");
    db.execute("INSERT INTO orders VALUES (3, 2, '2024-01-15')");
    db.execute("INSERT INTO orders VALUES (4, 3, '2024-03-01')");

    // Order items
    db.execute("INSERT INTO order_items VALUES (1, 1, 5)");
    db.execute("INSERT INTO order_items VALUES (1, 2, 2)");
    db.execute("INSERT INTO order_items VALUES (2, 3, 10)");
    db.execute("INSERT INTO order_items VALUES (3, 2, 1)");
    db.execute("INSERT INTO order_items VALUES (4, 1, 3)");
    db.execute("INSERT INTO order_items VALUES (4, 3, 7)");
  });

  describe('3-way JOINs', () => {
    it('customers → orders → order_items', () => {
      const r = db.execute(`
        SELECT c.name, o.id AS order_id, oi.product_id, oi.quantity
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        ORDER BY c.name, o.id, oi.product_id
      `);
      assert.ok(r.rows.length >= 6);
      assert.equal(r.rows[0].name, 'Alice');
    });

    it('3-way JOIN with aggregation', () => {
      const r = db.execute(`
        SELECT c.name, SUM(oi.quantity) AS total_items
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        GROUP BY c.name
        ORDER BY total_items DESC
      `);
      assert.equal(r.rows.length, 3);
      // Alice: 5 + 2 + 10 = 17, Bob: 1, Carol: 3 + 7 = 10
      assert.equal(r.rows[0].name, 'Alice');
      assert.equal(r.rows[0].total_items, 17);
    });
  });

  describe('4-way JOINs', () => {
    it('customers → orders → order_items → products', () => {
      const r = db.execute(`
        SELECT c.name AS customer, p.name AS product, oi.quantity, p.price
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        ORDER BY c.name, p.name
      `);
      assert.ok(r.rows.length >= 6);
      // Verify data
      const aliceWidget = r.rows.find(r => r.customer === 'Alice' && r.product === 'Widget');
      assert.ok(aliceWidget);
      assert.equal(aliceWidget.quantity, 5);
    });

    it('4-way JOIN with revenue calculation', () => {
      const r = db.execute(`
        SELECT c.name AS customer, SUM(oi.quantity * p.price) AS revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        GROUP BY c.name
        ORDER BY revenue DESC
      `);
      assert.equal(r.rows.length, 3);
      // Alice: 5*10 + 2*25 + 10*5 = 50 + 50 + 50 = 150
      assert.equal(r.rows[0].customer, 'Alice');
      assert.equal(r.rows[0].revenue, 150);
    });

    it('4-way JOIN with HAVING', () => {
      const r = db.execute(`
        SELECT c.name AS customer, SUM(oi.quantity * p.price) AS revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        GROUP BY c.name
        HAVING SUM(oi.quantity * p.price) > 30
        ORDER BY revenue DESC
      `);
      assert.ok(r.rows.length >= 2);
      assert.ok(r.rows.every(row => row.revenue > 30));
    });

    it('4-way JOIN with WHERE filter', () => {
      const r = db.execute(`
        SELECT c.name AS customer, p.category, SUM(oi.quantity) AS qty
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        WHERE c.region = 'North'
        GROUP BY c.name, p.category
        ORDER BY c.name, p.category
      `);
      // Only Alice and Carol (North region)
      assert.ok(r.rows.length >= 2);
      assert.ok(r.rows.every(row => row.customer === 'Alice' || row.customer === 'Carol'));
    });
  });

  describe('Mixed JOIN types', () => {
    it('LEFT JOIN + INNER JOIN', () => {
      db.execute("INSERT INTO customers VALUES (4, 'Dave', 'East')"); // No orders
      const r = db.execute(`
        SELECT c.name, COUNT(oi.product_id) AS item_count
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        LEFT JOIN order_items oi ON oi.order_id = o.id
        GROUP BY c.name
        ORDER BY item_count DESC
      `);
      assert.equal(r.rows.length, 4);
      const dave = r.rows.find(row => row.name === 'Dave');
      assert.equal(dave.item_count, 0);
    });
  });

  describe('Self-joins', () => {
    it('find customers in same region', () => {
      const r = db.execute(`
        SELECT c1.name AS customer1, c2.name AS customer2
        FROM customers c1
        JOIN customers c2 ON c1.region = c2.region AND c1.id < c2.id
        ORDER BY c1.name
      `);
      assert.equal(r.rows.length, 1); // Alice-Carol (both North)
      assert.equal(r.rows[0].customer1, 'Alice');
      assert.equal(r.rows[0].customer2, 'Carol');
    });
  });

  describe('Subquery with JOIN', () => {
    it('scalar subquery referencing joined tables', () => {
      const r = db.execute(`
        SELECT c.name, 
          (SELECT SUM(oi2.quantity) FROM order_items oi2 
           JOIN orders o2 ON oi2.order_id = o2.id 
           WHERE o2.customer_id = c.id) AS total_qty
        FROM customers c
        ORDER BY c.name
      `);
      assert.equal(r.rows.length, 3);
    });
  });
});
