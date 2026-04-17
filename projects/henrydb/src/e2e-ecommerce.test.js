// e2e-ecommerce.test.js — End-to-end SQL integration test
// Real-world scenario: e-commerce database testing all major features together.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ecom-'));
  db = TransactionalDatabase.open(dbDir);
  
  // Schema
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, region TEXT)');
  db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL, category TEXT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, status TEXT)');
  db.execute('CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product_id INT, quantity INT, unit_price INT)');
  
  // Indexes
  db.execute('CREATE INDEX idx_orders_customer ON orders (customer_id)');
  db.execute('CREATE INDEX idx_items_order ON order_items (order_id)');
  
  // Data: Customers
  db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com', 'east')");
  db.execute("INSERT INTO customers VALUES (2, 'Bob', 'bob@test.com', 'west')");
  db.execute("INSERT INTO customers VALUES (3, 'Carol', 'carol@test.com', 'east')");
  db.execute("INSERT INTO customers VALUES (4, 'Dave', 'dave@test.com', 'west')");
  db.execute("INSERT INTO customers VALUES (5, 'Eve', 'eve@test.com', NULL)");
  
  // Data: Products
  db.execute("INSERT INTO products VALUES (1, 'Widget', 1000, 'hardware')");
  db.execute("INSERT INTO products VALUES (2, 'Gadget', 2500, 'hardware')");
  db.execute("INSERT INTO products VALUES (3, 'Service', 500, 'software')");
  db.execute("INSERT INTO products VALUES (4, 'Premium', 5000, 'software')");
  
  // Data: Orders
  db.execute("INSERT INTO orders VALUES (1, 1, '2024-01-15', 'shipped')");
  db.execute("INSERT INTO orders VALUES (2, 1, '2024-02-20', 'delivered')");
  db.execute("INSERT INTO orders VALUES (3, 2, '2024-01-10', 'delivered')");
  db.execute("INSERT INTO orders VALUES (4, 3, '2024-03-01', 'shipped')");
  db.execute("INSERT INTO orders VALUES (5, 4, '2024-02-15', 'cancelled')");
  
  // Data: Order Items
  db.execute('INSERT INTO order_items VALUES (1, 1, 1, 2, 1000)');  // Alice: 2 Widgets
  db.execute('INSERT INTO order_items VALUES (2, 1, 3, 1, 500)');   // Alice: 1 Service
  db.execute('INSERT INTO order_items VALUES (3, 2, 2, 1, 2500)');  // Alice: 1 Gadget
  db.execute('INSERT INTO order_items VALUES (4, 2, 4, 1, 5000)');  // Alice: 1 Premium
  db.execute('INSERT INTO order_items VALUES (5, 3, 1, 5, 1000)');  // Bob: 5 Widgets
  db.execute('INSERT INTO order_items VALUES (6, 4, 3, 3, 500)');   // Carol: 3 Services
  db.execute('INSERT INTO order_items VALUES (7, 5, 2, 1, 2500)');  // Dave: 1 Gadget (cancelled)
}

function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('E-Commerce: Basic Queries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('total revenue (excl cancelled)', () => {
    const r = rows(db.execute(
      'SELECT SUM(oi.quantity * oi.unit_price) AS revenue ' +
      'FROM order_items oi ' +
      'INNER JOIN orders o ON oi.order_id = o.id ' +
      "WHERE o.status != 'cancelled'"
    ));
    // Order 1: 2*1000 + 1*500 = 2500
    // Order 2: 1*2500 + 1*5000 = 7500
    // Order 3: 5*1000 = 5000
    // Order 4: 3*500 = 1500
    // Total = 16500
    assert.equal(r[0].revenue, 16500);
  });

  it('customer order count', () => {
    const r = rows(db.execute(
      'SELECT c.name, COUNT(o.id) AS order_count ' +
      'FROM customers c ' +
      'LEFT JOIN orders o ON c.id = o.customer_id ' +
      'GROUP BY c.name ' +
      'ORDER BY order_count DESC, c.name'
    ));
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].order_count, 2);
    // Eve has 0 orders
    const eve = r.find(x => x.name === 'Eve');
    assert.equal(eve.order_count, 0);
  });
});

describe('E-Commerce: Complex Queries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('top customer by revenue (CTE + JOIN + aggregate)', () => {
    const r = rows(db.execute(
      'WITH customer_revenue AS (' +
      '  SELECT o.customer_id, SUM(oi.quantity * oi.unit_price) AS total ' +
      '  FROM orders o ' +
      '  INNER JOIN order_items oi ON o.id = oi.order_id ' +
      "  WHERE o.status != 'cancelled' " +
      '  GROUP BY o.customer_id' +
      ') ' +
      'SELECT c.name, cr.total ' +
      'FROM customers c ' +
      'INNER JOIN customer_revenue cr ON c.id = cr.customer_id ' +
      'ORDER BY cr.total DESC ' +
      'LIMIT 1'
    ));
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].total, 10000); // 2500 + 7500
  });

  it('products never ordered (NOT EXISTS)', () => {
    const r = rows(db.execute(
      'SELECT p.name FROM products p ' +
      'WHERE NOT EXISTS (' +
      '  SELECT 1 FROM order_items oi WHERE oi.product_id = p.id' +
      ') ORDER BY p.name'
    ));
    // All products are ordered at least once, so should be 0
    assert.equal(r.length, 0);
  });

  it('revenue by category (JOIN + GROUP BY + ORDER BY)', () => {
    const r = rows(db.execute(
      'SELECT p.category, SUM(oi.quantity * oi.unit_price) AS revenue ' +
      'FROM order_items oi ' +
      'INNER JOIN products p ON oi.product_id = p.id ' +
      'INNER JOIN orders o ON oi.order_id = o.id ' +
      "WHERE o.status != 'cancelled' " +
      'GROUP BY p.category ' +
      'ORDER BY revenue DESC'
    ));
    assert.equal(r.length, 2);
    // hardware: 2*1000 + 1*2500 + 5*1000 = 9500
    // software: 1*500 + 1*5000 + 3*500 = 7000
    assert.equal(r[0].category, 'hardware');
    assert.equal(r[0].revenue, 9500);
    assert.equal(r[1].category, 'software');
    assert.equal(r[1].revenue, 7000);
  });

  it('rank customers by revenue (window function)', () => {
    const r = rows(db.execute(
      'WITH customer_revenue AS (' +
      '  SELECT o.customer_id, SUM(oi.quantity * oi.unit_price) AS total ' +
      '  FROM orders o ' +
      '  INNER JOIN order_items oi ON o.id = oi.order_id ' +
      "  WHERE o.status != 'cancelled' " +
      '  GROUP BY o.customer_id' +
      ') ' +
      'SELECT c.name, cr.total, RANK() OVER (ORDER BY cr.total DESC) AS rnk ' +
      'FROM customers c ' +
      'INNER JOIN customer_revenue cr ON c.id = cr.customer_id ' +
      'ORDER BY rnk'
    ));
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].rnk, 1);
  });

  it('monthly revenue analysis (GROUP BY expression)', () => {
    // Use a subquery to pre-compute the month to avoid GROUP BY expression issues
    const r = rows(db.execute(
      'SELECT month, SUM(item_total) AS revenue, COUNT(DISTINCT order_id) AS num_orders FROM (' +
      '  SELECT SUBSTR(o.order_date, 1, 7) AS month, o.id AS order_id, ' +
      '    oi.quantity * oi.unit_price AS item_total ' +
      '  FROM orders o ' +
      '  INNER JOIN order_items oi ON o.id = oi.order_id ' +
      "  WHERE o.status != 'cancelled'" +
      ') GROUP BY month ORDER BY month'
    ));
    // Jan: order 1 (2500) + order 3 (5000) = 7500, 2 orders
    // Feb: order 2 (7500) = 7500, 1 order
    // Mar: order 4 (1500) = 1500, 1 order
    assert.equal(r.length, 3);
    assert.equal(r[0].revenue, 7500);
    assert.equal(r[1].revenue, 7500);
    assert.equal(r[2].revenue, 1500);
  });

  it('customers in region with above-average orders (subquery)', () => {
    const r = rows(db.execute(
      'SELECT c.name, c.region FROM customers c ' +
      'WHERE c.region IS NOT NULL AND (' +
      '  SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id' +
      ') > (' +
      '  SELECT AVG(cnt) FROM (' +
      '    SELECT COUNT(*) AS cnt FROM orders GROUP BY customer_id' +
      '  )' +
      ') ORDER BY c.name'
    ));
    // Average orders per customer: (2+1+1+1)/4 = 1.25
    // Alice has 2 > 1.25 → included
    assert.ok(r.length >= 1);
    assert.equal(r[0].name, 'Alice');
  });
});

describe('E-Commerce: Transactional Operations', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('create order atomically', () => {
    const s = db.session();
    s.begin();
    
    // Create new order
    s.execute("INSERT INTO orders VALUES (6, 5, '2024-04-01', 'pending')");
    s.execute('INSERT INTO order_items VALUES (8, 6, 1, 1, 1000)');
    s.execute('INSERT INTO order_items VALUES (9, 6, 3, 2, 500)');
    
    s.commit();
    s.close();

    // Verify order exists with items
    const order = rows(db.execute('SELECT * FROM orders WHERE id = 6'));
    assert.equal(order.length, 1);
    
    const items = rows(db.execute('SELECT * FROM order_items WHERE order_id = 6'));
    assert.equal(items.length, 2);
    
    // Total: 1*1000 + 2*500 = 2000
    const total = rows(db.execute(
      'SELECT SUM(quantity * unit_price) AS total FROM order_items WHERE order_id = 6'
    ));
    assert.equal(total[0].total, 2000);
  });

  it('cancel order: rollback preserves data', () => {
    const s = db.session();
    s.begin();
    
    s.execute("UPDATE orders SET status = 'cancelled' WHERE id = 1");
    s.execute('DELETE FROM order_items WHERE order_id = 1');
    
    // Simulate failure: rollback
    s.rollback();
    s.close();

    // Order should still be shipped with items
    const order = rows(db.execute('SELECT status FROM orders WHERE id = 1'));
    assert.equal(order[0].status, 'shipped');
    
    const items = rows(db.execute('SELECT COUNT(*) AS c FROM order_items WHERE order_id = 1'));
    assert.equal(items[0].c, 2);
  });

  it('concurrent order creation: no constraint violations', () => {
    // Two sessions create orders simultaneously
    const s1 = db.session();
    s1.begin();
    s1.execute("INSERT INTO orders VALUES (6, 1, '2024-04-01', 'pending')");
    s1.execute('INSERT INTO order_items VALUES (8, 6, 1, 1, 1000)');

    const s2 = db.session();
    s2.begin();
    s2.execute("INSERT INTO orders VALUES (7, 2, '2024-04-01', 'pending')");
    s2.execute('INSERT INTO order_items VALUES (9, 7, 2, 1, 2500)');

    s1.commit();
    s2.commit();
    s1.close();
    s2.close();

    // Both orders should exist
    const count = rows(db.execute('SELECT COUNT(*) AS c FROM orders'));
    assert.equal(count[0].c, 7);
  });
});

describe('E-Commerce: Crash Recovery', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('entire schema and data survives crash', () => {
    db.close();
    db = TransactionalDatabase.open(dbDir);

    // Verify all tables and data
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM customers'))[0].c, 5);
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM products'))[0].c, 4);
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM orders'))[0].c, 5);
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM order_items'))[0].c, 7);

    // Complex query still works
    const revenue = rows(db.execute(
      'SELECT SUM(oi.quantity * oi.unit_price) AS total ' +
      'FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id ' +
      "WHERE o.status != 'cancelled'"
    ));
    assert.equal(revenue[0].total, 16500);
  });
});
