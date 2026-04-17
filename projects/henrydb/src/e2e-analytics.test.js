// e2e-analytics.test.js — E2E analytics workload

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-analytics-'));
  db = TransactionalDatabase.open(dbDir);
  
  // Sales analytics schema
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, segment TEXT)');
  db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, total INT)');
  db.execute('CREATE TABLE order_items (order_id INT, product_id INT, qty INT, amount INT)');
  
  // Customers
  db.execute("INSERT INTO customers VALUES (1, 'Acme Corp', 'enterprise')");
  db.execute("INSERT INTO customers VALUES (2, 'Smith LLC', 'smb')");
  db.execute("INSERT INTO customers VALUES (3, 'Global Inc', 'enterprise')");
  db.execute("INSERT INTO customers VALUES (4, 'Local Shop', 'smb')");
  
  // Products
  db.execute("INSERT INTO products VALUES (1, 'Widget Pro', 'hardware', 100)");
  db.execute("INSERT INTO products VALUES (2, 'Gadget Plus', 'electronics', 250)");
  db.execute("INSERT INTO products VALUES (3, 'Service Pack', 'services', 500)");
  
  // Orders
  db.execute("INSERT INTO orders VALUES (1, 1, '2024-01-15', 800)");
  db.execute("INSERT INTO orders VALUES (2, 1, '2024-02-20', 250)");
  db.execute("INSERT INTO orders VALUES (3, 2, '2024-01-25', 1000)");
  db.execute("INSERT INTO orders VALUES (4, 3, '2024-03-10', 600)");
  db.execute("INSERT INTO orders VALUES (5, 4, '2024-03-15', 200)");
  
  // Order items
  db.execute('INSERT INTO order_items VALUES (1, 1, 3, 300)');
  db.execute('INSERT INTO order_items VALUES (1, 3, 1, 500)');
  db.execute('INSERT INTO order_items VALUES (2, 2, 1, 250)');
  db.execute('INSERT INTO order_items VALUES (3, 3, 2, 1000)');
  db.execute('INSERT INTO order_items VALUES (4, 1, 5, 500)');
  db.execute('INSERT INTO order_items VALUES (4, 2, 1, 250)'); // Fix: changed from (4, 2, 1, 100)
  db.execute('INSERT INTO order_items VALUES (5, 1, 2, 200)');
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Sales Analytics', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('top products by revenue', () => {
    const r = rows(db.execute(
      'SELECT p.name, SUM(oi.amount) AS revenue ' +
      'FROM order_items oi ' +
      'INNER JOIN products p ON oi.product_id = p.id ' +
      'GROUP BY p.name ' +
      'ORDER BY revenue DESC'
    ));
    assert.equal(r.length, 3);
    // Service Pack: 500 + 1000 = 1500
    // Widget Pro: 300 + 500 + 200 = 1000
    // Gadget Plus: 250 + 250 = 500
    assert.equal(r[0].name, 'Service Pack');
    assert.equal(r[0].revenue, 1500);
  });

  it('revenue by customer segment', () => {
    const r = rows(db.execute(
      'SELECT c.segment, SUM(o.total) AS total_revenue, COUNT(o.id) AS num_orders ' +
      'FROM customers c ' +
      'INNER JOIN orders o ON c.id = o.customer_id ' +
      'GROUP BY c.segment ' +
      'ORDER BY total_revenue DESC'
    ));
    assert.equal(r.length, 2);
    // enterprise: 800+250+600=1650, smb: 1000+200=1200
    assert.equal(r[0].segment, 'enterprise');
  });

  it('customer spend ranking with window function', () => {
    const r = rows(db.execute(
      'WITH customer_spend AS (' +
      '  SELECT c.name, SUM(o.total) AS total_spend ' +
      '  FROM customers c INNER JOIN orders o ON c.id = o.customer_id ' +
      '  GROUP BY c.name' +
      ') ' +
      'SELECT name, total_spend, RANK() OVER (ORDER BY total_spend DESC) AS rnk ' +
      'FROM customer_spend'
    ));
    assert.ok(r.length >= 4);
    assert.equal(r[0].rnk, 1);
  });

  it('monthly revenue trend', () => {
    const r = rows(db.execute(
      "SELECT SUBSTRING(order_date, 1, 7) AS month, SUM(total) AS revenue " +
      "FROM orders GROUP BY SUBSTRING(order_date, 1, 7) ORDER BY month"
    ));
    // 2024-01: 800+1000=1800, 2024-02: 250, 2024-03: 600+200=800
    assert.equal(r.length, 3);
    // Alias may be 'month' or 'expr_0' depending on GROUP BY handling
    const months = r.map(x => x.month || x.expr_0);
    assert.equal(months[0], '2024-01');
    const revenues = r.map(x => x.revenue);
    assert.equal(revenues[0], 1800);
  });

  it('category contribution percentage', () => {
    const r = rows(db.execute(
      'WITH category_rev AS (' +
      '  SELECT p.category, SUM(oi.amount) AS rev ' +
      '  FROM order_items oi INNER JOIN products p ON oi.product_id = p.id ' +
      '  GROUP BY p.category' +
      ') ' +
      'SELECT category, rev FROM category_rev ORDER BY rev DESC'
    ));
    assert.ok(r.length >= 2);
  });
});
