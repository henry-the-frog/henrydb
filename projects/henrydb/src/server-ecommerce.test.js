// server-ecommerce.test.js — E-commerce data model through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15515;

describe('E-Commerce Data Model', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    // Schema
    await client.query('CREATE TABLE customers (id INTEGER, name TEXT, email TEXT, city TEXT)');
    await client.query('CREATE TABLE products (id INTEGER, name TEXT, price REAL, category TEXT, stock INTEGER)');
    await client.query('CREATE TABLE orders (id INTEGER, customer_id INTEGER, total REAL, status TEXT, created_at TEXT)');
    await client.query('CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER, price REAL)');
    
    // Customers
    await client.query("INSERT INTO customers VALUES (1, 'Alice Johnson', 'alice@email.com', 'Denver')");
    await client.query("INSERT INTO customers VALUES (2, 'Bob Smith', 'bob@email.com', 'Austin')");
    await client.query("INSERT INTO customers VALUES (3, 'Charlie Brown', 'charlie@email.com', 'Denver')");
    
    // Products
    await client.query("INSERT INTO products VALUES (1, 'Laptop', 999.99, 'electronics', 50)");
    await client.query("INSERT INTO products VALUES (2, 'Headphones', 79.99, 'electronics', 200)");
    await client.query("INSERT INTO products VALUES (3, 'Coffee Beans', 14.99, 'food', 500)");
    await client.query("INSERT INTO products VALUES (4, 'Desk Lamp', 45.50, 'home', 150)");
    await client.query("INSERT INTO products VALUES (5, 'Running Shoes', 129.99, 'sports', 75)");
    
    // Orders
    await client.query("INSERT INTO orders VALUES (1, 1, 1079.98, 'completed', '2026-04-01')");
    await client.query("INSERT INTO orders VALUES (2, 2, 94.98, 'completed', '2026-04-02')");
    await client.query("INSERT INTO orders VALUES (3, 1, 14.99, 'pending', '2026-04-03')");
    await client.query("INSERT INTO orders VALUES (4, 3, 175.49, 'shipped', '2026-04-04')");
    
    // Order items
    await client.query("INSERT INTO order_items VALUES (1, 1, 1, 1, 999.99)");
    await client.query("INSERT INTO order_items VALUES (2, 1, 2, 1, 79.99)");
    await client.query("INSERT INTO order_items VALUES (3, 2, 2, 1, 79.99)");
    await client.query("INSERT INTO order_items VALUES (4, 2, 3, 1, 14.99)");
    await client.query("INSERT INTO order_items VALUES (5, 3, 3, 1, 14.99)");
    await client.query("INSERT INTO order_items VALUES (6, 4, 4, 1, 45.50)");
    await client.query("INSERT INTO order_items VALUES (7, 4, 5, 1, 129.99)");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('customer order history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT c.name, o.id AS order_id, o.total, o.status FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.name = 'Alice Johnson' ORDER BY o.id"
    );
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].status, 'completed');

    await client.end();
  });

  it('order details with products', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name AS product, oi.quantity, oi.price FROM order_items oi JOIN products p ON oi.product_id = p.id WHERE oi.order_id = 1'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('revenue by category', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.category, SUM(oi.price * oi.quantity) AS revenue FROM order_items oi JOIN products p ON oi.product_id = p.id GROUP BY p.category ORDER BY revenue DESC'
    );
    assert.ok(result.rows.length >= 2);
    // Electronics should be highest
    assert.strictEqual(result.rows[0].category, 'electronics');

    await client.end();
  });

  it('top customers by spend', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT c.name, SUM(o.total) AS total_spent, COUNT(o.id) AS order_count FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total_spent DESC'
    );
    assert.ok(result.rows.length >= 2);
    assert.strictEqual(result.rows[0].name, 'Alice Johnson'); // Highest spender

    await client.end();
  });

  it('best-selling products', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name, SUM(oi.quantity) AS total_sold FROM order_items oi JOIN products p ON oi.product_id = p.id GROUP BY p.name ORDER BY total_sold DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('low stock alert', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT name, stock FROM products WHERE stock < 100 ORDER BY stock ASC'
    );
    assert.ok(result.rows.length >= 2); // Laptop (50) and Running Shoes (75)

    await client.end();
  });

  it('order status summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT status, COUNT(*) AS cnt, SUM(total) AS total FROM orders GROUP BY status ORDER BY cnt DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('customers in same city', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT name, city FROM customers WHERE city = 'Denver'"
    );
    assert.strictEqual(result.rows.length, 2); // Alice and Charlie

    await client.end();
  });
});
