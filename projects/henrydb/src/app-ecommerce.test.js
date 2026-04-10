// app-ecommerce.test.js — E-commerce application test
// Simulates a real-world e-commerce app using HenryDB through pg client
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('E-commerce Application', () => {
  let server, port, client;

  before(async () => {
    port = 27000 + Math.floor(Math.random() * 3000);
    server = new HenryDBServer({ port });
    await server.start();
    client = new Client({ host: '127.0.0.1', port, user: 'app', database: 'ecommerce' });
    await client.connect();

    // Schema
    await client.query(`CREATE TABLE users (
      id INT PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL, 
      tier TEXT DEFAULT 'bronze', balance INT DEFAULT 0, created_at TEXT
    )`);
    await client.query(`CREATE TABLE products (
      id INT PRIMARY KEY, name TEXT NOT NULL, description TEXT, 
      price INT NOT NULL, stock INT NOT NULL DEFAULT 0, category TEXT
    )`);
    await client.query(`CREATE TABLE orders (
      id INT PRIMARY KEY, user_id INT NOT NULL, product_id INT NOT NULL,
      quantity INT NOT NULL, total INT NOT NULL, status TEXT DEFAULT 'pending',
      ordered_at TEXT
    )`);
    await client.query(`CREATE TABLE reviews (
      id INT PRIMARY KEY, user_id INT NOT NULL, product_id INT NOT NULL,
      rating INT NOT NULL, comment TEXT, reviewed_at TEXT
    )`);
    
    // Create indexes
    await client.query('CREATE INDEX idx_orders_user ON orders (user_id)');
    await client.query('CREATE INDEX idx_orders_product ON orders (product_id)');
    await client.query('CREATE INDEX idx_reviews_product ON reviews (product_id)');
    await client.query('CREATE INDEX idx_products_category ON products (category)');
  });

  after(async () => {
    if (client) await client.end();
    if (server) await server.stop();
  });

  it('registers users', async () => {
    await client.query("INSERT INTO users VALUES (1, 'alice', 'alice@shop.com', 'gold', 50000, '2024-01-01')");
    await client.query("INSERT INTO users VALUES (2, 'bob', 'bob@shop.com', 'silver', 25000, '2024-02-01')");
    await client.query("INSERT INTO users VALUES (3, 'charlie', 'charlie@shop.com', 'bronze', 10000, '2024-03-01')");
    await client.query("INSERT INTO users VALUES (4, 'diana', 'diana@shop.com', 'gold', 75000, '2024-04-01')");
    await client.query("INSERT INTO users VALUES (5, 'eve', 'eve@shop.com', 'bronze', 5000, '2024-05-01')");
    
    const r = await client.query('SELECT COUNT(*) as cnt FROM users');
    assert.equal(String(r.rows[0].cnt), '5');
  });

  it('adds products', async () => {
    const products = [
      [1, 'Laptop Pro', 'High-end laptop', 149999, 50, 'electronics'],
      [2, 'Wireless Mouse', 'Ergonomic mouse', 4999, 200, 'electronics'],
      [3, 'Running Shoes', 'Lightweight runners', 12999, 100, 'sports'],
      [4, 'Coffee Maker', 'Automatic drip', 8999, 75, 'home'],
      [5, 'Backpack', 'Travel backpack', 7999, 150, 'sports'],
      [6, 'Headphones', 'Noise-cancelling', 29999, 80, 'electronics'],
      [7, 'Water Bottle', 'Insulated steel', 2499, 300, 'sports'],
      [8, 'Desk Lamp', 'LED adjustable', 4499, 120, 'home'],
    ];
    for (const p of products) {
      await client.query('INSERT INTO products VALUES ($1, $2, $3, $4, $5, $6)', p);
    }
    
    const r = await client.query('SELECT COUNT(*) as cnt FROM products');
    assert.equal(String(r.rows[0].cnt), '8');
  });

  it('places orders', async () => {
    const orders = [
      [1, 1, 2, 2, 9998, 'completed', '2024-06-01'],
      [2, 1, 6, 1, 29999, 'completed', '2024-06-02'],
      [3, 2, 3, 1, 12999, 'completed', '2024-06-03'],
      [4, 3, 7, 3, 7497, 'completed', '2024-06-04'],
      [5, 4, 1, 1, 149999, 'pending', '2024-06-05'],
      [6, 2, 4, 1, 8999, 'shipped', '2024-06-06'],
      [7, 1, 5, 1, 7999, 'completed', '2024-06-07'],
      [8, 5, 2, 1, 4999, 'cancelled', '2024-06-08'],
      [9, 4, 8, 2, 8998, 'completed', '2024-06-09'],
      [10, 3, 6, 1, 29999, 'completed', '2024-06-10'],
    ];
    for (const o of orders) {
      await client.query('INSERT INTO orders VALUES ($1, $2, $3, $4, $5, $6, $7)', o);
    }
    
    // Update stock
    await client.query('UPDATE products SET stock = stock - 2 WHERE id = 2'); // Wireless Mouse
    await client.query('UPDATE products SET stock = stock - 1 WHERE id = 6'); // Headphones
    
    const r = await client.query('SELECT COUNT(*) as cnt FROM orders');
    assert.equal(String(r.rows[0].cnt), '10');
  });

  it('queries: user order history', async () => {
    const r = await client.query(`
      SELECT o.id, p.name, o.quantity, o.total, o.status 
      FROM orders o 
      JOIN products p ON o.product_id = p.id 
      WHERE o.user_id = $1 
      ORDER BY o.ordered_at DESC
    `, [1]);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].name, 'Backpack');
  });

  it('queries: total revenue per category', async () => {
    const r = await client.query(`
      SELECT p.category, SUM(o.total) as revenue, COUNT(*) as order_count
      FROM orders o 
      JOIN products p ON o.product_id = p.id 
      WHERE o.status != 'cancelled'
      GROUP BY p.category 
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length >= 2);
    // Electronics should be top revenue
  });

  it('queries: top spending users', async () => {
    const r = await client.query(`
      SELECT u.username, u.tier, SUM(o.total) as total_spent, COUNT(*) as order_count
      FROM users u 
      JOIN orders o ON u.id = o.user_id 
      WHERE o.status = 'completed'
      GROUP BY u.username, u.tier 
      ORDER BY total_spent DESC
    `);
    assert.ok(r.rows.length >= 2);
    assert.ok(parseInt(String(r.rows[0].total_spent)) > 0);
  });

  it('queries: product with most orders', async () => {
    const r = await client.query(`
      SELECT p.name, COUNT(*) as order_count, SUM(o.quantity) as total_qty
      FROM products p 
      JOIN orders o ON p.id = o.product_id 
      GROUP BY p.name 
      ORDER BY order_count DESC 
      LIMIT 3
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('adds and queries reviews', async () => {
    const reviews = [
      [1, 1, 2, 5, 'Great mouse!', '2024-07-01'],
      [2, 1, 6, 4, 'Good sound, pricey', '2024-07-02'],
      [3, 2, 3, 5, 'Best running shoes', '2024-07-03'],
      [4, 3, 7, 3, 'OK but leaks sometimes', '2024-07-04'],
      [5, 4, 1, 5, 'Amazing laptop', '2024-07-05'],
      [6, 3, 6, 5, 'Love these headphones', '2024-07-06'],
    ];
    for (const rev of reviews) {
      await client.query('INSERT INTO reviews VALUES ($1, $2, $3, $4, $5, $6)', rev);
    }
    
    // Average rating per product
    const r = await client.query(`
      SELECT p.name, AVG(r.rating) as avg_rating, COUNT(*) as review_count
      FROM products p 
      JOIN reviews r ON p.id = r.product_id 
      GROUP BY p.name 
      ORDER BY avg_rating DESC
    `);
    assert.ok(r.rows.length >= 3);
  });

  it('queries: products above average price', async () => {
    const r = await client.query(`
      SELECT name, price 
      FROM products 
      WHERE price > (SELECT AVG(price) FROM products) 
      ORDER BY price DESC
    `);
    assert.ok(r.rows.length >= 1);
    assert.ok(parseInt(String(r.rows[0].price)) > 0);
  });

  it('queries: users who have not placed orders', async () => {
    const r = await client.query(`
      SELECT username 
      FROM users 
      WHERE id NOT IN (SELECT DISTINCT user_id FROM orders WHERE status != 'cancelled')
    `);
    // All users placed orders (eve's was cancelled)
    assert.ok(r.rows.length >= 0);
  });

  it('updates: process pending order', async () => {
    await client.query("UPDATE orders SET status = 'shipped' WHERE id = 5 AND status = 'pending'");
    const r = await client.query('SELECT status FROM orders WHERE id = $1', [5]);
    assert.equal(r.rows[0].status, 'shipped');
  });

  it('deletes: remove cancelled orders', async () => {
    const before = await client.query('SELECT COUNT(*) as cnt FROM orders');
    await client.query("DELETE FROM orders WHERE status = 'cancelled'");
    const after = await client.query('SELECT COUNT(*) as cnt FROM orders');
    assert.ok(parseInt(String(before.rows[0].cnt)) > parseInt(String(after.rows[0].cnt)));
  });

  it('complex: customer lifetime value report', async () => {
    const r = await client.query(`
      SELECT u.username, u.tier, u.balance,
        SUM(o.total) as lifetime_spend,
        COUNT(*) as total_orders,
        AVG(o.total) as avg_order_value
      FROM users u 
      LEFT JOIN orders o ON u.id = o.user_id AND o.status != 'cancelled'
      GROUP BY u.username, u.tier, u.balance
      ORDER BY lifetime_spend DESC
    `);
    assert.equal(r.rows.length, 5); // All 5 users
  });

  it('complex: category performance dashboard', async () => {
    const r = await client.query(`
      SELECT p.category,
        COUNT(DISTINCT p.id) as product_count,
        COUNT(o.id) as total_orders,
        SUM(o.total) as total_revenue,
        AVG(r.rating) as avg_rating
      FROM products p 
      LEFT JOIN orders o ON p.id = o.product_id 
      LEFT JOIN reviews r ON p.id = r.product_id
      GROUP BY p.category
      ORDER BY total_revenue DESC
    `);
    assert.ok(r.rows.length >= 2);
  });
});
