// e2e.test.js — End-to-end integration tests simulating real database usage
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('End-to-End: E-commerce Database', () => {
  function createEcommerce() {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, tier TEXT)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL, category TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, product_id INT, quantity INT, status TEXT)');
    
    // Insert data
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com', 'premium')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob', 'bob@test.com', 'standard')");
    db.execute("INSERT INTO customers VALUES (3, 'Charlie', 'charlie@test.com', 'premium')");
    
    db.execute("INSERT INTO products VALUES (1, 'Widget', 25, 'electronics')");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 50, 'electronics')");
    db.execute("INSERT INTO products VALUES (3, 'Book', 15, 'books')");
    db.execute("INSERT INTO products VALUES (4, 'Pen', 5, 'office')");
    
    db.execute("INSERT INTO orders VALUES (1, 1, 1, 3, 'completed')");
    db.execute("INSERT INTO orders VALUES (2, 1, 2, 1, 'completed')");
    db.execute("INSERT INTO orders VALUES (3, 2, 3, 2, 'pending')");
    db.execute("INSERT INTO orders VALUES (4, 2, 1, 1, 'completed')");
    db.execute("INSERT INTO orders VALUES (5, 3, 2, 5, 'pending')");
    db.execute("INSERT INTO orders VALUES (6, 3, 4, 10, 'completed')");
    
    return db;
  }

  it('JOIN with aggregation: total spend per customer', () => {
    const db = createEcommerce();
    const r = db.execute(`
      SELECT c.name, SUM(p.price * o.quantity) AS total_spend
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN products p ON o.product_id = p.id
      WHERE o.status = 'completed'
      GROUP BY c.name
      ORDER BY total_spend DESC
    `);
    assert.ok(r.rows.length >= 2);
    // Alice: 3*25 + 1*50 = 125, Bob: 1*25 = 25, Charlie: 10*5 = 50
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].total_spend, 125);
  });

  it('window function: rank products by price', () => {
    const db = createEcommerce();
    const r = db.execute('SELECT name, price, RANK() OVER (ORDER BY price DESC) AS price_rank FROM products');
    assert.equal(r.rows.length, 4);
    const gadget = r.rows.find(row => row.name === 'Gadget');
    assert.equal(gadget.price_rank, 1);
    const pen = r.rows.find(row => row.name === 'Pen');
    assert.equal(pen.price_rank, 4);
  });

  it('UPSERT: update on conflict', () => {
    const db = createEcommerce();
    // Try to insert duplicate customer, update tier instead
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'newemail@test.com', 'vip') ON CONFLICT (id) DO UPDATE SET tier = 'vip'");
    const r = db.execute("SELECT tier FROM customers WHERE id = 1");
    assert.equal(r.rows[0].tier, 'vip');
  });

  it('materialized view for analytics', () => {
    const db = createEcommerce();
    db.execute(`CREATE MATERIALIZED VIEW order_summary AS
      SELECT o.status, COUNT(*) AS cnt
      FROM orders o
      GROUP BY o.status
    `);
    const r = db.execute('SELECT * FROM order_summary ORDER BY status');
    assert.ok(r.rows.length >= 2);
    
    // Add new order and refresh
    db.execute("INSERT INTO orders VALUES (7, 1, 3, 1, 'cancelled')");
    db.execute('REFRESH MATERIALIZED VIEW order_summary');
    const r2 = db.execute('SELECT * FROM order_summary ORDER BY status');
    assert.ok(r2.rows.length >= 3); // Now includes 'cancelled'
  });

  it('prepared statement for repeated queries', () => {
    const db = createEcommerce();
    const stmt = db.prepare('SELECT name, price FROM products WHERE category = $1');
    
    const electronics = stmt.execute(['electronics']);
    assert.equal(electronics.rows.length, 2);
    
    const books = stmt.execute(['books']);
    assert.equal(books.rows.length, 1);
    assert.equal(books.rows[0].name, 'Book');
  });

  it('CTE: premium customer orders', () => {
    const db = createEcommerce();
    const r = db.execute("WITH premium AS (SELECT id, name FROM customers WHERE tier = 'premium') SELECT name, COUNT(*) AS orders FROM premium JOIN orders o ON premium.id = o.customer_id GROUP BY name");
    assert.ok(r.rows.length >= 2); // Alice and Charlie
  });

  it('RETURNING with INSERT', () => {
    const db = createEcommerce();
    const r = db.execute("INSERT INTO products VALUES (5, 'Tablet', 300, 'electronics') RETURNING *");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Tablet');
    assert.equal(r.rows[0].price, 300);
  });

  it('JSON data storage and querying', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO events VALUES (1, '{\"type\": \"click\", \"page\": \"/home\"}')");
    db.execute("INSERT INTO events VALUES (2, '{\"type\": \"view\", \"page\": \"/about\"}')");
    db.execute("INSERT INTO events VALUES (3, '{\"type\": \"click\", \"page\": \"/products\"}')");
    
    const r = db.execute("SELECT id, JSON_EXTRACT(data, '$.type') AS event_type FROM events");
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].event_type, 'click');
  });

  it('full-text search on product descriptions', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, description TEXT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget Pro', 'Professional grade widget for engineering')");
    db.execute("INSERT INTO products VALUES (2, 'Gadget X', 'Consumer gadget for entertainment')");
    db.execute("INSERT INTO products VALUES (3, 'Widget Basic', 'Basic widget for everyday use')");
    
    db.execute('CREATE FULLTEXT INDEX idx_desc ON products(description)');
    
    const r = db.execute("SELECT name FROM products WHERE MATCH(description) AGAINST('widget')");
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.some(row => row.name === 'Widget Pro'));
    assert.ok(r.rows.some(row => row.name === 'Widget Basic'));
  });

  it('GENERATE_SERIES for date-like sequences', () => {
    const db = new Database();
    const r = db.execute('SELECT value AS day_num FROM GENERATE_SERIES(1, 7)');
    assert.equal(r.rows.length, 7);
    assert.deepEqual(r.rows.map(r => r.day_num), [1, 2, 3, 4, 5, 6, 7]);
  });
});
