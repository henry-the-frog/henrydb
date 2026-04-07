// join-complex.test.js — Join and subquery tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Complex Joins and Subqueries', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999)");
    db.execute("INSERT INTO products VALUES (2, 'Phone', 'Electronics', 699)");
    db.execute("INSERT INTO products VALUES (3, 'Book', 'Education', 29)");
    db.execute("INSERT INTO products VALUES (4, 'Tablet', 'Electronics', 499)");
    db.execute("INSERT INTO products VALUES (5, 'Course', 'Education', 199)");

    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, customer TEXT, qty INT, amount INT)');
    db.execute("INSERT INTO orders VALUES (1, 1, 'Alice', 1, 999)");
    db.execute("INSERT INTO orders VALUES (2, 2, 'Bob', 2, 1398)");
    db.execute("INSERT INTO orders VALUES (3, 1, 'Charlie', 1, 999)");
    db.execute("INSERT INTO orders VALUES (4, 3, 'Alice', 3, 87)");
    db.execute("INSERT INTO orders VALUES (5, 4, 'Bob', 1, 499)");
    db.execute("INSERT INTO orders VALUES (6, 5, 'Alice', 1, 199)");
    db.execute("INSERT INTO orders VALUES (7, 2, 'Diana', 1, 699)");

    db.execute('CREATE TABLE reviews (id INT PRIMARY KEY, product_id INT, rating INT, reviewer TEXT)');
    db.execute("INSERT INTO reviews VALUES (1, 1, 5, 'Alice')");
    db.execute("INSERT INTO reviews VALUES (2, 1, 4, 'Bob')");
    db.execute("INSERT INTO reviews VALUES (3, 2, 3, 'Charlie')");
    db.execute("INSERT INTO reviews VALUES (4, 3, 5, 'Diana')");
  });

  describe('JOIN basics', () => {
    it('JOIN products and orders', () => {
      const result = db.execute('SELECT p.name, o.customer FROM products p JOIN orders o ON p.id = o.product_id ORDER BY o.id');
      assert.equal(result.rows.length, 7);
    });

    it('JOIN with GROUP BY and COUNT', () => {
      const result = db.execute('SELECT p.name, COUNT(*) AS order_count FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.name ORDER BY order_count DESC');
      assert.ok(result.rows.length > 0);
    });

    it('JOIN with GROUP BY and SUM', () => {
      const result = db.execute('SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer ORDER BY total DESC');
      assert.ok(result.rows.length > 0);
      const alice = result.rows.find(r => r.customer === 'Alice');
      assert.equal(alice.total, 999 + 87 + 199); // 1285
    });
  });

  describe('IN subquery', () => {
    it('products ordered by Alice', () => {
      const result = db.execute("SELECT name FROM products WHERE id IN (SELECT product_id FROM orders WHERE customer = 'Alice')");
      assert.equal(result.rows.length, 3);
      const names = result.rows.map(r => r.name).sort();
      assert.deepEqual(names, ['Book', 'Course', 'Laptop']);
    });

    it('NOT IN subquery', () => {
      const result = db.execute('SELECT name FROM products WHERE id NOT IN (SELECT product_id FROM orders)');
      assert.equal(result.rows.length, 0); // all products ordered
    });

    it('customers who ordered Electronics', () => {
      const result = db.execute("SELECT DISTINCT customer FROM orders WHERE product_id IN (SELECT id FROM products WHERE category = 'Electronics')");
      assert.ok(result.rows.length > 0);
      const customers = result.rows.map(r => r.customer).sort();
      assert.ok(customers.includes('Alice')); // ordered Laptop
      assert.ok(customers.includes('Bob'));   // ordered Phone, Tablet
    });
  });

  describe('Self-join', () => {
    it('products in same category (cheaper)', () => {
      const result = db.execute('SELECT p1.name AS expensive, p2.name AS cheaper FROM products p1 JOIN products p2 ON p1.category = p2.category AND p1.price > p2.price ORDER BY p1.name');
      assert.ok(result.rows.length > 0);
    });

    it('finds pairs', () => {
      const result = db.execute('SELECT p1.name, p2.name FROM products p1 JOIN products p2 ON p1.category = p2.category AND p1.id < p2.id');
      // Electronics: (1,2),(1,4),(2,4) = 3; Education: (3,5) = 1
      assert.equal(result.rows.length, 4);
    });
  });

  describe('LEFT JOIN', () => {
    it('LEFT JOIN shows all products', () => {
      const result = db.execute('SELECT p.name, r.rating FROM products p LEFT JOIN reviews r ON p.id = r.product_id ORDER BY p.name');
      assert.ok(result.rows.length >= 5);
    });

    it('products without reviews using IS NULL', () => {
      const result = db.execute('SELECT p.name FROM products p LEFT JOIN reviews r ON p.id = r.product_id WHERE r.id IS NULL');
      assert.equal(result.rows.length, 2); // Tablet, Course
    });
  });

  describe('Subquery in WHERE with comparison', () => {
    it('products above average price', () => {
      const result = db.execute('SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products)');
      // Avg: (999+699+29+499+199)/5 = 485
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows.every(r => r.price > 485));
    });

    it('most expensive product', () => {
      const result = db.execute('SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products)');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Laptop');
    });

    it('customers with multiple orders', () => {
      const result = db.execute('SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer ORDER BY cnt DESC');
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows[0].cnt >= 2);
    });
  });

  describe('Three-table join', () => {
    it('joins products, orders, and reviews', () => {
      const result = db.execute(`
        SELECT p.name, o.customer, r.rating
        FROM products p
        JOIN orders o ON p.id = o.product_id
        JOIN reviews r ON p.id = r.product_id
        ORDER BY p.name
      `);
      assert.ok(result.rows.length > 0);
    });
  });

  describe('Edge cases', () => {
    it('JOIN with no matching rows', () => {
      db.execute('CREATE TABLE empty_table (id INT PRIMARY KEY, val TEXT)');
      const result = db.execute('SELECT p.name FROM products p JOIN empty_table e ON p.id = e.id');
      assert.equal(result.rows.length, 0);
    });

    it('subquery returning empty set', () => {
      const result = db.execute("SELECT name FROM products WHERE id IN (SELECT product_id FROM orders WHERE customer = 'Nobody')");
      assert.equal(result.rows.length, 0);
    });

    it('multiple conditions in JOIN', () => {
      const result = db.execute("SELECT o.customer, r.rating FROM orders o JOIN reviews r ON o.product_id = r.product_id AND o.customer = r.reviewer");
      // Alice ordered product 1, reviewed product 1 → match
      assert.ok(result.rows.length >= 1);
    });
  });
});
