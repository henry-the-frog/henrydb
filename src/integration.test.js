// integration.test.js — End-to-end SQL feature integration tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Integration Tests', () => {
  it('E-commerce schema with all features', () => {
    const db = new Database();
    
    // Schema with constraints
    db.execute('CREATE TABLE categories (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, category_id INT REFERENCES categories(id), price INT CHECK (price > 0))');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE CASCADE, total INT DEFAULT 0)');
    db.execute('CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT REFERENCES orders(id) ON DELETE CASCADE, product_id INT REFERENCES products(id), quantity INT CHECK (quantity > 0), subtotal INT)');

    // Data
    db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
    db.execute("INSERT INTO categories VALUES (2, 'Books')");
    db.execute("INSERT INTO categories VALUES (3, 'Clothing')");

    db.execute("INSERT INTO products VALUES (1, 'Laptop', 1, 999)");
    db.execute("INSERT INTO products VALUES (2, 'Phone', 1, 699)");
    db.execute("INSERT INTO products VALUES (3, 'SQL Book', 2, 49)");
    db.execute("INSERT INTO products VALUES (4, 'T-Shirt', 3, 25)");
    db.execute("INSERT INTO products VALUES (5, 'Jacket', 3, 89)");

    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob', 'bob@test.com')");
    db.execute("INSERT INTO customers VALUES (3, 'Carol', 'carol@test.com')");

    db.execute("INSERT INTO orders VALUES (1, 1, 0)");
    db.execute("INSERT INTO orders VALUES (2, 1, 0)");
    db.execute("INSERT INTO orders VALUES (3, 2, 0)");

    db.execute('INSERT INTO order_items VALUES (1, 1, 1, 1, 999)');
    db.execute('INSERT INTO order_items VALUES (2, 1, 3, 2, 98)');
    db.execute('INSERT INTO order_items VALUES (3, 2, 2, 1, 699)');
    db.execute('INSERT INTO order_items VALUES (4, 3, 4, 3, 75)');
    db.execute('INSERT INTO order_items VALUES (5, 3, 5, 1, 89)');

    // Test 1: JOIN + aggregate + GROUP BY
    const r1 = db.execute(`
      SELECT c.name, COUNT(*) AS order_count
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      ORDER BY order_count DESC
    `);
    assert.equal(r1.rows[0].name, 'Alice');
    assert.equal(r1.rows[0].order_count, 2);

    // Test 2: Subquery in WHERE
    const r2 = db.execute(`
      SELECT name, price FROM products
      WHERE price > (SELECT AVG(price) FROM products)
      ORDER BY price DESC
    `);
    assert.ok(r2.rows.length > 0);
    assert.ok(r2.rows.every(r => r.price > 372)); // avg = (999+699+49+25+89)/5 = 372.2

    // Test 3: Window function
    const r3 = db.execute(`
      SELECT name, price,
        ROW_NUMBER() OVER (ORDER BY price DESC) AS rank
      FROM products
    `);
    assert.equal(r3.rows.length, 5);

    // Test 4: CTE
    const r4 = db.execute(`
      WITH expensive AS (
        SELECT * FROM products WHERE price > 100
      )
      SELECT name FROM expensive ORDER BY price DESC
    `);
    assert.equal(r4.rows.length, 2); // Laptop and Phone

    // Test 5: CASCADE delete
    db.execute('DELETE FROM customers WHERE id = 2'); // Should cascade to orders and order_items
    const remaining = db.execute('SELECT * FROM order_items');
    assert.ok(remaining.rows.every(r => r.order_id !== 3)); // Bob's items gone

    // Test 6: CHECK constraint
    assert.throws(() => db.execute('INSERT INTO products VALUES (6, \'Free\', 1, 0)'), /CHECK/);

    // Test 7: FK constraint
    assert.throws(() => db.execute('INSERT INTO products VALUES (7, \'Ghost\', 99, 10)'), /Foreign key/);

    // Test 8: EXPLAIN ANALYZE
    const r8 = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE id = 1');
    assert.equal(r8.type, 'ANALYZE');
    assert.equal(r8.actual_rows, 1);
  });

  it('Analytics query with window + CTE + aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product TEXT, amount INT, sale_date TEXT)');
    for (let i = 1; i <= 100; i++) {
      const region = ['East', 'West', 'North', 'South'][i % 4];
      const product = ['Widget', 'Gadget', 'Doohickey'][i % 3];
      db.execute(`INSERT INTO sales VALUES (${i}, '${region}', '${product}', ${100 + i * 7}, '2024-0${1 + (i % 3)}-${10 + (i % 20)}')`);
    }

    // Region totals
    const r1 = db.execute(`
      SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
      FROM sales
      GROUP BY region
      ORDER BY total DESC
    `);
    assert.equal(r1.rows.length, 4);

    // Running sum per region
    const r2 = db.execute(`
      SELECT region, amount,
        SUM(amount) OVER (PARTITION BY region ORDER BY amount) AS running_total
      FROM sales
      WHERE region = 'East'
    `);
    assert.ok(r2.rows.length > 0);

    // Top products
    const r3 = db.execute(`
      WITH product_totals AS (
        SELECT product, SUM(amount) AS total
        FROM sales
        GROUP BY product
      )
      SELECT product, total FROM product_totals ORDER BY total DESC
    `);
    assert.equal(r3.rows.length, 3);
  });

  it('Recursive CTE for graph traversal', () => {
    const db = new Database();
    db.execute('CREATE TABLE graph (id INT PRIMARY KEY, src INT, dst INT, weight INT)');
    // Simple graph: 1→2→3→4
    db.execute('INSERT INTO graph VALUES (1, 1, 2, 10)');
    db.execute('INSERT INTO graph VALUES (2, 2, 3, 20)');
    db.execute('INSERT INTO graph VALUES (3, 3, 4, 30)');
    db.execute('INSERT INTO graph VALUES (4, 1, 3, 50)'); // Shortcut

    const r = db.execute(`
      WITH RECURSIVE reachable AS (
        SELECT dst AS node FROM graph WHERE src = 1
        UNION ALL
        SELECT g.dst FROM graph g JOIN reachable r ON g.src = r.node
      )
      SELECT DISTINCT node FROM reachable
    `);
    // Should find nodes 2, 3, 4 (all reachable from 1)
    assert.ok(r.rows.length >= 3);
  });
});
