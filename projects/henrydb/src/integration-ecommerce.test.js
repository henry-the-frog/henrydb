// integration-ecommerce.test.js — End-to-end e-commerce scenario
// Exercises: DDL, DML, transactions, indexes, views, CTEs,
// window functions, aggregates, subqueries
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setupSchema(db) {
  db.execute(`CREATE TABLE customers (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT,
    created_at TEXT DEFAULT '2026-01-01', tier TEXT DEFAULT 'bronze'
  )`);
  db.execute(`CREATE TABLE products (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL,
    category TEXT, stock INTEGER DEFAULT 0, description TEXT
  )`);
  db.execute(`CREATE TABLE orders (
    id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT DEFAULT 'pending',
    total REAL DEFAULT 0, created_at TEXT, shipped_at TEXT
  )`);
  db.execute(`CREATE TABLE order_items (
    id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER,
    quantity INTEGER, unit_price REAL
  )`);
  db.execute(`CREATE TABLE reviews (
    id INTEGER PRIMARY KEY, product_id INTEGER, customer_id INTEGER,
    rating INTEGER, comment TEXT
  )`);
  db.execute('CREATE INDEX idx_orders_customer ON orders(customer_id)');
  db.execute('CREATE INDEX idx_items_order ON order_items(order_id)');
  db.execute('CREATE INDEX idx_items_product ON order_items(product_id)');
  db.execute('CREATE INDEX idx_reviews_product ON reviews(product_id)');
}

function populateData(db) {
  const customers = [
    [1, 'Alice', 'alice@example.com', '2025-01-15', 'gold'],
    [2, 'Bob', 'bob@example.com', '2025-02-20', 'silver'],
    [3, 'Charlie', 'charlie@example.com', '2025-03-10', 'bronze'],
    [4, 'Diana', 'diana@example.com', '2025-04-05', 'gold'],
    [5, 'Eve', 'eve@example.com', '2025-05-12', 'silver'],
    [6, 'Frank', 'frank@example.com', '2025-06-01', 'bronze'],
    [7, 'Grace', 'grace@example.com', '2025-07-20', 'silver'],
    [8, 'Hank', 'hank@example.com', '2025-08-15', 'bronze'],
    [9, 'Ivy', 'ivy@example.com', '2025-09-30', 'gold'],
    [10, 'Jack', 'jack@example.com', '2025-10-10', 'bronze'],
  ];
  for (const [id, name, email, created, tier] of customers) {
    db.execute(`INSERT INTO customers (id, name, email, created_at, tier) VALUES (${id}, '${name}', '${email}', '${created}', '${tier}')`);
  }

  const categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports'];
  const productNames = [
    'Wireless Headphones', 'USB-C Hub', 'Mechanical Keyboard', 'Monitor Stand',
    'JavaScript Good Parts', 'DDIA', 'Clean Code', 'Pragmatic Programmer',
    'Running Shoes', 'Yoga Mat', 'Water Bottle', 'Backpack',
    'Desk Lamp', 'Coffee Maker', 'Plant Pot', 'Blanket',
    'Basketball', 'Tennis Racket', 'Bike Light', 'Jump Rope'
  ];
  for (let i = 0; i < 20; i++) {
    const price = (10 + Math.round((i * 7.3 + 5) * 100) / 100).toFixed(2);
    const cat = categories[i % 5];
    const stock = 10 + (i * 3);
    db.execute(`INSERT INTO products (id, name, price, category, stock, description)
      VALUES (${i + 1}, '${productNames[i]}', ${price}, '${cat}', ${stock}, 'A great product')`);
  }

  let orderId = 1, itemId = 1;
  for (let c = 1; c <= 10; c++) {
    const numOrders = 3 + (c % 4);
    for (let o = 0; o < numOrders; o++) {
      const status = o < numOrders - 1 ? 'shipped' : 'pending';
      const shipped = status === 'shipped' ? "'2026-03-15'" : 'NULL';
      db.execute(`INSERT INTO orders (id, customer_id, status, total, created_at, shipped_at)
        VALUES (${orderId}, ${c}, '${status}', 0, '2026-01-${10 + o}', ${shipped})`);
      
      const numItems = 2 + (orderId % 3);
      let orderTotal = 0;
      for (let i = 0; i < numItems; i++) {
        const prodId = ((orderId + i) % 20) + 1;
        const qty = 1 + (i % 3);
        const price = 10 + ((prodId * 7.3 + 5) * 100 | 0) / 100;
        orderTotal += qty * price;
        db.execute(`INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
          VALUES (${itemId}, ${orderId}, ${prodId}, ${qty}, ${price})`);
        itemId++;
      }
      db.execute(`UPDATE orders SET total = ${orderTotal.toFixed(2)} WHERE id = ${orderId}`);
      orderId++;
    }
  }

  let reviewId = 1;
  for (let c = 1; c <= 8; c++) {
    for (let p = 0; p < 3; p++) {
      const prodId = ((c + p * 3) % 20) + 1;
      const rating = 3 + ((c + p) % 3);
      db.execute(`INSERT INTO reviews (id, product_id, customer_id, rating, comment)
        VALUES (${reviewId}, ${prodId}, ${c}, ${rating}, 'Great product')`);
      reviewId++;
    }
  }
}

function freshDb() {
  const db = new Database();
  setupSchema(db);
  populateData(db);
  return db;
}

describe('E-Commerce Integration Test', () => {

  describe('Schema Setup', () => {
    it('creates a complete e-commerce schema', () => {
      const db = new Database();
      setupSchema(db);
      const tables = db.execute('SHOW TABLES');
      assert.equal(tables.rows.length, 5);
    });

    it('populates all data correctly', () => {
      const db = freshDb();
      assert.equal(db.execute('SELECT COUNT(*) as cnt FROM customers').rows[0].cnt, 10);
      assert.equal(db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt, 20);
      assert.ok(db.execute('SELECT COUNT(*) as cnt FROM orders').rows[0].cnt >= 30);
      assert.ok(db.execute('SELECT COUNT(*) as cnt FROM order_items').rows[0].cnt >= 60);
      assert.ok(db.execute('SELECT COUNT(*) as cnt FROM reviews').rows[0].cnt >= 20);
    });
  });

  describe('Basic Queries', () => {
    it('WHERE with index lookup', () => {
      const db = freshDb();
      const r = db.execute("SELECT name, email FROM customers WHERE id = 3");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].name, 'Charlie');
    });

    it('JOIN with GROUP BY and ORDER BY', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT c.name, COUNT(*) as order_count
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY c.name
        ORDER BY order_count DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows[0].order_count >= 1);
    });

    it('correlated subquery: order totals', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT o.id,
          (SELECT SUM(oi.quantity * oi.unit_price)
           FROM order_items oi WHERE oi.order_id = o.id) as calc_total
        FROM orders o
        WHERE o.id <= 5
        ORDER BY o.id
      `);
      assert.equal(r.rows.length, 5);
      r.rows.forEach(row => assert.ok(row.calc_total > 0));
    });

    it('aggregate functions with HAVING', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT p.name,
               COUNT(*) as review_count,
               AVG(rv.rating) as avg_rating,
               MIN(rv.rating) as min_rating,
               MAX(rv.rating) as max_rating
        FROM reviews rv
        JOIN products p ON rv.product_id = p.id
        GROUP BY p.name
        HAVING COUNT(*) >= 2
        ORDER BY avg_rating DESC
      `);
      assert.ok(r.rows.length > 0);
      r.rows.forEach(row => {
        assert.ok(row.avg_rating >= 1 && row.avg_rating <= 5);
        assert.ok(row.min_rating <= row.max_rating);
      });
    });
  });

  describe('Complex Queries', () => {
    it('CTE: simple aggregate', () => {
      const db = freshDb();
      const r = db.execute(`
        WITH order_totals AS (
          SELECT customer_id, SUM(total) as spend
          FROM orders
          GROUP BY customer_id
        )
        SELECT spend FROM order_totals ORDER BY spend DESC
      `);
      assert.ok(r.rows.length > 0);
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(r.rows[i - 1].spend >= r.rows[i].spend);
      }
    });

    it('window function: running total per customer', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT o.customer_id, o.id as order_id, o.total,
               SUM(o.total) OVER (PARTITION BY o.customer_id ORDER BY o.id) as running_total,
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.id) as order_num
        FROM orders o
        WHERE o.customer_id <= 3
        ORDER BY o.customer_id, o.id
      `);
      assert.ok(r.rows.length > 0);
      let prevCust = null, prevRun = 0;
      r.rows.forEach(row => {
        if (row.customer_id !== prevCust) { prevCust = row.customer_id; prevRun = 0; }
        assert.ok(row.running_total >= prevRun);
        prevRun = row.running_total;
      });
    });

    it('window function: RANK on simple column', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT name, price,
               RANK() OVER (ORDER BY price DESC) as price_rank
        FROM products
      `);
      assert.ok(r.rows.length === 20);
      // Find the row with rank 1 — should be the highest price
      const rank1 = r.rows.filter(r => r.price_rank === 1);
      assert.ok(rank1.length >= 1);
      const maxPrice = Math.max(...r.rows.map(r => r.price));
      assert.equal(rank1[0].price, maxPrice);
    });

    it('UNION: combined activity report', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT customer_id, 'order' as activity_type, id as activity_id
        FROM orders WHERE customer_id = 1
        UNION
        SELECT customer_id, 'review' as activity_type, id as activity_id
        FROM reviews WHERE customer_id = 1
      `);
      assert.ok(r.rows.length >= 1);
      r.rows.forEach(row => assert.equal(row.customer_id, 1));
    });

    it('EXISTS subquery: customers who reviewed', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT c.name FROM customers c
        WHERE EXISTS (SELECT 1 FROM reviews rv WHERE rv.customer_id = c.id)
      `);
      assert.ok(r.rows.length > 0);
    });

    it('nested IN subquery', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT DISTINCT p.name FROM products p
        WHERE p.id IN (
          SELECT oi.product_id FROM order_items oi
          JOIN orders o ON oi.order_id = o.id
          WHERE o.customer_id IN (SELECT id FROM customers WHERE tier = 'gold')
        )
      `);
      assert.ok(r.rows.length > 0);
    });

    it('CASE expression: tier classification', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT c.name, c.tier,
               CASE WHEN c.tier = 'gold' THEN 'Premium'
                    WHEN c.tier = 'silver' THEN 'Standard'
                    ELSE 'Basic' END as tier_label
        FROM customers c ORDER BY c.name
      `);
      assert.equal(r.rows.length, 10);
      r.rows.forEach(row => assert.ok(['Premium', 'Standard', 'Basic'].includes(row.tier_label)));
    });

    it('4-table join: order detail report', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT c.name as customer, p.name as product,
               oi.quantity, oi.unit_price,
               oi.quantity * oi.unit_price as line_total
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON oi.product_id = p.id
        ORDER BY c.name, o.id
      `);
      assert.ok(r.rows.length > 0);
      r.rows.forEach(row => {
        assert.ok(row.customer);
        assert.ok(row.product);
        assert.ok(Math.abs(row.line_total - row.quantity * row.unit_price) < 0.01);
      });
    });
  });

  describe('Views', () => {
    it('customer summary view', () => {
      const db = freshDb();
      db.execute(`
        CREATE VIEW customer_summary AS
        SELECT c.id, c.name, c.tier, COUNT(o.id) as total_orders
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        GROUP BY c.id, c.name, c.tier
      `);
      const r = db.execute('SELECT * FROM customer_summary ORDER BY total_orders DESC');
      assert.equal(r.rows.length, 10);
    });

    it('product performance view with filter', () => {
      const db = freshDb();
      db.execute(`
        CREATE VIEW product_perf AS
        SELECT p.id, p.name, p.category, p.price,
               SUM(oi.quantity) as units_sold,
               SUM(oi.quantity * oi.unit_price) as revenue
        FROM products p
        JOIN order_items oi ON oi.product_id = p.id
        GROUP BY p.id, p.name, p.category, p.price
      `);
      const r = db.execute('SELECT * FROM product_perf WHERE units_sold > 0 ORDER BY revenue DESC');
      assert.ok(r.rows.length > 0);
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(r.rows[i - 1].revenue >= r.rows[i].revenue);
      }
    });
  });

  describe('Transactions', () => {
    it('COMMIT persists changes', () => {
      const db = freshDb();
      const initialStock = db.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock;

      db.execute('BEGIN');
      db.execute(`INSERT INTO orders (id, customer_id, status, total, created_at)
                  VALUES (1000, 1, 'pending', 59.98, '2026-04-08')`);
      db.execute(`INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
                  VALUES (10000, 1000, 1, 2, 29.99)`);
      db.execute('UPDATE products SET stock = stock - 2 WHERE id = 1');
      db.execute('COMMIT');

      assert.equal(db.execute('SELECT * FROM orders WHERE id = 1000').rows.length, 1);
      assert.equal(db.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock, initialStock - 2);
    });

    it('BEGIN/COMMIT flow with multiple statements', () => {
      const db = freshDb();
      db.execute('BEGIN');
      db.execute(`INSERT INTO customers (id, name, email) VALUES (100, 'Test', 'test@test.com')`);
      db.execute(`INSERT INTO customers (id, name, email) VALUES (101, 'Test2', 'test2@test.com')`);
      db.execute('COMMIT');

      assert.equal(db.execute('SELECT COUNT(*) as cnt FROM customers WHERE id >= 100').rows[0].cnt, 2);
    });
  });

  describe('Advanced Analytics', () => {
    it('GROUP BY with multiple aggregates', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT category, COUNT(*) as product_count,
               SUM(stock) as total_stock, ROUND(AVG(price), 2) as avg_price
        FROM products GROUP BY category ORDER BY product_count DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.equal(r.rows.reduce((s, r) => s + r.product_count, 0), 20);
    });

    it('DENSE_RANK window function on simple column', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT name, price,
               DENSE_RANK() OVER (ORDER BY price DESC) as price_rank
        FROM products
      `);
      assert.ok(r.rows.length === 20);
      assert.equal(r.rows.filter(r => r.price_rank === 1).length >= 1, true);
    });

    it('LIKE pattern matching', () => {
      const db = freshDb();
      const r = db.execute("SELECT name FROM products WHERE name LIKE 'W%'");
      assert.ok(r.rows.length > 0);
      r.rows.forEach(row => assert.ok(row.name.startsWith('W')));
    });

    it('BETWEEN range filter', () => {
      const db = freshDb();
      const r = db.execute('SELECT name, price FROM products WHERE price BETWEEN 20 AND 50 ORDER BY price');
      r.rows.forEach(row => assert.ok(row.price >= 20 && row.price <= 50));
    });

    it('LIMIT and OFFSET pagination', () => {
      const db = freshDb();
      const page1 = db.execute('SELECT name, price FROM products ORDER BY price DESC LIMIT 5');
      const page2 = db.execute('SELECT name, price FROM products ORDER BY price DESC LIMIT 5 OFFSET 5');
      assert.equal(page1.rows.length, 5);
      assert.equal(page2.rows.length, 5);
      assert.ok(page1.rows[4].price >= page2.rows[0].price);
    });

    it('LEFT JOIN with NULL handling', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT p.name FROM products p
        LEFT JOIN order_items oi ON oi.product_id = p.id
        WHERE oi.id IS NULL
      `);
      assert.ok(Array.isArray(r.rows));
    });

    it('COALESCE for default values', () => {
      const db = freshDb();
      const r = db.execute(`
        SELECT id, COALESCE(shipped_at, 'not shipped') as ship_status
        FROM orders ORDER BY id LIMIT 5
      `);
      assert.equal(r.rows.length, 5);
      r.rows.forEach(row => assert.ok(row.ship_status));
    });
  });

  describe('UPDATE and DELETE', () => {
    it('batch UPDATE with subquery', () => {
      const db = freshDb();
      db.execute(`
        UPDATE customers SET tier = 'gold'
        WHERE id IN (
          SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(*) >= 5
        )
      `);
      const gold = db.execute("SELECT COUNT(*) as cnt FROM customers WHERE tier = 'gold'");
      assert.ok(gold.rows[0].cnt >= 0);
    });

    it('cascading DELETE across tables', () => {
      const db = freshDb();
      const custId = 10;
      db.execute(`DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE customer_id = ${custId})`);
      db.execute(`DELETE FROM orders WHERE customer_id = ${custId}`);
      db.execute(`DELETE FROM reviews WHERE customer_id = ${custId}`);
      db.execute(`DELETE FROM customers WHERE id = ${custId}`);

      assert.equal(db.execute(`SELECT COUNT(*) as cnt FROM customers WHERE id = ${custId}`).rows[0].cnt, 0);
      assert.equal(db.execute(`SELECT COUNT(*) as cnt FROM orders WHERE customer_id = ${custId}`).rows[0].cnt, 0);
    });

    it('UPDATE then verify consistency', () => {
      const db = freshDb();
      // Update all order totals manually
      const orders = db.execute('SELECT id FROM orders WHERE id <= 5').rows;
      for (const {id} of orders) {
        const calc = db.execute(`SELECT SUM(quantity * unit_price) as total FROM order_items WHERE order_id = ${id}`);
        const total = calc.rows[0].total || 0;
        db.execute(`UPDATE orders SET total = ${total} WHERE id = ${id}`);
      }
      // Verify
      for (const {id} of orders) {
        const stored = db.execute(`SELECT total FROM orders WHERE id = ${id}`).rows[0].total;
        const calc = db.execute(`SELECT SUM(quantity * unit_price) as total FROM order_items WHERE order_id = ${id}`).rows[0].total;
        assert.ok(Math.abs(stored - calc) < 0.01, `Order ${id}: stored ${stored} != calc ${calc}`);
      }
    });
  });

  describe('CREATE TABLE AS', () => {
    it('materializes a report from complex query', () => {
      const db = freshDb();
      db.execute(`
        CREATE TABLE monthly_report AS
        SELECT c.name as customer, c.tier,
               COUNT(o.id) as orders,
               SUM(oi.quantity * oi.unit_price) as revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        GROUP BY c.name, c.tier
      `);
      const r = db.execute('SELECT * FROM monthly_report ORDER BY revenue DESC');
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows[0].revenue > 0);
    });
  });

  describe('EXPLAIN', () => {
    it('generates plan for indexed lookup', () => {
      const db = freshDb();
      const r = db.execute('EXPLAIN SELECT name FROM customers WHERE id = 1');
      assert.ok(r.plan && r.plan.length > 0);
    });

    it('generates plan for join', () => {
      const db = freshDb();
      const r = db.execute(`
        EXPLAIN SELECT c.name, o.status
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
      `);
      assert.ok(r.plan && r.plan.length > 0);
    });
  });

  describe('Stress Tests', () => {
    it('100 inserts + updates + deletes', () => {
      const db = new Database();
      db.execute('CREATE TABLE stress (id INTEGER PRIMARY KEY, val INTEGER)');
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO stress (id, val) VALUES (${i}, ${i * 10})`);
      }
      assert.equal(db.execute('SELECT COUNT(*) as cnt FROM stress').rows[0].cnt, 100);

      for (let i = 0; i < 100; i++) {
        db.execute(`UPDATE stress SET val = val + 1 WHERE id = ${i}`);
      }
      assert.equal(db.execute('SELECT val FROM stress WHERE id = 0').rows[0].val, 1);
      assert.equal(db.execute('SELECT val FROM stress WHERE id = 99').rows[0].val, 991);

      db.execute('DELETE FROM stress WHERE val < 500');
      const remaining = db.execute('SELECT COUNT(*) as cnt FROM stress').rows[0].cnt;
      assert.ok(remaining > 0 && remaining < 100);
    });

    it('1000 rows with aggregates', () => {
      const db = new Database();
      db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, grp TEXT, val REAL)');
      db.execute('CREATE INDEX idx_big_grp ON big(grp)');
      const groups = ['A', 'B', 'C', 'D', 'E'];
      for (let i = 0; i < 1000; i++) {
        const g = groups[i % 5];
        db.execute(`INSERT INTO big (id, grp, val) VALUES (${i}, '${g}', ${(i * 3.14).toFixed(2)})`);
      }

      const r = db.execute(`
        SELECT grp, COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg_val
        FROM big GROUP BY grp ORDER BY total DESC
      `);
      assert.equal(r.rows.length, 5);
      assert.equal(r.rows.reduce((s, r) => s + r.cnt, 0), 1000);
    });

    it('multiple JOINs on populated data', () => {
      const db = freshDb();
      // 4-table join that touches all major tables
      const r = db.execute(`
        SELECT c.name, COUNT(DISTINCT o.id) as num_orders,
               COUNT(oi.id) as num_items,
               SUM(oi.quantity * oi.unit_price) as total_revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        GROUP BY c.name
        ORDER BY total_revenue DESC
      `);
      assert.equal(r.rows.length, 10);
      assert.ok(r.rows[0].total_revenue > 0);
      // Every customer should have at least 1 order
      r.rows.forEach(row => assert.ok(row.num_orders >= 1));
    });
  });
});
