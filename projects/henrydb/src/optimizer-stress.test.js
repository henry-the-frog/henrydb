// optimizer-stress.test.js — Query optimizer stress tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setupSalesDB() {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
  db.execute('CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product TEXT, qty INT, price INT)');
  db.execute('CREATE INDEX idx_orders_customer ON orders(customer_id)');
  db.execute('CREATE INDEX idx_items_order ON items(order_id)');
  db.execute('CREATE INDEX idx_orders_status ON orders(status)');
  
  db.execute("INSERT INTO customers VALUES (1,'alice','north'),(2,'bob','south'),(3,'charlie','north'),(4,'dave','east'),(5,'eve','south')");
  db.execute("INSERT INTO orders VALUES (1,1,100,'shipped'),(2,1,200,'pending'),(3,2,150,'shipped'),(4,3,300,'shipped'),(5,4,50,'cancelled'),(6,5,250,'pending')");
  db.execute("INSERT INTO items VALUES (1,1,'widget',2,25),(2,1,'gadget',1,50),(3,2,'widget',5,20),(4,3,'gadget',3,50),(5,4,'thing',1,300),(6,5,'widget',1,50),(7,6,'gadget',5,50)");
  return db;
}

describe('Complex JOINs', () => {
  it('3-way join with aggregation', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT c.name, COUNT(o.id) as num_orders, SUM(i.qty * i.price) as total_value
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN items i ON o.id = i.order_id
      GROUP BY c.name
      ORDER BY total_value DESC
    `);
    assert.ok(r.rows.length > 0);
    // Verify totals make sense
    for (const row of r.rows) {
      assert.ok(row.total_value > 0);
      assert.ok(row.num_orders > 0);
    }
  });

  it('LEFT JOIN preserves unmatched rows', () => {
    const db = setupSalesDB();
    // Add a customer with no orders
    db.execute("INSERT INTO customers VALUES (6,'frank','west')");
    
    const r = db.execute(`
      SELECT c.name, COUNT(o.id) as num_orders
      FROM customers c
      LEFT JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      ORDER BY c.name
    `);
    
    const frank = r.rows.find(row => row.name === 'frank');
    assert.ok(frank, 'frank should appear in LEFT JOIN results');
    assert.equal(frank.num_orders, 0);
  });

  it('self-join: customers in same region', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT a.name as customer1, b.name as customer2, a.region
      FROM customers a
      JOIN customers b ON a.region = b.region AND a.id < b.id
      ORDER BY a.region, a.name
    `);
    assert.ok(r.rows.length > 0);
    // north has alice and charlie
    const northPair = r.rows.find(row => row.region === 'north');
    assert.ok(northPair);
  });

  it('CROSS JOIN with filter (equivalent to inner join)', () => {
    const db = setupSalesDB();
    const r1 = db.execute(`
      SELECT c.name, o.amount
      FROM customers c, orders o
      WHERE c.id = o.customer_id AND o.status = 'shipped'
      ORDER BY c.name
    `);
    const r2 = db.execute(`
      SELECT c.name, o.amount
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE o.status = 'shipped'
      ORDER BY c.name
    `);
    assert.deepEqual(r1.rows, r2.rows);
  });
});

describe('Subquery Patterns', () => {
  it('scalar subquery in SELECT', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT name,
             (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) as order_count
      FROM customers
      ORDER BY name
    `);
    assert.equal(r.rows.length, 5);
    const alice = r.rows.find(row => row.name === 'alice');
    assert.equal(alice.order_count, 2); // alice has 2 orders
  });

  it('IN subquery', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT name FROM customers
      WHERE id IN (SELECT customer_id FROM orders WHERE status = 'shipped')
      ORDER BY name
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.some(row => row.name === 'alice'));
  });

  it('EXISTS subquery', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT name FROM customers c
      WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id AND amount > 200)
      ORDER BY name
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.some(row => row.name === 'charlie')); // order #4 = 300
  });

  it('NOT IN subquery', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT name FROM customers
      WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'cancelled')
      ORDER BY name
    `);
    // dave has a cancelled order, everyone else should be included
    assert.ok(!r.rows.some(row => row.name === 'dave'));
    assert.ok(r.rows.some(row => row.name === 'alice'));
  });

  it('subquery in FROM (derived table)', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT region, avg_amount
      FROM (
        SELECT c.region, AVG(o.amount) as avg_amount
        FROM customers c JOIN orders o ON c.id = o.customer_id
        GROUP BY c.region
      ) as regional_avg
      ORDER BY avg_amount DESC
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(typeof row.avg_amount === 'number');
    }
  });
});

describe('Predicate Pushdown', () => {
  it('WHERE on joined table filters before join', () => {
    const db = setupSalesDB();
    // These should produce the same result
    const r1 = db.execute(`
      SELECT c.name, o.amount
      FROM customers c JOIN orders o ON c.id = o.customer_id
      WHERE o.status = 'shipped'
      ORDER BY c.name, o.amount
    `);
    // Verify correctness
    for (const row of r1.rows) {
      assert.ok(row.amount > 0);
    }
    assert.ok(r1.rows.length > 0);
  });

  it('combined predicates on multiple tables', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT c.name, o.amount, o.status
      FROM customers c JOIN orders o ON c.id = o.customer_id
      WHERE c.region = 'north' AND o.status = 'shipped'
      ORDER BY c.name
    `);
    for (const row of r.rows) {
      assert.equal(row.status, 'shipped');
    }
  });

  it('HAVING after GROUP BY with JOIN', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT c.name, SUM(o.amount) as total
      FROM customers c JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      HAVING SUM(o.amount) > 200
      ORDER BY total DESC
    `);
    for (const row of r.rows) {
      assert.ok(row.total > 200);
    }
  });
});

describe('Index Selection', () => {
  it('primary key lookup', () => {
    const db = setupSalesDB();
    const r = db.execute('SELECT name FROM customers WHERE id = 3');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'charlie');
  });

  it('secondary index lookup', () => {
    const db = setupSalesDB();
    const r = db.execute("SELECT id, amount FROM orders WHERE status = 'shipped' ORDER BY id");
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      // Verify these are actually shipped
      const check = db.execute(`SELECT status FROM orders WHERE id = ${row.id}`);
      assert.equal(check.rows[0].status, 'shipped');
    }
  });

  it('index on join column', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT c.name, o.amount
      FROM customers c JOIN orders o ON c.id = o.customer_id
      WHERE c.id = 1
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.equal(row.name, 'alice');
    }
  });

  it('composite filter: index + non-indexed', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT id, amount FROM orders
      WHERE status = 'shipped' AND amount > 100
      ORDER BY amount
    `);
    for (const row of r.rows) {
      assert.ok(row.amount > 100);
    }
  });
});

describe('Complex Query Patterns', () => {
  it('UNION ALL + ORDER BY', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT name, 'customer' as type FROM customers WHERE region = 'north'
      UNION ALL
      SELECT product as name, 'product' as type FROM items WHERE qty > 2
      ORDER BY name
    `);
    assert.ok(r.rows.length > 0);
  });

  it('nested aggregation with CASE', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT 
        c.region,
        COUNT(*) as total_orders,
        SUM(CASE WHEN o.status = 'shipped' THEN 1 ELSE 0 END) as shipped_orders,
        SUM(CASE WHEN o.status = 'pending' THEN o.amount ELSE 0 END) as pending_amount
      FROM customers c JOIN orders o ON c.id = o.customer_id
      GROUP BY c.region
      ORDER BY c.region
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.total_orders >= row.shipped_orders);
    }
  });

  it('CTE + JOIN + window function + HAVING', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      WITH ranked AS (
        SELECT c.name, c.region, o.amount,
               RANK() OVER (PARTITION BY c.region ORDER BY o.amount DESC) as rk
        FROM customers c JOIN orders o ON c.id = o.customer_id
      )
      SELECT name, region, amount, rk FROM ranked
      WHERE rk <= 2
      ORDER BY region, rk
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.rk <= 2);
    }
  });

  it('multiple CTEs referencing each other', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      WITH 
        cust_orders AS (
          SELECT c.id as cid, c.name, COUNT(o.id) as cnt, SUM(o.amount) as total
          FROM customers c JOIN orders o ON c.id = o.customer_id
          GROUP BY c.id, c.name
        ),
        high_value AS (
          SELECT * FROM cust_orders WHERE total > 200
        )
      SELECT name, cnt, total FROM high_value ORDER BY total DESC
    `);
    for (const row of r.rows) {
      assert.ok(row.total > 200);
    }
  });

  it('DISTINCT + ORDER BY + LIMIT', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT DISTINCT c.region, o.status
      FROM customers c JOIN orders o ON c.id = o.customer_id
      ORDER BY c.region
      LIMIT 5
    `);
    assert.ok(r.rows.length <= 5);
    // Verify distinct
    const seen = new Set();
    for (const row of r.rows) {
      const key = `${row.region}-${row.status}`;
      assert.ok(!seen.has(key), `Duplicate: ${key}`);
      seen.add(key);
    }
  });

  it('correlated subquery with aggregate', () => {
    const db = setupSalesDB();
    const r = db.execute(`
      SELECT c.name,
             (SELECT MAX(amount) FROM orders WHERE customer_id = c.id) as max_order
      FROM customers c
      WHERE (SELECT COUNT(*) FROM orders WHERE customer_id = c.id) > 0
      ORDER BY max_order DESC
    `);
    assert.ok(r.rows.length > 0);
    // Should be ordered by max_order DESC
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i - 1].max_order >= r.rows[i].max_order);
    }
  });
});
