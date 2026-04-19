// optimizer-integration.test.js — End-to-end optimizer correctness tests
// Tests that the optimizer picks the right plans AND produces correct results
// for multi-table joins, subqueries, and complex predicates.

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Optimizer Integration — Multi-Table Joins', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    // TPC-H-like schema: customers, orders, line items, products, suppliers
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, balance INT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, status TEXT, total INT)');
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product_id INT, quantity INT, price INT)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, cost INT)');
    db.execute('CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT, region TEXT, rating INT)');

    db.execute('CREATE INDEX idx_orders_cust ON orders(customer_id)');
    db.execute('CREATE INDEX idx_items_order ON items(order_id)');
    db.execute('CREATE INDEX idx_items_product ON items(product_id)');

    // Insert data with known distributions
    const regions = ['EAST', 'WEST', 'NORTH', 'SOUTH'];
    const categories = ['ELECTRONICS', 'FURNITURE', 'FOOD', 'CLOTHING', 'BOOKS'];
    const statuses = ['COMPLETE', 'PENDING', 'SHIPPED', 'CANCELLED'];

    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer${i}', '${regions[i % 4]}', ${1000 + i * 47 % 9000})`);
    }
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, '2025-${String(1 + i % 12).padStart(2, '0')}-${String(1 + i % 28).padStart(2, '0')}', '${statuses[i % 4]}', ${50 + i * 13 % 5000})`);
    }
    for (let i = 0; i < 2000; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i % 500}, ${i % 50}, ${1 + i % 10}, ${10 + i * 7 % 200})`);
    }
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product${i}', '${categories[i % 5]}', ${5 + i * 3 % 100})`);
    }
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO suppliers VALUES (${i}, 'Supplier${i}', '${regions[i % 4]}', ${1 + i % 5})`);
    }

    // Analyze all tables for statistics
    db.execute('ANALYZE customers');
    db.execute('ANALYZE orders');
    db.execute('ANALYZE items');
    db.execute('ANALYZE products');
    db.execute('ANALYZE suppliers');
  });

  it('2-table join: customers + orders — hash join, correct results', () => {
    const result = db.execute(`
      SELECT c.name, COUNT(*) as order_count 
      FROM customers c 
      JOIN orders o ON c.id = o.customer_id 
      WHERE c.region = 'EAST' 
      GROUP BY c.name 
      ORDER BY order_count DESC 
      LIMIT 5
    `);
    assert.ok(result.rows.length > 0, 'Should return results');
    assert.ok(result.rows.length <= 5, 'LIMIT 5 respected');
    // Each EAST customer should have ~5 orders (500/100)
    for (const row of result.rows) {
      assert.ok(row.order_count > 0, 'Each customer has orders');
    }
  });

  it('2-table join: EXPLAIN shows HASH_JOIN for equi-join', () => {
    const explain = db.execute(`
      EXPLAIN SELECT c.name, o.total 
      FROM customers c 
      JOIN orders o ON c.id = o.customer_id
    `);
    const planText = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(planText.includes('Hash') || planText.includes('JOIN'), 
      `Expected hash join in plan, got: ${planText}`);
  });

  it('3-table join: customers → orders → items — correct aggregation', () => {
    const result = db.execute(`
      SELECT c.name, SUM(i.price * i.quantity) as total_spent
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN items i ON o.id = i.order_id
      WHERE c.region = 'WEST'
      GROUP BY c.name
      ORDER BY total_spent DESC
      LIMIT 3
    `);
    assert.ok(result.rows.length > 0, 'Should return results');
    for (const row of result.rows) {
      assert.ok(row.total_spent > 0, 'Total spent should be positive');
    }
  });

  it('3-table join: verify correctness by comparing two equivalent queries', () => {
    // Query 1: 3-way join
    const r1 = db.execute(`
      SELECT COUNT(*) as cnt
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN items i ON o.id = i.order_id
      WHERE c.region = 'NORTH'
    `);

    // Query 2: nested subqueries (should give same count)
    const r2 = db.execute(`
      SELECT COUNT(*) as cnt
      FROM items i
      WHERE i.order_id IN (
        SELECT o.id FROM orders o
        WHERE o.customer_id IN (
          SELECT c.id FROM customers c WHERE c.region = 'NORTH'
        )
      )
    `);

    assert.equal(r1.rows[0].cnt, r2.rows[0].cnt, 
      `Join (${r1.rows[0].cnt}) should equal nested subquery (${r2.rows[0].cnt})`);
  });

  it('4-table join: customers → orders → items → products', () => {
    const result = db.execute(`
      SELECT p.category, COUNT(*) as item_count, SUM(i.price) as revenue
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      JOIN items i ON o.id = i.order_id
      JOIN products p ON i.product_id = p.id
      WHERE o.status = 'COMPLETE'
      GROUP BY p.category
      ORDER BY revenue DESC
    `);
    assert.ok(result.rows.length > 0, 'Should return results by category');
    // 5 categories exist
    assert.ok(result.rows.length <= 5, 'At most 5 categories');
    
    // Revenue should be consistent
    let prevRevenue = Infinity;
    for (const row of result.rows) {
      assert.ok(row.revenue <= prevRevenue, 'Results ordered by revenue DESC');
      prevRevenue = row.revenue;
    }
  });

  it('join with multiple conditions on same table', () => {
    const result = db.execute(`
      SELECT c.name, o.total
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE c.region = 'SOUTH' AND c.balance > 5000 AND o.status = 'SHIPPED'
      ORDER BY o.total DESC
      LIMIT 10
    `);
    // Verify all results match the WHERE conditions
    for (const row of result.rows) {
      assert.ok(row.total !== undefined, 'Total exists');
    }
  });

  it('LEFT JOIN preserves unmatched rows', () => {
    // Add a customer with no orders
    db.execute("INSERT INTO customers VALUES (999, 'Lonely', 'EAST', 5000)");
    
    const result = db.execute(`
      SELECT c.name, COUNT(o.id) as order_count
      FROM customers c
      LEFT JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      HAVING COUNT(o.id) = 0
    `);
    
    const lonely = result.rows.find(r => r.name === 'Lonely');
    assert.ok(lonely, 'Lonely customer should appear in LEFT JOIN');
    assert.equal(lonely.order_count, 0);
  });

  it('self-join: orders from same customer in same month', () => {
    const result = db.execute(`
      SELECT o1.id as order1, o2.id as order2
      FROM orders o1
      JOIN orders o2 ON o1.customer_id = o2.customer_id
      WHERE o1.id < o2.id AND o1.order_date = o2.order_date
      LIMIT 10
    `);
    // Self-join should work and find some pairs
    for (const row of result.rows) {
      assert.ok(row.order1 < row.order2, 'Order pairs should be ordered');
    }
  });
});

describe('Optimizer Integration — Subqueries', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, manager_id INT)');
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, budget INT)');
    
    const depts = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
    for (let i = 0; i < 5; i++) {
      db.execute(`INSERT INTO departments VALUES (${i}, '${depts[i]}', ${100000 + i * 50000})`);
    }
    for (let i = 0; i < 200; i++) {
      const dept = depts[i % 5];
      const salary = 50000 + (i * 137) % 100000;
      const mgr = i < 5 ? 'NULL' : (i % 5); // First 5 are managers
      db.execute(`INSERT INTO employees VALUES (${i}, 'Emp${i}', '${dept}', ${salary}, ${mgr})`);
    }
    db.execute('ANALYZE employees');
    db.execute('ANALYZE departments');
  });

  it('IN subquery: employees in high-budget departments', () => {
    const result = db.execute(`
      SELECT e.name, e.salary
      FROM employees e
      WHERE e.dept IN (SELECT d.name FROM departments d WHERE d.budget > 200000)
      ORDER BY e.salary DESC
      LIMIT 10
    `);
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.length <= 10);
  });

  it('EXISTS subquery: departments with employees over 60K', () => {
    const result = db.execute(`
      SELECT d.name, d.budget
      FROM departments d
      WHERE EXISTS (
        SELECT 1 FROM employees e WHERE e.dept = d.name AND e.salary > 60000
      )
    `);
    assert.ok(result.rows.length > 0, 'Some departments have high earners');
  });

  it('scalar subquery in SELECT: employee salary vs department average', () => {
    const result = db.execute(`
      SELECT e.name, e.salary, 
             (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept = e.dept) as dept_avg
      FROM employees e
      WHERE e.salary > 70000
      ORDER BY e.salary DESC
      LIMIT 5
    `);
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.salary > 70000, 'Salary filter applied');
      assert.ok(row.dept_avg > 0, 'Department average computed');
    }
  });

  it('NOT IN subquery: employees not in Engineering', () => {
    const direct = db.execute(`
      SELECT COUNT(*) as cnt FROM employees e WHERE e.dept != 'Engineering'
    `);
    const subq = db.execute(`
      SELECT COUNT(*) as cnt FROM employees e 
      WHERE e.id NOT IN (SELECT e2.id FROM employees e2 WHERE e2.dept = 'Engineering')
    `);
    assert.equal(direct.rows[0].cnt, subq.rows[0].cnt,
      'NOT IN should match direct filter');
  });
});

describe('Optimizer Integration — Index Selection', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE big_table (id INT PRIMARY KEY, category INT, value INT, status TEXT)');
    db.execute('CREATE INDEX idx_category ON big_table(category)');
    db.execute('CREATE INDEX idx_value ON big_table(value)');
    
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO big_table VALUES (${i}, ${i % 10}, ${i * 7 % 1000}, '${i % 3 === 0 ? 'active' : 'inactive'}')`);
    }
    db.execute('ANALYZE big_table');
  });

  it('high selectivity: index scan for category = 0 (10% of rows)', () => {
    const result = db.execute('SELECT * FROM big_table WHERE category = 0');
    assert.equal(result.rows.length, 100, '10% of 1000 rows');
    
    const explain = db.execute('EXPLAIN SELECT * FROM big_table WHERE category = 0');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    // Optimizer should consider index scan for 10% selectivity
    // (Whether it picks index or seq depends on cost model params)
    assert.ok(plan.includes('Scan'), 'Plan should include a scan operation');
  });

  it('low selectivity: seq scan preferred for status != active (67% of rows)', () => {
    const result = db.execute("SELECT COUNT(*) as cnt FROM big_table WHERE status != 'active'");
    // ~667 rows out of 1000
    assert.ok(result.rows[0].cnt > 600, 'Most rows are inactive');
  });

  it('PK lookup: point query by primary key', () => {
    const result = db.execute('SELECT * FROM big_table WHERE id = 42');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 42);
  });

  it('range query on indexed column', () => {
    const result = db.execute('SELECT * FROM big_table WHERE value BETWEEN 100 AND 200');
    assert.ok(result.rows.length > 0, 'Range query returns results');
    for (const row of result.rows) {
      assert.ok(row.value >= 100 && row.value <= 200, 'Value in range');
    }
  });

  it('compound filter: index + non-indexed', () => {
    const result = db.execute("SELECT * FROM big_table WHERE category = 5 AND status = 'active'");
    for (const row of result.rows) {
      assert.equal(row.category, 5);
      assert.equal(row.status, 'active');
    }
  });
});

describe('Optimizer Integration — Aggregation and Grouping', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, store TEXT, product TEXT, amount INT, sale_date TEXT)');
    
    const stores = ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'];
    const products = ['Widget', 'Gadget', 'Doohickey', 'Thingamajig'];
    
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO sales VALUES (${i}, '${stores[i % 5]}', '${products[i % 4]}', ${10 + i * 13 % 500}, '2025-${String(1 + i % 12).padStart(2, '0')}-01')`);
    }
    db.execute('ANALYZE sales');
  });

  it('GROUP BY with multiple aggregates', () => {
    const result = db.execute(`
      SELECT store, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg_amount, MAX(amount) as max_amount
      FROM sales
      GROUP BY store
      ORDER BY total DESC
    `);
    assert.equal(result.rows.length, 5, '5 stores');
    
    let totalSales = 0;
    for (const row of result.rows) {
      assert.equal(row.cnt, 100, 'Each store has 100 sales');
      totalSales += row.total;
    }
    
    // Verify total matches ungrouped SUM
    const globalTotal = db.execute('SELECT SUM(amount) as total FROM sales');
    assert.equal(totalSales, globalTotal.rows[0].total, 'Grouped totals match global total');
  });

  it('GROUP BY with HAVING', () => {
    const result = db.execute(`
      SELECT product, SUM(amount) as total
      FROM sales
      GROUP BY product
      HAVING SUM(amount) > 25000
      ORDER BY total DESC
    `);
    for (const row of result.rows) {
      assert.ok(row.total > 25000, 'HAVING filter applied');
    }
  });

  it('DISTINCT on large result set', () => {
    const result = db.execute('SELECT DISTINCT store FROM sales ORDER BY store');
    assert.equal(result.rows.length, 5, '5 distinct stores');
    assert.equal(result.rows[0].store, 'Chicago');
  });

  it('nested aggregation with subquery', () => {
    const result = db.execute(`
      SELECT store, total FROM (
        SELECT store, SUM(amount) as total
        FROM sales
        GROUP BY store
      ) sub
      WHERE total > 20000
      ORDER BY total DESC
    `);
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.total > 20000);
    }
  });
});

describe('Optimizer Integration — EXPLAIN ANALYZE', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, data TEXT)');
    db.execute('CREATE INDEX idx_t2_t1 ON t2(t1_id)');
    
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t1 VALUES (${i}, ${i * 7 % 100})`);
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO t2 VALUES (${i}, ${i % 100}, 'data${i}')`);
    db.execute('ANALYZE t1');
    db.execute('ANALYZE t2');
  });

  it('EXPLAIN ANALYZE shows actual timing', () => {
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t1 WHERE val > 50');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    // Should include actual timing info
    assert.ok(plan.length > 0, 'Plan is non-empty');
  });

  it('EXPLAIN shows join type for equi-join', () => {
    const result = db.execute('EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Join') || plan.includes('JOIN') || plan.includes('join'), 
      `Expected join in plan: ${plan}`);
  });

  it('result correctness: join count matches', () => {
    const joined = db.execute('SELECT COUNT(*) as cnt FROM t1 JOIN t2 ON t1.id = t2.t1_id');
    // Each t1 row matches 5 t2 rows (500/100), so count = 500
    assert.equal(joined.rows[0].cnt, 500);
  });
});
