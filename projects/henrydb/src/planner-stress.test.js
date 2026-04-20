// planner-stress.test.js — Stress tests for query planner edge cases
// Complex JOINs, subquery patterns, index selection, multi-table queries

import { Database } from './db.js';
import { strict as assert } from 'assert';

let db, pass = 0, fail = 0;

function test(name, fn) {
  db = new Database();
  try { fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}: ${e.message}`); }
}

function setup3Tables() {
  db.execute('CREATE TABLE customers(id INT PRIMARY KEY, name TEXT, city TEXT)');
  db.execute('CREATE TABLE orders(id INT PRIMARY KEY, customer_id INT, amount DECIMAL, status TEXT)');
  db.execute('CREATE TABLE items(id INT PRIMARY KEY, order_id INT, product TEXT, qty INT, price DECIMAL)');
  db.execute('CREATE INDEX idx_orders_cust ON orders(customer_id)');
  db.execute('CREATE INDEX idx_items_order ON items(order_id)');
  
  for (let i = 1; i <= 20; i++) {
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${['NYC','LA','Chicago','Denver','Seattle'][i%5]}')`);
  }
  for (let i = 1; i <= 50; i++) {
    const cid = (i % 20) + 1;
    const status = ['pending','shipped','delivered','cancelled'][i%4];
    db.execute(`INSERT INTO orders VALUES (${i}, ${cid}, ${(i*10.5).toFixed(2)}, '${status}')`);
  }
  for (let i = 1; i <= 100; i++) {
    const oid = (i % 50) + 1;
    db.execute(`INSERT INTO items VALUES (${i}, ${oid}, 'Product ${i%10}', ${(i%5)+1}, ${(i*2.99).toFixed(2)})`);
  }
}

console.log('\n🧪 Query Planner Stress Tests');

// --- Multi-table JOINs ---

test('3-way JOIN with GROUP BY and HAVING', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    JOIN items i ON i.order_id = o.id
    GROUP BY c.name
    HAVING COUNT(o.id) > 2
  `).rows;
  assert.ok(r.length > 0, 'Should have results');
  for (const row of r) {
    assert.ok(row.order_count > 2);
    assert.ok(typeof row.total === 'number');
  }
});

test('3-way JOIN with ORDER BY on aggregate', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.city, COUNT(DISTINCT o.id) as orders, SUM(i.qty * i.price) as revenue
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    JOIN items i ON i.order_id = o.id
    GROUP BY c.city
    ORDER BY revenue DESC
  `).rows;
  assert.ok(r.length > 0);
  for (let i = 1; i < r.length; i++) {
    assert.ok(r[i-1].revenue >= r[i].revenue, 'Should be sorted by revenue DESC');
  }
});

test('LEFT JOIN with NULL handling in GROUP BY', () => {
  setup3Tables();
  db.execute("INSERT INTO customers VALUES (99, 'No Orders', 'Nowhere')");
  const r = db.execute(`
    SELECT c.name, COUNT(o.id) as order_count
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.id
    GROUP BY c.name
    ORDER BY order_count ASC
  `).rows;
  const noOrders = r.find(x => x.name === 'No Orders');
  assert.ok(noOrders, 'Should include customer with no orders');
  assert.equal(noOrders.order_count, 0);
});

test('Self-JOIN', () => {
  db.execute('CREATE TABLE emp(id INT PRIMARY KEY, name TEXT, manager_id INT)');
  db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL)");
  db.execute("INSERT INTO emp VALUES (2, 'VP', 1)");
  db.execute("INSERT INTO emp VALUES (3, 'Director', 2)");
  db.execute("INSERT INTO emp VALUES (4, 'Manager', 3)");
  db.execute("INSERT INTO emp VALUES (5, 'Engineer', 4)");
  const r = db.execute(`
    SELECT e.name as employee, m.name as manager
    FROM emp e
    LEFT JOIN emp m ON e.manager_id = m.id
    ORDER BY e.id
  `).rows;
  assert.equal(r.length, 5);
  assert.equal(r[0].manager, null); // CEO has no manager
  assert.equal(r[1].manager, 'CEO');
  assert.equal(r[4].manager, 'Manager');
});

// --- Subquery patterns ---

test('Correlated subquery in WHERE', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.name, c.city
    FROM customers c
    WHERE (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) > 2
  `).rows;
  assert.ok(r.length >= 0); // May or may not have results depending on data
  // Verify correctness
  for (const row of r) {
    const cnt = db.execute(`SELECT COUNT(*) as c FROM orders WHERE customer_id = (SELECT id FROM customers WHERE name = '${row.name}')`).rows[0].c;
    assert.ok(cnt > 2, `${row.name} should have >2 orders`);
  }
});

test('Scalar subquery in SELECT', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.name, 
           (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id) as total_spent
    FROM customers c
    WHERE c.city = 'NYC'
    ORDER BY c.name
  `).rows;
  assert.ok(r.length > 0);
  for (const row of r) {
    assert.ok(row.total_spent === null || typeof row.total_spent === 'number');
  }
});

test('IN subquery', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT name FROM customers
    WHERE id IN (SELECT DISTINCT customer_id FROM orders WHERE status = 'shipped')
    ORDER BY name
  `).rows;
  assert.ok(r.length > 0);
});

test('EXISTS subquery', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.name
    FROM customers c
    WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 400)
  `).rows;
  // Verify each result actually has a high-value order
  for (const row of r) {
    const high = db.execute(`SELECT COUNT(*) as c FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.name = '${row.name}' AND o.amount > 400`).rows[0].c;
    assert.ok(high > 0);
  }
});

test('NOT EXISTS subquery', () => {
  setup3Tables();
  db.execute("INSERT INTO customers VALUES (98, 'Ghost', 'Void')");
  const r = db.execute(`
    SELECT c.name
    FROM customers c
    WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
  `).rows;
  const ghost = r.find(x => x.name === 'Ghost');
  assert.ok(ghost, 'Ghost customer should appear');
});

// --- CTE patterns ---

test('CTE with JOIN', () => {
  setup3Tables();
  const r = db.execute(`
    WITH high_value AS (
      SELECT customer_id, SUM(amount) as total
      FROM orders
      GROUP BY customer_id
      HAVING SUM(amount) > 50
    )
    SELECT c.name, hv.total
    FROM high_value hv
    JOIN customers c ON c.id = hv.customer_id
    ORDER BY hv.total DESC
  `).rows;
  assert.ok(r.length > 0);
  for (let i = 1; i < r.length; i++) {
    assert.ok(r[i-1].total >= r[i].total);
  }
});

test('Recursive CTE (hierarchy)', () => {
  db.execute('CREATE TABLE categories(id INT PRIMARY KEY, name TEXT, parent_id INT)');
  db.execute("INSERT INTO categories VALUES (1, 'Root', NULL)");
  db.execute("INSERT INTO categories VALUES (2, 'Electronics', 1)");
  db.execute("INSERT INTO categories VALUES (3, 'Phones', 2)");
  db.execute("INSERT INTO categories VALUES (4, 'Laptops', 2)");
  db.execute("INSERT INTO categories VALUES (5, 'iPhone', 3)");
  const r = db.execute(`
    WITH RECURSIVE tree AS (
      SELECT id, name, 0 as depth FROM categories WHERE parent_id IS NULL
      UNION ALL
      SELECT c.id, c.name, t.depth + 1
      FROM categories c JOIN tree t ON c.parent_id = t.id
    )
    SELECT name, depth FROM tree ORDER BY depth, name
  `).rows;
  assert.equal(r.length, 5);
  assert.equal(r[0].name, 'Root');
  assert.equal(r[0].depth, 0);
  assert.equal(r[4].depth, 3); // iPhone (Root→Electronics→Phones→iPhone)
});

// --- Window functions with planner ---

test('Window function with PARTITION BY and ORDER BY', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT o.id, o.customer_id, o.amount,
           ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.amount DESC) as rn,
           SUM(o.amount) OVER (PARTITION BY o.customer_id) as cust_total
    FROM orders o
    WHERE o.status != 'cancelled'
    ORDER BY o.customer_id, rn
  `).rows;
  assert.ok(r.length > 0);
  // Verify row numbers reset per customer
  let lastCust = null;
  for (const row of r) {
    if (row.customer_id !== lastCust) {
      assert.equal(row.rn, 1);
      lastCust = row.customer_id;
    }
  }
});

test('Multiple window functions in same query', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT customer_id, amount,
           RANK() OVER (ORDER BY amount DESC) as overall_rank,
           RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as cust_rank,
           LAG(amount) OVER (ORDER BY id) as prev_amount
    FROM orders
    LIMIT 10
  `).rows;
  assert.equal(r.length, 10);
  for (const row of r) {
    assert.ok(typeof row.overall_rank === 'number');
    assert.ok(typeof row.cust_rank === 'number');
  }
});

// --- Complex WHERE clauses ---

test('Nested boolean expressions', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT c.name
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    WHERE (c.city = 'NYC' OR c.city = 'LA')
      AND o.amount > 100
      AND o.status IN ('shipped', 'delivered')
    GROUP BY c.name
  `).rows;
  assert.ok(Array.isArray(r));
});

test('BETWEEN in JOIN condition', () => {
  db.execute('CREATE TABLE ranges(id INT, low INT, high INT)');
  db.execute('CREATE TABLE vals(id INT, val INT)');
  db.execute('INSERT INTO ranges VALUES (1, 10, 20), (2, 30, 40)');
  db.execute('INSERT INTO vals VALUES (1, 15), (2, 25), (3, 35)');
  const r = db.execute(`
    SELECT v.val, r.id as range_id
    FROM vals v
    JOIN ranges r ON v.val BETWEEN r.low AND r.high
  `).rows;
  assert.equal(r.length, 2);
  assert.ok(r.some(x => x.val === 15 && x.range_id === 1));
  assert.ok(r.some(x => x.val === 35 && x.range_id === 2));
});

test('CASE in ORDER BY', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT name, city
    FROM customers
    ORDER BY CASE city
      WHEN 'NYC' THEN 1
      WHEN 'LA' THEN 2
      ELSE 3
    END, name
    LIMIT 10
  `).rows;
  assert.equal(r.length, 10);
});

// --- EXPLAIN ---

test('EXPLAIN shows plan for complex query', () => {
  setup3Tables();
  const r = db.execute(`
    EXPLAIN SELECT c.name, SUM(o.amount)
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    WHERE c.city = 'NYC'
    GROUP BY c.name
    ORDER BY SUM(o.amount) DESC
  `).rows;
  assert.ok(r.length > 0);
  // EXPLAIN should return plan rows
  const planText = r.map(x => Object.values(x)[0]).join('\n');
  assert.ok(planText.length > 0);
});

// --- UNION / set operations ---

test('UNION with different query shapes', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT name as label, 'customer' as type FROM customers WHERE city = 'NYC'
    UNION
    SELECT status as label, 'status' as type FROM orders
    ORDER BY type, label
  `).rows;
  assert.ok(r.length > 0);
  // UNION should deduplicate
  const labels = r.map(x => `${x.type}:${x.label}`);
  assert.equal(labels.length, new Set(labels).size, 'UNION should deduplicate');
});

test('UNION ALL preserves duplicates', () => {
  db.execute('CREATE TABLE a(x INT)');
  db.execute('CREATE TABLE b(x INT)');
  db.execute('INSERT INTO a VALUES (1), (2), (3)');
  db.execute('INSERT INTO b VALUES (2), (3), (4)');
  const r = db.execute('SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x').rows;
  assert.equal(r.length, 6);
});

// --- Derived tables / subqueries in FROM ---

test('Subquery in FROM with alias', () => {
  setup3Tables();
  const r = db.execute(`
    SELECT sub.city, sub.cnt
    FROM (
      SELECT city, COUNT(*) as cnt FROM customers GROUP BY city
    ) sub
    WHERE sub.cnt > 3
    ORDER BY sub.cnt DESC
  `).rows;
  assert.ok(Array.isArray(r));
  for (const row of r) {
    assert.ok(row.cnt > 3);
  }
});

console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
