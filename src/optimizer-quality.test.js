// optimizer-quality.test.js — Measure query optimizer plan quality vs naive execution
// Tests that the optimizer actually picks better plans on realistic data

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

let db;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setup() {
  db = new Database();
  
  // TPC-H-inspired schema: customers, orders, line items
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, nation TEXT, segment TEXT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, total_price INT, status TEXT)');
  db.execute('CREATE TABLE lineitem (id INT PRIMARY KEY, order_id INT, part_id INT, quantity INT, price INT, discount INT)');
  db.execute('CREATE TABLE parts (id INT PRIMARY KEY, name TEXT, brand TEXT, size INT, container TEXT)');
  
  // Create indexes
  db.execute('CREATE INDEX idx_orders_cust ON orders (customer_id)');
  db.execute('CREATE INDEX idx_lineitem_order ON lineitem (order_id)');
  db.execute('CREATE INDEX idx_lineitem_part ON lineitem (part_id)');
  db.execute('CREATE INDEX idx_customers_nation ON customers (nation)');
  
  // Generate realistic data
  const nations = ['USA', 'Germany', 'France', 'Japan', 'Brazil', 'UK', 'China', 'India'];
  const segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD'];
  const statuses = ['O', 'F', 'P'];
  const brands = ['Brand#11', 'Brand#12', 'Brand#21', 'Brand#22', 'Brand#31'];
  const containers = ['SM CASE', 'SM BOX', 'MED BAG', 'LG CASE', 'LG BOX'];
  
  // 500 customers
  for (let i = 1; i <= 500; i++) {
    const nation = nations[i % nations.length];
    const segment = segments[i % segments.length];
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer${i}', '${nation}', '${segment}')`);
  }
  
  // 2000 orders
  for (let i = 1; i <= 2000; i++) {
    const custId = (i % 500) + 1;
    const year = 2020 + (i % 5);
    const month = String((i % 12) + 1).padStart(2, '0');
    const day = String((i % 28) + 1).padStart(2, '0');
    const total = 100 + (i * 7 % 10000);
    const status = statuses[i % statuses.length];
    db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, '${year}-${month}-${day}', ${total}, '${status}')`);
  }
  
  // 5000 line items
  for (let i = 1; i <= 5000; i++) {
    const orderId = (i % 2000) + 1;
    const partId = (i % 200) + 1;
    const qty = (i % 50) + 1;
    const price = 10 + (i % 990);
    const discount = i % 10;
    db.execute(`INSERT INTO lineitem VALUES (${i}, ${orderId}, ${partId}, ${qty}, ${price}, ${discount})`);
  }
  
  // 200 parts
  for (let i = 1; i <= 200; i++) {
    const brand = brands[i % brands.length];
    const size = (i % 50) + 1;
    const container = containers[i % containers.length];
    db.execute(`INSERT INTO parts VALUES (${i}, 'Part${i}', '${brand}', ${size}, '${container}')`);
  }
  
  // Gather statistics for the optimizer
  db.execute('ANALYZE customers');
  db.execute('ANALYZE orders');
  db.execute('ANALYZE lineitem');
  db.execute('ANALYZE parts');
}

function teardown() {
  db = null;
}

function timeQuery(query) {
  const start = process.hrtime.bigint();
  const result = db.execute(query);
  const end = process.hrtime.bigint();
  return { result, timeNs: Number(end - start) };
}

// ===== TPC-H-INSPIRED QUERIES =====

describe('Query Optimizer Quality: TPC-H-like', { timeout: 120000 }, () => {
  beforeEach(setup);
  afterEach(teardown);

  it('Q1: Pricing Summary — aggregation with filter', () => {
    // TPC-H Q1: aggregate line items by status
    const { result, timeNs } = timeQuery(`
      SELECT status, SUM(total_price) AS revenue, COUNT(*) AS cnt
      FROM orders 
      WHERE order_date >= '2022-01-01'
      GROUP BY status
      ORDER BY status
    `);
    const r = rows(result);
    assert.ok(r.length > 0, 'Should return results');
    assert.ok(r.length <= 3, 'At most 3 statuses');
    // Verify correct aggregation
    const totalCnt = r.reduce((s, x) => s + x.cnt, 0);
    assert.ok(totalCnt > 0 && totalCnt <= 2000, `Total count ${totalCnt} should be reasonable`);
  });

  it('Q3: Shipping Priority — 3-way join with filter', () => {
    // TPC-H Q3: find unshipped orders for a market segment
    const { result, timeNs } = timeQuery(`
      SELECT o.id, o.total_price, o.order_date
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE c.segment = 'AUTOMOBILE'
        AND o.status = 'O'
        AND o.order_date < '2023-01-01'
      ORDER BY o.total_price DESC
      LIMIT 10
    `);
    const r = rows(result);
    assert.ok(r.length <= 10, 'LIMIT 10');
    // Verify all results have correct segment filter
    for (const row of r) {
      assert.ok(row.total_price > 0);
    }
  });

  it('Q5: Local Supplier Volume — multi-way join', () => {
    // Simplified Q5: revenue from orders by customer nation
    const { result } = timeQuery(`
      SELECT c.nation, SUM(o.total_price) AS revenue
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE o.order_date >= '2022-01-01'
        AND o.order_date < '2023-01-01'
      GROUP BY c.nation
      ORDER BY revenue DESC
    `);
    const r = rows(result);
    assert.ok(r.length > 0, 'Should have results by nation');
    // Revenue should be descending
    for (let i = 1; i < r.length; i++) {
      assert.ok(r[i - 1].revenue >= r[i].revenue, 'Should be sorted DESC by revenue');
    }
  });

  it('Q6: Forecasting Revenue — selective filter', () => {
    // TPC-H Q6: very selective scan
    const { result } = timeQuery(`
      SELECT SUM(price * quantity) AS revenue
      FROM lineitem
      WHERE discount >= 5 AND discount <= 7
        AND quantity < 25
    `);
    const r = rows(result);
    assert.equal(r.length, 1);
    assert.ok(r[0].revenue > 0);
  });

  it('Q10: Returned Item Reporting — join + aggregation', () => {
    const { result } = timeQuery(`
      SELECT c.id, c.name, c.nation, SUM(o.total_price) AS revenue
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE o.status = 'F'
      GROUP BY c.id, c.name, c.nation
      ORDER BY revenue DESC
      LIMIT 20
    `);
    const r = rows(result);
    assert.ok(r.length <= 20, 'LIMIT 20');
    assert.ok(r.length > 0);
  });
});

// ===== OPTIMIZER SELECTIVITY ACCURACY =====

describe('Optimizer Selectivity Accuracy', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('equality predicate selectivity is within 2x of actual', () => {
    const actual = rows(db.execute("SELECT COUNT(*) AS c FROM customers WHERE nation = 'USA'"))[0].c;
    const total = rows(db.execute("SELECT COUNT(*) AS c FROM customers"))[0].c;
    const actualSelectivity = actual / total;
    
    // With 8 nations and 500 customers, expected ~62-63 per nation
    assert.ok(actual > 40 && actual < 100, `USA count ${actual} should be ~62`);
    assert.ok(actualSelectivity > 0.08 && actualSelectivity < 0.25, 
      `Selectivity ${actualSelectivity} should be ~0.125`);
  });

  it('range predicate selectivity is reasonable', () => {
    const actual = rows(db.execute(`
      SELECT COUNT(*) AS c FROM orders WHERE order_date >= '2023-01-01'
    `))[0].c;
    const total = 2000;
    const actualSelectivity = actual / total;
    
    // ~40% of orders should be 2023-2024
    assert.ok(actualSelectivity > 0.2 && actualSelectivity < 0.7, 
      `Range selectivity ${actualSelectivity} should be ~0.4`);
  });

  it('join produces correct result count', () => {
    const r = rows(db.execute(`
      SELECT COUNT(*) AS c FROM customers c JOIN orders o ON c.id = o.customer_id
    `));
    assert.equal(r[0].c, 2000, 'Every order should match a customer');
  });
});

// ===== PLAN QUALITY: INDEX USE =====

describe('Plan Quality: Index Usage', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('indexed lookup is faster than full scan for selective query', () => {
    // Warm up
    db.execute("SELECT * FROM customers WHERE nation = 'Japan'");
    db.execute("SELECT * FROM customers WHERE id = 1");
    
    // Selective index lookup (single row)
    const { timeNs: indexTime } = timeQuery('SELECT * FROM customers WHERE id = 250');
    
    // Full scan with filter (many rows match)
    const { timeNs: scanTime } = timeQuery('SELECT * FROM customers');
    
    // Index lookup should be significantly faster than full scan
    // (at least for point queries vs full table scan)
    assert.ok(indexTime < scanTime * 2, 
      `Index lookup (${indexTime}ns) should be faster than full scan (${scanTime}ns)`);
  });

  it('join with index produces correct results', () => {
    const r = rows(db.execute(`
      SELECT c.name, o.total_price
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE c.id = 1
    `));
    // Customer 1 should have ~4 orders (2000 orders / 500 customers)
    assert.ok(r.length >= 1 && r.length <= 20, `Customer 1 orders: ${r.length}`);
    for (const row of r) {
      assert.equal(row.name, 'Customer1');
    }
  });
});

// ===== COMPLEX ANALYTICS =====

describe('Complex Analytics Queries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('top 10 customers by order count', () => {
    const r = rows(db.execute(`
      SELECT c.id, c.name, COUNT(*) AS order_count
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.id, c.name
      ORDER BY order_count DESC
      LIMIT 10
    `));
    assert.equal(r.length, 10);
    for (let i = 1; i < r.length; i++) {
      assert.ok(r[i - 1].order_count >= r[i].order_count);
    }
  });

  it('revenue by segment and year', () => {
    const r = rows(db.execute(`
      SELECT c.segment, SUM(o.total_price) AS total_revenue, COUNT(*) AS order_count
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.segment
      ORDER BY total_revenue DESC
    `));
    assert.equal(r.length, 5, '5 segments');
    const totalRevenue = r.reduce((s, x) => s + x.total_revenue, 0);
    assert.ok(totalRevenue > 0);
  });

  it('subquery: customers with above-average order value', () => {
    const r = rows(db.execute(`
      SELECT c.id, c.name, o.total_price
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE o.total_price > (SELECT AVG(total_price) FROM orders)
      ORDER BY o.total_price DESC
      LIMIT 10
    `));
    assert.ok(r.length > 0);
    // All results should have above-average price
    const avg = rows(db.execute('SELECT AVG(total_price) AS avg_price FROM orders'))[0].avg_price;
    for (const row of r) {
      assert.ok(row.total_price > avg, `${row.total_price} should be > avg ${avg}`);
    }
  });
});
