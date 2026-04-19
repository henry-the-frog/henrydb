// tpch-mini.test.js — Simplified TPC-H benchmark queries
// Tests complex analytical queries against a star-schema dataset

import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

let db;

before(() => {
  db = new Database();
  
  // TPC-H-like schema (simplified)
  db.execute(`CREATE TABLE nation (id INT PRIMARY KEY, name TEXT, region TEXT)`);
  db.execute(`CREATE TABLE customer (id INT PRIMARY KEY, name TEXT, nation_id INT, balance INT)`);
  db.execute(`CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, order_date TEXT, total_price INT, status TEXT)`);
  db.execute(`CREATE TABLE lineitem (id INT PRIMARY KEY, order_id INT, part_name TEXT, quantity INT, price INT, discount INT, tax INT, ship_date TEXT, return_flag TEXT, line_status TEXT)`);
  
  // Create indexes
  db.execute('CREATE INDEX idx_orders_cust ON orders(customer_id)');
  db.execute('CREATE INDEX idx_lineitem_order ON lineitem(order_id)');
  db.execute('CREATE INDEX idx_orders_date ON orders(order_date)');
  
  // Load nations
  const nations = [
    [0, 'USA', 'AMERICA'], [1, 'CANADA', 'AMERICA'], [2, 'UK', 'EUROPE'],
    [3, 'GERMANY', 'EUROPE'], [4, 'FRANCE', 'EUROPE'], [5, 'JAPAN', 'ASIA'],
    [6, 'CHINA', 'ASIA'], [7, 'BRAZIL', 'AMERICA'], [8, 'INDIA', 'ASIA'],
  ];
  for (const [id, name, region] of nations) {
    db.execute(`INSERT INTO nation VALUES (${id}, '${name}', '${region}')`);
  }
  
  // Load customers (100)
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO customer VALUES (${i}, 'Cust${i}', ${i % 9}, ${1000 + (i * 137) % 9000})`);
  }
  
  // Load orders (500) 
  const statuses = ['F', 'O', 'P'];
  for (let i = 0; i < 500; i++) {
    const month = String(1 + i % 12).padStart(2, '0');
    const day = String(1 + i % 28).padStart(2, '0');
    const year = 2023 + (i % 3);
    db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, '${year}-${month}-${day}', ${100 + (i * 73) % 10000}, '${statuses[i % 3]}')`);
  }
  
  // Load line items (2000)
  const parts = ['Bolt', 'Nut', 'Screw', 'Widget', 'Gasket', 'Bracket', 'Spring', 'Washer'];
  const flags = ['R', 'A', 'N'];
  const lstatuses = ['F', 'O'];
  for (let i = 0; i < 2000; i++) {
    const qty = 1 + (i * 7) % 50;
    const price = 100 + (i * 13) % 5000;
    const disc = (i * 3) % 10;
    const tax = (i * 5) % 8;
    const shipMonth = String(1 + i % 12).padStart(2, '0');
    db.execute(`INSERT INTO lineitem VALUES (${i}, ${i % 500}, '${parts[i % 8]}', ${qty}, ${price}, ${disc}, ${tax}, '2024-${shipMonth}-15', '${flags[i % 3]}', '${lstatuses[i % 2]}')`);
  }
  
  db.execute('ANALYZE nation');
  db.execute('ANALYZE customer');
  db.execute('ANALYZE orders');
  db.execute('ANALYZE lineitem');
});

describe('TPC-H Mini Benchmark', () => {
  it('Q1: Pricing Summary — aggregate by return_flag and line_status', () => {
    const r = db.execute(`
      SELECT return_flag, line_status,
        SUM(quantity) as sum_qty,
        SUM(price) as sum_base_price,
        SUM(price * (100 - discount) / 100) as sum_disc_price,
        COUNT(*) as count_order
      FROM lineitem
      WHERE ship_date <= '2024-06-15'
      GROUP BY return_flag, line_status
      ORDER BY return_flag, line_status
    `);
    assert.ok(r.rows.length > 0, 'Should return aggregated results');
    // 3 flags × 2 statuses = up to 6 groups
    assert.ok(r.rows.length <= 6);
    for (const row of r.rows) {
      assert.ok(row.sum_qty > 0);
      assert.ok(row.count_order > 0);
    }
  });

  it('Q3: Shipping Priority — top unshipped orders', () => {
    const r = db.execute(`
      SELECT o.id as order_id, SUM(l.price * (100 - l.discount) / 100) as revenue, o.order_date
      FROM customer c
      JOIN orders o ON c.id = o.customer_id
      JOIN lineitem l ON o.id = l.order_id
      WHERE o.order_date < '2024-06-01'
        AND l.ship_date > '2024-06-01'
      GROUP BY o.id, o.order_date
      ORDER BY revenue DESC
      LIMIT 10
    `);
    assert.ok(r.rows.length <= 10);
    if (r.rows.length > 1) {
      assert.ok(r.rows[0].revenue >= r.rows[1].revenue, 'Ordered by revenue DESC');
    }
  });

  it('Q5: Local Supplier Volume — revenue by nation', () => {
    const r = db.execute(`
      SELECT n.name as nation, SUM(l.price * (100 - l.discount) / 100) as revenue
      FROM customer c
      JOIN orders o ON c.id = o.customer_id
      JOIN lineitem l ON o.id = l.order_id
      JOIN nation n ON c.nation_id = n.id
      WHERE n.region = 'EUROPE'
        AND o.order_date >= '2024-01-01' AND o.order_date < '2025-01-01'
      GROUP BY n.name
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length > 0, 'European nations should have orders');
    assert.ok(r.rows.length <= 3, 'Max 3 European nations');
  });

  it('Q6: Forecasting Revenue — simple aggregate with range filters', () => {
    const r = db.execute(`
      SELECT SUM(price * discount / 100) as revenue
      FROM lineitem
      WHERE ship_date >= '2024-01-01' AND ship_date < '2025-01-01'
        AND discount BETWEEN 2 AND 7
        AND quantity < 25
    `);
    assert.equal(r.rows.length, 1);
    assert.ok(r.rows[0].revenue > 0, 'Should have forecasted revenue');
  });

  it('Q10: Returned Item Reporting — customers with returns', () => {
    const r = db.execute(`
      SELECT c.id as customer_id, c.name, n.name as nation,
        SUM(l.price * (100 - l.discount) / 100) as revenue
      FROM customer c
      JOIN orders o ON c.id = o.customer_id
      JOIN lineitem l ON o.id = l.order_id
      JOIN nation n ON c.nation_id = n.id
      WHERE o.order_date >= '2024-01-01' AND o.order_date < '2024-07-01'
        AND l.return_flag = 'R'
      GROUP BY c.id, c.name, n.name
      ORDER BY revenue DESC
      LIMIT 20
    `);
    assert.ok(r.rows.length > 0, 'Should find customers with returns');
    assert.ok(r.rows.length <= 20);
  });

  it('additional: Window function over TPC-H data', () => {
    const r = db.execute(`
      SELECT n.name as nation, n.region,
        SUM(o.total_price) as total_orders,
        RANK() OVER (PARTITION BY n.region ORDER BY SUM(o.total_price) DESC) as region_rank
      FROM customer c
      JOIN orders o ON c.id = o.customer_id
      JOIN nation n ON c.nation_id = n.id
      GROUP BY n.name, n.region
      ORDER BY n.region, region_rank
    `);
    assert.ok(r.rows.length > 0);
    // Each region should have ranks starting from 1
    const regions = [...new Set(r.rows.map(r => r.region))];
    for (const region of regions) {
      const regionRows = r.rows.filter(r => r.region === region);
      assert.equal(regionRows[0].region_rank, 1, `${region} should have rank 1`);
    }
  });

  it('additional: CTE with TPC-H data', () => {
    const r = db.execute(`
      WITH customer_totals AS (
        SELECT c.id, c.name, SUM(o.total_price) as total_spent
        FROM customer c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name
      ),
      avg_spending AS (
        SELECT AVG(total_spent) as avg_total FROM customer_totals
      )
      SELECT ct.name, ct.total_spent
      FROM customer_totals ct, avg_spending a
      WHERE ct.total_spent > a.avg_total * 0.5
      ORDER BY ct.total_spent DESC
      LIMIT 10
    `);
    assert.ok(r.rows.length > 0, 'Should find high-spending customers');
    assert.ok(r.rows.length <= 10);
  });
});
