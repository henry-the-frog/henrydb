// showcase.test.js — HenryDB feature showcase and integration tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('HenryDB Feature Showcase', () => {
  it('complete analytics pipeline', () => {
    const db = new Database();
    
    // Schema
    db.execute('CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, category TEXT, price INT)');
    db.execute('CREATE TABLE orders (id SERIAL PRIMARY KEY, product_id INT, qty INT, order_date TEXT)');
    db.execute('CREATE INDEX idx_orders_product ON orders(product_id)');
    
    // Data
    const products = [
      [1, 'Widget A', 'widgets', 25], [2, 'Widget B', 'widgets', 35],
      [3, 'Gadget X', 'gadgets', 50], [4, 'Gadget Y', 'gadgets', 75],
      [5, 'Thing Z', 'things', 100]
    ];
    for (const [id, name, cat, price] of products) {
      db.execute(`INSERT INTO products VALUES (${id}, '${name}', '${cat}', ${price})`);
    }
    
    for (let i = 1; i <= 50; i++) {
      const pid = (i % 5) + 1;
      const qty = (i % 7) + 1;
      const month = ((i % 3) + 1).toString().padStart(2, '0');
      db.execute(`INSERT INTO orders VALUES (${i}, ${pid}, ${qty}, '2026-${month}-15')`);
    }
    
    // Complex analytics query
    const r = db.execute(`
      WITH order_details AS (
        SELECT o.id, p.name, p.category, p.price * o.qty as revenue, o.order_date
        FROM orders o JOIN products p ON o.product_id = p.id
      ),
      category_stats AS (
        SELECT category,
               COUNT(*) as num_orders,
               SUM(revenue) as total_revenue,
               AVG(revenue) as avg_revenue,
               STDDEV_POP(revenue) as revenue_sd,
               PERCENTILE_CONT(revenue, 0.5) as median_revenue
        FROM order_details
        GROUP BY category
      )
      SELECT category, num_orders, total_revenue, 
             ROUND(avg_revenue, 2) as avg_rev,
             ROUND(revenue_sd, 2) as sd,
             median_revenue,
             RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
      FROM category_stats
      ORDER BY revenue_rank
    `);
    
    assert.ok(r.rows.length >= 3);
    assert.equal(r.rows[0].revenue_rank, 1);
    for (const row of r.rows) {
      assert.ok(row.total_revenue > 0);
      assert.ok(row.num_orders > 0);
    }
  });

  it('time series analysis', () => {
    const db = new Database();
    db.execute('CREATE TABLE metrics (ts TEXT, cpu_pct FLOAT, mem_mb INT)');
    
    for (let h = 0; h < 24; h++) {
      const ts = `2026-04-19T${h.toString().padStart(2, '0')}:00:00`;
      const cpu = 20 + Math.sin(h / 4) * 30 + (h > 8 && h < 18 ? 20 : 0);
      const mem = 4000 + h * 50;
      db.execute(`INSERT INTO metrics VALUES ('${ts}', ${cpu.toFixed(1)}, ${mem})`);
    }
    
    const r = db.execute(`
      SELECT ts, cpu_pct,
             AVG(cpu_pct) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as cpu_smooth,
             mem_mb,
             LAG(mem_mb) OVER (ORDER BY ts) as prev_mem,
             LEAD(mem_mb) OVER (ORDER BY ts) as next_mem
      FROM metrics
      ORDER BY ts
    `);
    assert.equal(r.rows.length, 24);
    assert.equal(r.rows[0].prev_mem, null); // First row has no previous
    assert.ok(r.rows[12].cpu_smooth > 0); // Smoothed value should be positive
  });

  it('data quality checks with aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE raw_data (id INT, value FLOAT, source TEXT)');
    db.execute("INSERT INTO raw_data VALUES (1,10.5,'A'),(2,NULL,'A'),(3,20.3,'B'),(4,-5.0,'B'),(5,999.9,'A')");
    
    const r = db.execute(`
      SELECT source,
             COUNT(*) as total,
             COUNT(value) as non_null,
             MIN(value) as min_val,
             MAX(value) as max_val,
             PERCENTILE_CONT(value, 0.5) as median,
             CASE WHEN MAX(value) - MIN(value) > 100 THEN 'outliers detected' ELSE 'clean' END as quality
      FROM raw_data
      GROUP BY source
      ORDER BY source
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].source, 'A');
    assert.equal(r.rows[0].total, 3);
    assert.equal(r.rows[0].non_null, 2);
    assert.equal(r.rows[0].quality, 'outliers detected'); // 10.5 to 999.9
    assert.equal(r.rows[1].quality, 'clean'); // -5 to 20.3
  });

  it('savepoint-based transaction with analytics', () => {
    const db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)');
    db.execute("INSERT INTO accounts VALUES (1,'alice',1000),(2,'bob',500),(3,'charlie',750)");
    
    // Begin multi-step transaction with savepoints
    db.execute('SAVEPOINT before_transfers');
    
    // Transfer 200 from alice to bob
    db.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 2');
    
    db.execute('SAVEPOINT after_first_transfer');
    
    // Try to transfer 1000 from charlie (would overdraft)
    db.execute('UPDATE accounts SET balance = balance - 1000 WHERE id = 3');
    
    // Check: charlie is negative → rollback this transfer only
    const charlie = db.execute('SELECT balance FROM accounts WHERE id = 3').rows[0].balance;
    assert.ok(charlie < 0);
    db.execute('ROLLBACK TO after_first_transfer');
    
    // Verify: first transfer kept, second rolled back
    const final = db.execute(`
      SELECT name, balance,
             RANK() OVER (ORDER BY balance DESC) as rank
      FROM accounts
      ORDER BY balance DESC
    `);
    assert.equal(final.rows[0].name, 'alice');
    assert.equal(final.rows[0].balance, 800); // 1000 - 200
    assert.equal(final.rows[1].name, 'charlie');
    assert.equal(final.rows[1].balance, 750); // Restored
    assert.equal(final.rows[2].name, 'bob');
    assert.equal(final.rows[2].balance, 700); // 500 + 200
  });
});
