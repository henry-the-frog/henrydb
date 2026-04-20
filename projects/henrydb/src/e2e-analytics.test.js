// e2e-analytics.test.js — End-to-end data analysis scenarios
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(a, b, tol = 0.01) { return Math.abs(a - b) < tol; }

function setupAnalyticsDB() {
  const db = new Database();
  
  // Sales data
  db.execute(`CREATE TABLE sales (
    id INT PRIMARY KEY, rep TEXT, region TEXT, product TEXT,
    amount INT, quantity INT, sale_date TEXT
  )`);
  
  const data = [
    [1,'alice','north','widget',500,5,'2026-01-15'],
    [2,'alice','north','gadget',800,2,'2026-02-10'],
    [3,'bob','south','widget',300,3,'2026-01-20'],
    [4,'bob','south','gadget',600,4,'2026-02-15'],
    [5,'charlie','east','widget',400,4,'2026-01-25'],
    [6,'charlie','east','widget',350,3,'2026-03-10'],
    [7,'alice','north','thing',1200,1,'2026-03-01'],
    [8,'bob','south','thing',900,2,'2026-03-15'],
    [9,'dave','west','widget',250,2,'2026-01-30'],
    [10,'dave','west','gadget',700,3,'2026-02-20'],
    [11,'eve','north','widget',450,4,'2026-02-01'],
    [12,'eve','north','gadget',550,2,'2026-03-05'],
    [13,'alice','north','widget',600,6,'2026-03-20'],
    [14,'bob','south','widget',280,3,'2026-03-25'],
    [15,'charlie','east','gadget',750,5,'2026-02-28'],
  ];
  
  for (const row of data) {
    db.execute(`INSERT INTO sales VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', ${row[4]}, ${row[5]}, '${row[6]}')`);
  }
  
  return db;
}

describe('Real-World Analytics Queries', () => {
  it('sales by region with ranking', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT region, SUM(amount) as total,
             RANK() OVER (ORDER BY SUM(amount) DESC) as rank
      FROM sales
      GROUP BY region
      ORDER BY total DESC
    `);
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].rank, 1); // Highest total
  });

  it('rep performance: above/below average', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT rep, SUM(amount) as total,
             CASE 
               WHEN SUM(amount) > (SELECT AVG(rep_total) FROM 
                 (SELECT SUM(amount) as rep_total FROM sales GROUP BY rep) sub)
               THEN 'Above Average'
               ELSE 'Below Average'
             END as performance
      FROM sales
      GROUP BY rep
      ORDER BY total DESC
    `);
    assert.ok(r.rows.length >= 4);
    assert.ok(r.rows.some(row => row.performance === 'Above Average'));
    assert.ok(r.rows.some(row => row.performance === 'Below Average'));
  });

  it('monthly trend with running total', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      WITH monthly AS (
        SELECT DATE_TRUNC('month', sale_date) as month,
               SUM(amount) as monthly_total
        FROM sales
        GROUP BY DATE_TRUNC('month', sale_date)
      )
      SELECT month, monthly_total,
             SUM(monthly_total) OVER (ORDER BY month) as running_total
      FROM monthly
      ORDER BY month
    `);
    assert.equal(r.rows.length, 3); // Jan, Feb, Mar
    // Running total should increase
    assert.ok(r.rows[0].running_total <= r.rows[1].running_total);
    assert.ok(r.rows[1].running_total <= r.rows[2].running_total);
  });

  it('product statistics with percentiles', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT product,
             COUNT(*) as num_sales,
             AVG(amount) as avg_amount,
             PERCENTILE_CONT(amount, 0.5) as median_amount,
             STDDEV_POP(amount) as sd_amount
      FROM sales
      GROUP BY product
      ORDER BY avg_amount DESC
    `);
    assert.ok(r.rows.length >= 3);
    for (const row of r.rows) {
      assert.ok(row.num_sales > 0);
      assert.ok(row.avg_amount > 0);
    }
  });

  it('top N reps per region', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      WITH ranked AS (
        SELECT rep, region, SUM(amount) as total,
               ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) as rn
        FROM sales
        GROUP BY rep, region
      )
      SELECT rep, region, total FROM ranked
      WHERE rn = 1
      ORDER BY total DESC
    `);
    assert.ok(r.rows.length >= 3);
    // Each row should be the top rep in their region
    const regions = new Set(r.rows.map(row => row.region));
    assert.ok(regions.size >= 3); // At least 3 unique regions represented
  });

  it('year-over-year comparison (simulated)', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT 
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as total,
        COUNT(*) as num_sales,
        AVG(amount) as avg_sale
      FROM sales
      GROUP BY DATE_TRUNC('month', sale_date)
      HAVING COUNT(*) >= 3
      ORDER BY month
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.num_sales >= 3);
    }
  });

  it('price-quantity correlation', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT product,
             CORR(amount, quantity) as price_qty_corr,
             REGR_SLOPE(amount, quantity) as price_per_unit
      FROM sales
      GROUP BY product
      HAVING COUNT(*) >= 3
      ORDER BY product
    `);
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      if (row.price_qty_corr !== null) {
        assert.ok(row.price_qty_corr >= -1 && row.price_qty_corr <= 1, 
          `Correlation should be [-1,1], got ${row.price_qty_corr}`);
      }
    }
  });

  it('complex dashboard query', () => {
    const db = setupAnalyticsDB();
    const r = db.execute(`
      SELECT 
        region,
        COUNT(DISTINCT rep) as num_reps,
        COUNT(*) as num_sales,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_sale,
        MIN(amount) as min_sale,
        MAX(amount) as max_sale,
        PERCENTILE_CONT(amount, 0.25) as p25,
        PERCENTILE_CONT(amount, 0.75) as p75
      FROM sales
      GROUP BY region
      ORDER BY total_revenue DESC
    `);
    assert.equal(r.rows.length, 4);
    for (const row of r.rows) {
      assert.ok(row.num_reps > 0);
      assert.ok(row.min_sale <= row.avg_sale);
      assert.ok(row.avg_sale <= row.max_sale);
      assert.ok(row.p25 <= row.p75);
    }
  });

  it('savepoint + analytics: rollback partial import', () => {
    const db = setupAnalyticsDB();
    const before = db.execute('SELECT SUM(amount) as total FROM sales').rows[0].total;
    
    db.execute('SAVEPOINT before_import');
    db.execute("INSERT INTO sales VALUES (100, 'frank', 'west', 'widget', 10000, 1, '2026-04-01')");
    
    // Verify import changed total
    const during = db.execute('SELECT SUM(amount) as total FROM sales').rows[0].total;
    assert.equal(during, before + 10000);
    
    // Rollback bad import
    db.execute('ROLLBACK TO before_import');
    
    // Verify analytics are correct after rollback
    const after = db.execute('SELECT SUM(amount) as total FROM sales').rows[0].total;
    assert.equal(after, before);
    
    // Run complex analytics on rolled-back data
    const r = db.execute(`
      SELECT region, AVG(amount) as avg_amt,
             RANK() OVER (ORDER BY AVG(amount) DESC) as rank
      FROM sales
      GROUP BY region
    `);
    assert.equal(r.rows.length, 4); // Still 4 regions, not 5
  });
});
