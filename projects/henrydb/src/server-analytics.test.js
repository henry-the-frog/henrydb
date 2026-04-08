// server-analytics.test.js — Analytics/BI queries through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15519;

describe('Analytics & BI Queries', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    // Fact table: sales
    await client.query('CREATE TABLE sales (id INTEGER, product_id INTEGER, region TEXT, amount REAL, quantity INTEGER, sale_date TEXT)');
    // Dimension: products
    await client.query('CREATE TABLE dim_products (id INTEGER, name TEXT, category TEXT)');
    // Dimension: regions
    await client.query('CREATE TABLE dim_regions (name TEXT, country TEXT, population INTEGER)');
    
    // Products
    await client.query("INSERT INTO dim_products VALUES (1, 'Widget A', 'hardware')");
    await client.query("INSERT INTO dim_products VALUES (2, 'Widget B', 'hardware')");
    await client.query("INSERT INTO dim_products VALUES (3, 'Service X', 'software')");
    await client.query("INSERT INTO dim_products VALUES (4, 'Service Y', 'software')");
    
    // Regions
    await client.query("INSERT INTO dim_regions VALUES ('North', 'US', 5000000)");
    await client.query("INSERT INTO dim_regions VALUES ('South', 'US', 8000000)");
    await client.query("INSERT INTO dim_regions VALUES ('East', 'US', 12000000)");
    await client.query("INSERT INTO dim_regions VALUES ('West', 'US', 10000000)");
    
    // 100 sales records
    const regions = ['North', 'South', 'East', 'West'];
    for (let i = 1; i <= 100; i++) {
      const pid = (i % 4) + 1;
      const region = regions[i % 4];
      const amount = (50 + (i % 10) * 20 + Math.random() * 50).toFixed(2);
      const qty = 1 + (i % 5);
      const day = String(1 + (i % 28)).padStart(2, '0');
      await client.query(`INSERT INTO sales VALUES (${i}, ${pid}, '${region}', ${amount}, ${qty}, '2026-03-${day}')`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('total revenue by region', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT region, SUM(amount) AS revenue, SUM(quantity) AS units FROM sales GROUP BY region ORDER BY revenue DESC'
    );
    assert.strictEqual(result.rows.length, 4);
    for (const row of result.rows) {
      assert.ok(parseFloat(row.revenue) > 0);
    }

    await client.end();
  });

  it('revenue by product category', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT dp.category, SUM(s.amount) AS revenue, COUNT(s.id) AS transactions FROM sales s JOIN dim_products dp ON s.product_id = dp.id GROUP BY dp.category ORDER BY revenue DESC'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('top products by revenue', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT dp.name, SUM(s.amount) AS revenue FROM sales s JOIN dim_products dp ON s.product_id = dp.id GROUP BY dp.name ORDER BY revenue DESC'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('daily sales trend', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT sale_date, COUNT(*) AS transactions, SUM(amount) AS daily_revenue FROM sales GROUP BY sale_date ORDER BY sale_date'
    );
    assert.ok(result.rows.length >= 10);

    await client.end();
  });

  it('average order value by region', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT region, AVG(amount) AS avg_order, MIN(amount) AS min_order, MAX(amount) AS max_order FROM sales GROUP BY region'
    );
    assert.strictEqual(result.rows.length, 4);
    for (const row of result.rows) {
      assert.ok(parseFloat(row.min_order) <= parseFloat(row.avg_order));
      assert.ok(parseFloat(row.avg_order) <= parseFloat(row.max_order));
    }

    await client.end();
  });

  it('revenue per capita by region', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT dr.name, SUM(s.amount) AS revenue, dr.population FROM sales s JOIN dim_regions dr ON s.region = dr.name GROUP BY dr.name, dr.population ORDER BY revenue DESC'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('cross-tabulation: product and region', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT s.region, dp.category, SUM(s.amount) AS revenue FROM sales s JOIN dim_products dp ON s.product_id = dp.id GROUP BY s.region, dp.category ORDER BY s.region'
    );
    assert.ok(result.rows.length >= 4);

    await client.end();
  });

  it('running total simulation', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Get daily totals (simulated cumulative)
    const result = await client.query(
      "SELECT sale_date, SUM(amount) AS daily_total FROM sales GROUP BY sale_date ORDER BY sale_date"
    );
    assert.ok(result.rows.length >= 10);
    
    // Verify each day has positive revenue
    for (const row of result.rows) {
      assert.ok(parseFloat(row.daily_total) > 0);
    }

    await client.end();
  });
});
