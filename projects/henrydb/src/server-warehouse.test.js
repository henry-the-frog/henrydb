// server-warehouse.test.js — Data warehouse OLAP queries
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15530;

describe('Data Warehouse OLAP', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    // Star schema
    await client.query('CREATE TABLE dim_date (date_key INTEGER, year INTEGER, quarter INTEGER, month INTEGER, day_of_week TEXT)');
    await client.query('CREATE TABLE dim_store (store_key INTEGER, name TEXT, city TEXT, state TEXT)');
    await client.query('CREATE TABLE dim_product (product_key INTEGER, name TEXT, brand TEXT, category TEXT, price REAL)');
    await client.query('CREATE TABLE fact_sales (id INTEGER, date_key INTEGER, store_key INTEGER, product_key INTEGER, quantity INTEGER, revenue REAL)');
    
    // Dimension data
    for (let m = 1; m <= 4; m++) {
      for (let d = 1; d <= 7; d++) {
        const key = (m - 1) * 7 + d;
        const dow = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][d - 1];
        await client.query(`INSERT INTO dim_date VALUES (${key}, 2026, ${Math.ceil(m / 3)}, ${m}, '${dow}')`);
      }
    }
    
    await client.query("INSERT INTO dim_store VALUES (1, 'Downtown', 'Denver', 'CO')");
    await client.query("INSERT INTO dim_store VALUES (2, 'Airport', 'Denver', 'CO')");
    await client.query("INSERT INTO dim_store VALUES (3, 'Mall', 'Boulder', 'CO')");
    
    await client.query("INSERT INTO dim_product VALUES (1, 'Widget A', 'Acme', 'hardware', 29.99)");
    await client.query("INSERT INTO dim_product VALUES (2, 'Widget B', 'Acme', 'hardware', 49.99)");
    await client.query("INSERT INTO dim_product VALUES (3, 'Gadget X', 'TechCo', 'electronics', 199.99)");
    await client.query("INSERT INTO dim_product VALUES (4, 'Service Plan', 'TechCo', 'services', 9.99)");
    
    // Fact data: 200 sales
    for (let i = 1; i <= 200; i++) {
      const dateKey = (i % 28) + 1;
      const storeKey = (i % 3) + 1;
      const productKey = (i % 4) + 1;
      const qty = 1 + (i % 5);
      const rev = qty * [29.99, 49.99, 199.99, 9.99][productKey - 1];
      await client.query(`INSERT INTO fact_sales VALUES (${i}, ${dateKey}, ${storeKey}, ${productKey}, ${qty}, ${rev.toFixed(2)})`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('total revenue by store', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT s.name, SUM(f.revenue) AS total_rev FROM fact_sales f JOIN dim_store s ON f.store_key = s.store_key GROUP BY s.name ORDER BY total_rev DESC'
    );
    assert.strictEqual(result.rows.length, 3);
    for (const row of result.rows) assert.ok(parseFloat(row.total_rev) > 0);

    await client.end();
  });

  it('revenue by brand', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.brand, SUM(f.revenue) AS total FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key GROUP BY p.brand ORDER BY total DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('monthly revenue trend', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT d.month, SUM(f.revenue) AS monthly_rev FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key GROUP BY d.month ORDER BY d.month'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('top products by units sold', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name, SUM(f.quantity) AS units FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key GROUP BY p.name ORDER BY units DESC'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('store performance by category', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT s.name AS store, p.category, SUM(f.revenue) AS rev FROM fact_sales f JOIN dim_store s ON f.store_key = s.store_key JOIN dim_product p ON f.product_key = p.product_key GROUP BY s.name, p.category ORDER BY s.name, rev DESC'
    );
    assert.ok(result.rows.length >= 6);

    await client.end();
  });

  it('average basket size by day of week', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT d.day_of_week, AVG(f.revenue) AS avg_sale, COUNT(*) AS transactions FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key GROUP BY d.day_of_week'
    );
    assert.ok(result.rows.length >= 5);

    await client.end();
  });
});
