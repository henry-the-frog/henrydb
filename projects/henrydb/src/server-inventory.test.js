// server-inventory.test.js — Inventory management through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15536;

describe('Inventory Management', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE warehouses (id INTEGER, name TEXT, location TEXT)');
    await client.query('CREATE TABLE inventory (id INTEGER, warehouse_id INTEGER, product TEXT, sku TEXT, quantity INTEGER, min_stock INTEGER, last_restocked TEXT)');
    await client.query('CREATE TABLE stock_movements (id INTEGER, inventory_id INTEGER, type TEXT, quantity INTEGER, reason TEXT, ts TEXT)');
    
    // Warehouses
    await client.query("INSERT INTO warehouses VALUES (1, 'Main Warehouse', 'Denver')");
    await client.query("INSERT INTO warehouses VALUES (2, 'East Hub', 'New York')");
    
    // Inventory
    await client.query("INSERT INTO inventory VALUES (1, 1, 'Widget A', 'WDG-A-001', 150, 50, '2026-04-01')");
    await client.query("INSERT INTO inventory VALUES (2, 1, 'Widget B', 'WDG-B-001', 30, 50, '2026-03-15')");
    await client.query("INSERT INTO inventory VALUES (3, 2, 'Widget A', 'WDG-A-001', 200, 75, '2026-04-05')");
    await client.query("INSERT INTO inventory VALUES (4, 2, 'Gadget X', 'GDG-X-001', 10, 25, '2026-03-01')");
    
    // Stock movements
    await client.query("INSERT INTO stock_movements VALUES (1, 1, 'IN', 100, 'restock', '2026-04-01')");
    await client.query("INSERT INTO stock_movements VALUES (2, 1, 'OUT', 20, 'order-1001', '2026-04-03')");
    await client.query("INSERT INTO stock_movements VALUES (3, 2, 'OUT', 15, 'order-1002', '2026-04-04')");
    await client.query("INSERT INTO stock_movements VALUES (4, 4, 'OUT', 5, 'order-1003', '2026-04-05')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('low stock alerts', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT i.product, i.sku, i.quantity, i.min_stock, w.name AS warehouse FROM inventory i JOIN warehouses w ON i.warehouse_id = w.id WHERE i.quantity < i.min_stock'
    );
    assert.ok(result.rows.length >= 2); // Widget B and Gadget X

    await client.end();
  });

  it('total stock across warehouses', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT product, SUM(quantity) AS total_stock FROM inventory GROUP BY product ORDER BY total_stock DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('stock movement history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT sm.type, sm.quantity, sm.reason, sm.ts, i.product FROM stock_movements sm JOIN inventory i ON sm.inventory_id = i.id ORDER BY sm.ts'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('warehouse utilization', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT w.name, COUNT(i.id) AS products, SUM(i.quantity) AS total_units FROM warehouses w JOIN inventory i ON w.id = i.warehouse_id GROUP BY w.name'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('needs restocking (days since last restock)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT product, sku, last_restocked FROM inventory WHERE last_restocked < '2026-04-01' ORDER BY last_restocked"
    );
    assert.ok(result.rows.length >= 1); // Items restocked before April

    await client.end();
  });
});
