// grouping-extensions.test.js — GROUPING SETS, CUBE, ROLLUP

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUPING SETS', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'East', 'Widget', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'East', 'Gadget', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'West', 'Widget', 150)");
    db.execute("INSERT INTO sales VALUES (4, 'West', 'Gadget', 250)");
  });

  it('GROUPING SETS with subtotals', () => {
    const r = db.execute(`
      SELECT region, product, SUM(amount) as total
      FROM sales
      GROUP BY GROUPING SETS ((region), (product), ())
      ORDER BY region, product
    `);
    assert.equal(r.rows.length, 5);
    // Grand total
    const grandTotal = r.rows.find(r => r.region == null && r.product == null);
    assert.equal(grandTotal.total, 700);
    // Region subtotals
    const east = r.rows.find(r => r.region === 'East' && r.product == null);
    assert.equal(east.total, 300);
    const west = r.rows.find(r => r.region === 'West' && r.product == null);
    assert.equal(west.total, 400);
  });

  it('ROLLUP creates hierarchy', () => {
    const r = db.execute(`
      SELECT region, product, SUM(amount) as total
      FROM sales
      GROUP BY ROLLUP (region, product)
      ORDER BY region, product
    `);
    assert.equal(r.rows.length, 7); // 4 details + 2 region subtotals + 1 grand total
    // Grand total
    const grand = r.rows.find(r => r.region == null && r.product == null);
    assert.equal(grand.total, 700);
  });

  it('CUBE creates all combinations', () => {
    const r = db.execute(`
      SELECT region, product, SUM(amount) as total
      FROM sales
      GROUP BY CUBE (region, product)
      ORDER BY region, product
    `);
    assert.equal(r.rows.length, 9); // 4 details + 2 region + 2 product + 1 grand
    // Product-only subtotals exist
    const widgetTotal = r.rows.find(r => r.region == null && r.product === 'Widget');
    assert.equal(widgetTotal.total, 250);
  });

  it('ROLLUP with single column', () => {
    const r = db.execute(`
      SELECT region, SUM(amount) as total
      FROM sales
      GROUP BY ROLLUP (region)
      ORDER BY region
    `);
    assert.equal(r.rows.length, 3); // East, West, grand total
    const grand = r.rows.find(r => r.region == null);
    assert.equal(grand.total, 700);
  });

  it('CUBE with COUNT', () => {
    const r = db.execute(`
      SELECT region, product, COUNT(*) as cnt
      FROM sales
      GROUP BY CUBE (region, product)
      ORDER BY region, product
    `);
    const grand = r.rows.find(r => r.region == null && r.product == null);
    assert.equal(grand.cnt, 4);
  });
});
