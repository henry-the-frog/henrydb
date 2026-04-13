// grouping-sets.test.js — ROLLUP, CUBE, GROUPING SETS
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUPING SETS', () => {
  function setup() {
    const db = new Database();
    db.execute('CREATE TABLE sales (region TEXT, product TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('East', 'A', 100), ('East', 'B', 200), ('West', 'A', 150), ('West', 'B', 250)");
    return db;
  }

  it('ROLLUP produces hierarchical totals', () => {
    const db = setup();
    const r = db.execute('SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP(region, product)');
    
    // Should have: 4 detail + 2 region subtotal + 1 grand total = 7 rows
    assert.equal(r.rows.length, 7);
    
    // Grand total
    const grand = r.rows.find(r => r.region === null && r.product === null);
    assert.equal(grand.total, 700);
    
    // Region subtotals
    const eastSub = r.rows.find(r => r.region === 'East' && r.product === null);
    assert.equal(eastSub.total, 300);
  });

  it('CUBE produces all combinations', () => {
    const db = setup();
    const r = db.execute('SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE(region, product)');
    
    // 4 detail + 2 region + 2 product + 1 grand = 9 rows
    assert.equal(r.rows.length, 9);
    
    // Product subtotals (cross-region)
    const productA = r.rows.find(r => r.region === null && r.product === 'A');
    assert.equal(productA.total, 250); // East A + West A
  });

  it('GROUPING SETS with explicit sets', () => {
    const db = setup();
    const r = db.execute('SELECT region, product, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region), (product))');
    
    // 2 regions + 2 products = 4 rows
    assert.equal(r.rows.length, 4);
    
    const eastRow = r.rows.find(r => r.region === 'East' && r.product === null);
    assert.ok(eastRow);
    assert.equal(eastRow.total, 300);
  });
});
