// stress-test.test.js — Performance stress tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Stress Tests', () => {
  it('10k row aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, region TEXT, product TEXT, amount INT)');
    
    const regions = ['East', 'West', 'North', 'South'];
    const products = ['Widget', 'Gadget', 'Doohickey', 'Thingamajig'];
    
    // Bulk insert 10000 rows via CSV for speed
    const csvLines = ['id,region,product,amount'];
    for (let i = 0; i < 10000; i++) {
      csvLines.push(`${i},${regions[i % 4]},${products[i % 4]},${(i % 100) + 1}`);
    }
    db.copyFrom('orders', csvLines.join('\n'));
    
    const start = performance.now();
    const r = db.execute(`
      SELECT region, product, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt
      FROM orders
      GROUP BY region, product
      ORDER BY total DESC
    `);
    const elapsed = performance.now() - start;
    
    assert.equal(r.rows.length, 16); // 4 regions × 4 products
    assert.ok(elapsed < 5000, `Aggregation took ${elapsed}ms (should be <5s)`);
    console.log(`    10k aggregation: ${elapsed.toFixed(0)}ms`);
  });

  it('10k row JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, name TEXT)');
    
    const csv1 = ['id,val'];
    const csv2 = ['id,name'];
    for (let i = 0; i < 10000; i++) csv1.push(`${i},${i * 10}`);
    for (let i = 0; i < 100; i++) csv2.push(`${i},name${i}`);
    db.copyFrom('t1', csv1.join('\n'));
    db.copyFrom('t2', csv2.join('\n'));
    
    const start = performance.now();
    const r = db.execute('SELECT t2.name, SUM(t1.val) AS total FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY t2.name ORDER BY total DESC LIMIT 5');
    const elapsed = performance.now() - start;
    
    assert.equal(r.rows.length, 5);
    assert.ok(elapsed < 5000, `JOIN took ${elapsed}ms (should be <5s)`);
    console.log(`    10k JOIN + aggregate: ${elapsed.toFixed(0)}ms`);
  });

  it('window function over 5k rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT, dept TEXT, amount INT)');
    
    const depts = ['A', 'B', 'C', 'D', 'E'];
    const csv = ['id,dept,amount'];
    for (let i = 0; i < 5000; i++) csv.push(`${i},${depts[i % 5]},${(i % 200) + 1}`);
    db.copyFrom('sales', csv.join('\n'));
    
    const start = performance.now();
    const r = db.execute(`
      SELECT dept, amount,
        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) AS rn,
        SUM(amount) OVER (PARTITION BY dept) AS dept_total
      FROM sales
      ORDER BY dept, rn
      LIMIT 25
    `);
    const elapsed = performance.now() - start;
    
    assert.equal(r.rows.length, 25);
    assert.ok(elapsed < 10000, `Window took ${elapsed}ms (should be <10s)`);
    console.log(`    5k window function: ${elapsed.toFixed(0)}ms`);
  });

  it('recursive CTE depth 1000', () => {
    const db = new Database();
    const start = performance.now();
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 1000
      )
      SELECT SUM(n) AS total FROM nums
    `);
    const elapsed = performance.now() - start;
    
    assert.equal(r.rows[0].total, 500500); // 1+2+...+1000
    assert.ok(elapsed < 5000, `Recursive CTE took ${elapsed}ms`);
    console.log(`    Recursive CTE 1000: ${elapsed.toFixed(0)}ms`);
  });
});
