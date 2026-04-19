// explain-analyze.test.js — Tests for EXPLAIN ANALYZE with cost info
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN ANALYZE', () => {
  it('shows actual rows and execution time', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 50');
    assert.ok(result.execution_time_ms >= 0);
    assert.equal(result.actual_rows, 1);
    
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Execution Time'));
    assert.ok(plan.includes('Rows Returned: 1'), `Expected Rows Returned in plan: ${plan}`);
  });

  it('shows engine type in plan', () => {
    const db = new Database();
    db.execute('CREATE TABLE bt (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO bt VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM bt WHERE id = 5');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('engine=btree'), `Expected engine=btree in: ${plan}`);
  });

  it('shows heap engine for HeapFile', () => {
    const db = new Database();
    db.execute('CREATE TABLE ht (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO ht VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM ht');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('engine=heap'), `Expected engine=heap in: ${plan}`);
  });

  it('shows selectivity for filtered queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, score INTEGER)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO items VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE score > 90');
    assert.equal(result.actual_rows, 10);
    
    const analysis = result.analysis;
    assert.ok(analysis.some(a => a.selectivity));
  });

  it('shows sort elimination for BTree ORDER BY PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE sorted (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO sorted VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM sorted ORDER BY id ASC');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('SORT_ELIMINATED'), `Expected SORT_ELIMINATED in: ${plan}`);
  });

  it('shows GROUP BY in plan', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)');
    for (let i = 1; i <= 100; i++) {
      const cat = ['A', 'B', 'C'][i % 3];
      db.execute(`INSERT INTO sales VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    const result = db.execute('EXPLAIN ANALYZE SELECT category, SUM(amount) FROM sales GROUP BY category');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('GROUP_BY') || result.analysis.some(a => a.operation === 'GROUP_BY'));
  });

  it('full plan output for complex query', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER) USING BTREE');
    db.execute('CREATE INDEX idx_price ON products (price)');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${i * 5})`);
    }
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE price > 1000 ORDER BY id ASC LIMIT 10');
    
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    console.log('  Plan:\n' + plan.split('\n').map(l => '    ' + l).join('\n'));
    
    assert.ok(result.execution_time_ms >= 0);
    assert.ok(result.actual_rows <= 10); // LIMIT
  });

  it('estimated vs actual row comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INTEGER PRIMARY KEY, flag INTEGER)');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO data VALUES (${i}, ${i % 2})`);
    }
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM data WHERE flag = 0');
    assert.equal(result.actual_rows, 500);
    
    // Estimated rows might differ from actual — that's expected
    const scanStep = result.analysis.find(a => a.estimated_rows);
    if (scanStep) {
      console.log(`  Estimated: ${scanStep.estimated_rows}, Actual: ${scanStep.actual_rows}`);
    }
  });
});
