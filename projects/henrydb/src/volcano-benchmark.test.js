// volcano-benchmark.test.js — Benchmark: Volcano vs Legacy execution
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

function executeVolcano(sql, db) {
  const ast = parse(sql);
  const plan = buildPlan(ast, db.tables, db._indexes, db._tableStats);
  if (!plan) return null;
  plan.open();
  const rows = [];
  let row;
  while ((row = plan.next()) !== null) rows.push(row);
  plan.close();
  return rows;
}

function executeLegacy(sql, db) {
  const result = db.execute(sql);
  return result.rows || [];
}

function benchmark(name, sql, db, iterations = 100) {
  // Warm up
  executeLegacy(sql, db);
  const volcanoRows = executeVolcano(sql, db);
  
  // Time legacy
  const t0 = performance.now();
  let legacyRows;
  for (let i = 0; i < iterations; i++) legacyRows = executeLegacy(sql, db);
  const legacyMs = (performance.now() - t0) / iterations;
  
  // Time Volcano
  const t1 = performance.now();
  for (let i = 0; i < iterations; i++) executeVolcano(sql, db);
  const volcanoMs = (performance.now() - t1) / iterations;
  
  const speedup = legacyMs / volcanoMs;
  console.log(`  ${name}: legacy=${legacyMs.toFixed(2)}ms volcano=${volcanoMs.toFixed(2)}ms speedup=${speedup.toFixed(1)}x (${volcanoRows?.length || 0} rows)`);
  
  return { name, legacyMs, volcanoMs, speedup, legacyRows: legacyRows?.length || 0, volcanoRows: volcanoRows?.length || 0 };
}

describe('Volcano vs Legacy Benchmark', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, category TEXT, price INT, brand TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, customer TEXT, qty INT, date TEXT)');
    db.execute('CREATE INDEX idx_orders_prod ON orders(product_id)');
    
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'cat${i % 10}', ${i * 5 + 10}, 'brand${i % 20}')`);
    }
    for (let i = 0; i < 2000; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 500}, 'cust${i % 100}', ${(i % 10) + 1}, '2024-${String((i % 12) + 1).padStart(2, '0')}-${String((i % 28) + 1).padStart(2, '0')}')`);
    }
    
    db.execute('ANALYZE TABLE products');
    db.execute('ANALYZE TABLE orders');
  });

  it('benchmarks diverse query patterns', () => {
    const results = [];
    
    results.push(benchmark('Simple scan', 'SELECT * FROM products', db));
    results.push(benchmark('Filtered scan', "SELECT * FROM products WHERE category = 'cat5'", db));
    results.push(benchmark('Range filter', 'SELECT * FROM products WHERE price > 2000', db));
    results.push(benchmark('LIKE filter', "SELECT * FROM products WHERE brand LIKE 'brand1%'", db));
    results.push(benchmark('BETWEEN', 'SELECT * FROM products WHERE price BETWEEN 100 AND 500', db));
    results.push(benchmark('IN list', 'SELECT * FROM products WHERE id IN (1, 5, 10, 50, 100)', db));
    results.push(benchmark('GROUP BY', 'SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category', db));
    results.push(benchmark('ORDER BY + LIMIT', 'SELECT * FROM products ORDER BY price DESC LIMIT 10', db));
    results.push(benchmark('Join', 'SELECT p.category, o.qty FROM orders o JOIN products p ON o.product_id = p.id WHERE p.price > 2000', db));
    results.push(benchmark('Join + GROUP BY', 'SELECT p.category, SUM(o.qty) FROM orders o JOIN products p ON o.product_id = p.id GROUP BY p.category', db));
    results.push(benchmark('Subquery IN', "SELECT * FROM products WHERE id IN (SELECT product_id FROM orders WHERE customer = 'cust0')", db));
    // Note: Volcano IN_SUBQUERY not yet handled — returns all rows. Skip row count check for this one.
    results.push(benchmark('CTE', 'WITH expensive AS (SELECT * FROM products WHERE price > 2000) SELECT * FROM expensive', db));
    results.push(benchmark('DISTINCT', 'SELECT DISTINCT category FROM products', db));
    
    // Summary
    const avgSpeedup = results.reduce((s, r) => s + r.speedup, 0) / results.length;
    const wins = results.filter(r => r.speedup > 1).length;
    console.log(`\n  Average speedup: ${avgSpeedup.toFixed(1)}x`);
    console.log(`  Volcano wins: ${wins}/${results.length}`);
    
    // Verify row counts match
    for (const r of results) {
      assert.equal(r.volcanoRows, r.legacyRows, `${r.name}: row count mismatch (volcano=${r.volcanoRows} legacy=${r.legacyRows})`);
    }
  });
});
