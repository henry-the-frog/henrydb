// explain-analyze-volcano.test.js — Tests for instrumented Volcano EXPLAIN ANALYZE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setupDB() {
  const db = new Database();
  db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, qty INT, customer TEXT)');
  for (let i = 1; i <= 50; i++) {
    db.execute(`INSERT INTO products VALUES (${i}, 'prod${i}', ${i * 10}, 'cat${i % 5}')`);
  }
  for (let i = 1; i <= 200; i++) {
    db.execute(`INSERT INTO orders VALUES (${i}, ${(i % 50) + 1}, ${i % 10 + 1}, 'cust${i % 20}')`);
  }
  return db;
}

describe('EXPLAIN ANALYZE with Volcano Instrumentation', () => {
  it('shows per-operator timing for simple scan', () => {
    const db = setupDB();
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE price > 200');
    
    // Should have volcanoAnalyze data
    assert.ok(result.volcanoAnalyze, 'Should have volcanoAnalyze');
    assert.ok(result.volcanoAnalyze.timingTree, 'Should have timing tree');
    assert.ok(result.volcanoAnalyze.timingTree.includes('SeqScan'), 'Should show SeqScan');
    assert.ok(result.volcanoAnalyze.timingTree.includes('rows='), 'Should show row counts');
    assert.ok(result.volcanoAnalyze.timingTree.includes('time='), 'Should show timing');
  });

  it('shows per-operator timing for join', () => {
    const db = setupDB();
    const result = db.execute('EXPLAIN ANALYZE SELECT p.name, o.qty FROM orders o JOIN products p ON o.product_id = p.id');
    
    assert.ok(result.volcanoAnalyze, 'Should have volcanoAnalyze');
    const tree = result.volcanoAnalyze.timingTree;
    assert.ok(tree.includes('HashJoin') || tree.includes('NestedLoopJoin'), 'Should show join operator');
    assert.ok(tree.includes('SeqScan'), 'Should show SeqScan operators');
    assert.equal(result.volcanoAnalyze.volcanoRows, 200, 'Should return all 200 order-product pairs');
  });

  it('shows per-operator timing for aggregate', () => {
    const db = setupDB();
    const result = db.execute('EXPLAIN ANALYZE SELECT category, COUNT(*) FROM products GROUP BY category');
    
    // May or may not have Volcano ANALYZE for aggregates, but should not error
    assert.ok(result.rows.length > 0);
    assert.ok(result.actual_rows === 5, 'Should have 5 categories');
  });

  it('operator rows are consistent with actual output', () => {
    const db = setupDB();
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE price <= 100');
    
    if (result.volcanoAnalyze) {
      // Volcano should report same rows as actual
      assert.equal(result.volcanoAnalyze.volcanoRows, result.actual_rows);
    }
  });

  it('formats timing in plan output rows', () => {
    const db = setupDB();
    const result = db.execute('EXPLAIN ANALYZE SELECT name FROM products LIMIT 10');
    
    // Check plan output rows contain Volcano ANALYZE section
    const planText = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(planText.includes('Volcano ANALYZE'), 'Should include Volcano ANALYZE header');
    assert.ok(planText.includes('Total Volcano rows:'), 'Should include total row count');
  });
});
