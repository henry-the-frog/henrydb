// selectivity-accuracy.test.js — Benchmark: estimated vs actual row counts
// Tests the accuracy of the Volcano planner's selectivity estimation
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

// Helper: extract est and actual from Volcano ANALYZE output
function getEstActual(result) {
  if (!result.volcanoAnalyze) return null;
  const tree = result.volcanoAnalyze.timingTree;
  // Find first non-SeqScan operator (the one with filtering/joining)
  const lines = tree.split('\n');
  for (const line of lines) {
    const match = line.match(/est=(\d+)\s+actual=(\d+)/);
    if (match) {
      return { est: parseInt(match[1]), actual: parseInt(match[2]) };
    }
  }
  return null;
}

// Helper: accuracy ratio (1.0 = perfect, <1 = underestimate, >1 = overestimate)
function accuracy(est, actual) {
  if (actual === 0) return est === 0 ? 1.0 : Infinity;
  return est / actual;
}

describe('Selectivity Accuracy Benchmark', () => {
  let db;
  
  before(() => {
    db = new Database();
    // Create test tables
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, category TEXT, price INT, brand TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, customer TEXT, qty INT)');
    db.execute('CREATE TABLE customers (name TEXT PRIMARY KEY, city TEXT, tier INT)');
    
    // Populate: 200 products, 5 categories, 10 brands
    for (let i = 0; i < 200; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'cat${i % 5}', ${i * 10}, 'brand${i % 10}')`);
    }
    // 500 orders, 50 unique customers
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 200}, 'cust${i % 50}', ${(i % 10) + 1})`);
    }
    // 50 customers in 5 cities, 3 tiers
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO customers VALUES ('cust${i}', 'city${i % 5}', ${(i % 3) + 1})`);
    }
    
    // Run ANALYZE on all tables
    db.execute('ANALYZE TABLE products');
    db.execute('ANALYZE TABLE orders');
    db.execute('ANALYZE TABLE customers');
  });

  describe('Equality filters', () => {
    it('single value equality (category = cat0)', () => {
      const r = db.execute("EXPLAIN ANALYZE SELECT * FROM products WHERE category = 'cat0'");
      const ea = getEstActual(r);
      assert.ok(ea, 'Should have est/actual data');
      const ratio = accuracy(ea.est, ea.actual);
      console.log(`  Equality: est=${ea.est} actual=${ea.actual} ratio=${ratio.toFixed(2)}`);
      // With ANALYZE, equality selectivity should be within 50% of actual
      assert.ok(ratio >= 0.5 && ratio <= 2.0, `Accuracy ratio ${ratio.toFixed(2)} out of acceptable range [0.5, 2.0]`);
    });

    it('equality on high-cardinality column (customer)', () => {
      const r = db.execute("EXPLAIN ANALYZE SELECT * FROM orders WHERE customer = 'cust5'");
      const ea = getEstActual(r);
      assert.ok(ea, 'Should have est/actual data');
      const ratio = accuracy(ea.est, ea.actual);
      console.log(`  High-card equality: est=${ea.est} actual=${ea.actual} ratio=${ratio.toFixed(2)}`);
      assert.ok(ratio >= 0.5 && ratio <= 2.0, `Accuracy ratio ${ratio.toFixed(2)} out of range`);
    });
  });

  describe('Range filters', () => {
    it('narrow range (price < 200, ~10% of 200)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE price < 200');
      const ea = getEstActual(r);
      assert.ok(ea, 'Should have est/actual data');
      const ratio = accuracy(ea.est, ea.actual);
      console.log(`  Narrow range: est=${ea.est} actual=${ea.actual} ratio=${ratio.toFixed(2)}`);
      // Range with histogram should be within 3x
      assert.ok(ratio >= 0.33 && ratio <= 3.0, `Accuracy ratio ${ratio.toFixed(2)} out of range [0.33, 3.0]`);
    });

    it('wide range (price > 500, ~75% of 200)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT * FROM products WHERE price > 500');
      const ea = getEstActual(r);
      assert.ok(ea, 'Should have est/actual data');
      const ratio = accuracy(ea.est, ea.actual);
      console.log(`  Wide range: est=${ea.est} actual=${ea.actual} ratio=${ratio.toFixed(2)}`);
      assert.ok(ratio >= 0.33 && ratio <= 3.0, `Accuracy ratio ${ratio.toFixed(2)} out of range`);
    });

    it('medium range (qty BETWEEN 3 AND 7, ~50%)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT * FROM orders WHERE qty > 3 AND qty < 7');
      const ea = getEstActual(r);
      if (ea) {
        const ratio = accuracy(ea.est, ea.actual);
        console.log(`  Medium range: est=${ea.est} actual=${ea.actual} ratio=${ratio.toFixed(2)}`);
      }
    });
  });

  describe('Join selectivity', () => {
    it('1-to-many join (orders → products)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT p.category, o.qty FROM orders o JOIN products p ON o.product_id = p.id');
      if (r.volcanoAnalyze) {
        const tree = r.volcanoAnalyze.timingTree;
        // Find HashJoin line
        const joinLine = tree.split('\n').find(l => l.includes('HashJoin') || l.includes('NestedLoopJoin'));
        if (joinLine) {
          const match = joinLine.match(/est=(\d+)\s+actual=(\d+)/);
          if (match) {
            const est = parseInt(match[1]);
            const actual = parseInt(match[2]);
            const ratio = accuracy(est, actual);
            console.log(`  1-to-many join: est=${est} actual=${actual} ratio=${ratio.toFixed(2)}`);
            assert.ok(ratio >= 0.2 && ratio <= 5.0, `Join accuracy ratio ${ratio.toFixed(2)} out of range`);
          }
        }
      }
    });

    it('join with filter (selective join)', () => {
      const r = db.execute("EXPLAIN ANALYZE SELECT * FROM orders o JOIN customers c ON o.customer = c.name WHERE c.tier = 1");
      if (r.volcanoAnalyze) {
        console.log(`  Selective join: volcanoRows=${r.volcanoAnalyze.volcanoRows} actual=${r.actual_rows}`);
      }
    });
  });

  describe('Aggregate cardinality', () => {
    it('GROUP BY low cardinality (5 categories)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT category, COUNT(*) FROM products GROUP BY category');
      assert.equal(r.actual_rows, 5);
    });

    it('GROUP BY high cardinality (50 customers)', () => {
      const r = db.execute('EXPLAIN ANALYZE SELECT customer, SUM(qty) FROM orders GROUP BY customer');
      assert.equal(r.actual_rows, 50);
    });
  });
});
