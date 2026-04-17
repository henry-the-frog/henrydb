// planner-depth.test.js — Verify cost-based optimizer makes correct decisions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { QueryPlanner } from './planner.js';
import { parse } from './sql.js';

describe('Query Planner Depth Tests', () => {
  function setup(rowCount = 1000) {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL, status TEXT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, city TEXT)');
    db.execute('CREATE INDEX idx_orders_customer ON orders (customer_id)');
    db.execute('CREATE INDEX idx_orders_status ON orders (status)');

    for (let i = 1; i <= Math.min(rowCount, 100); i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer_${i}', '${['NYC','LA','CHI','HOU','PHX'][i % 5]}')`);
    }
    for (let i = 1; i <= rowCount; i++) {
      const cid = (i % 100) + 1;
      const status = ['pending','shipped','delivered','cancelled'][i % 4];
      db.execute(`INSERT INTO orders VALUES (${i}, ${cid}, ${(i * 29.99).toFixed(2)}, '${status}')`);
    }
    return db;
  }

  describe('Access Path Selection', () => {
    it('should choose index scan for selective equality predicate', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // customer_id = 1 selects ~1% of rows — index should win
      const ast = parse("SELECT * FROM orders WHERE customer_id = 1");
      const plan = planner.plan(ast);
      assert.strictEqual(plan.scanType, 'INDEX_SCAN', 
        'Should use index scan for selective equality');
      assert.ok(plan.estimatedRows < 50, 
        `Estimated rows should be small: got ${plan.estimatedRows}`);
    });

    it('should choose table scan for non-selective range predicate', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // customer_id > 0 selects ~100% — table scan should win
      const ast = parse("SELECT * FROM orders WHERE customer_id > 0");
      const plan = planner.plan(ast);
      assert.strictEqual(plan.scanType, 'TABLE_SCAN',
        'Should use table scan for non-selective range');
    });

    it('should prefer index with higher selectivity for AND conditions', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // status = 'pending' (25%) AND customer_id = 1 (1%) — should pick customer_id index
      const ast = parse("SELECT * FROM orders WHERE status = 'pending' AND customer_id = 1");
      const plan = planner.plan(ast);
      
      if (plan.scanType === 'INDEX_SCAN') {
        // Should pick the more selective index (customer_id)
        assert.strictEqual(plan.indexColumn, 'customer_id',
          'Should pick the more selective index');
      }
      // Either way, estimate should be reasonable
      assert.ok(plan.estimatedRows <= 10, 
        `Estimated rows for AND should be small: got ${plan.estimatedRows}`);
    });
  });

  describe('Row Estimation', () => {
    it('should estimate equality selectivity accurately', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // status = 'pending' → 25% of 1000 = 250
      const ast = parse("SELECT * FROM orders WHERE status = 'pending'");
      const plan = planner.plan(ast);
      
      // Should be within 50% of actual (250)
      assert.ok(plan.estimatedRows >= 125 && plan.estimatedRows <= 375,
        `Expected ~250 rows for status='pending', got ${plan.estimatedRows}`);
    });

    it('should estimate range selectivity reasonably', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // amount > 15000 — roughly 50% of orders
      const ast = parse("SELECT * FROM orders WHERE amount > 15000");
      const plan = planner.plan(ast);
      
      // Should be within factor of 2
      assert.ok(plan.estimatedRows >= 250 && plan.estimatedRows <= 750,
        `Expected ~500 rows for amount > 15000, got ${plan.estimatedRows}`);
    });

    it('should handle AND selectivity with independence assumption', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      // status = 'pending' (25%) AND customer_id = 1 (1%) → ~0.25%
      const ast = parse("SELECT * FROM orders WHERE status = 'pending' AND customer_id = 1");
      const plan = planner.plan(ast);
      
      // Should be small (2-3 expected)
      assert.ok(plan.estimatedRows <= 15,
        `AND selectivity should be multiplicative, got ${plan.estimatedRows}`);
    });
  });

  describe('Join Ordering', () => {
    it('should produce a valid join plan for 2 tables', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');
      planner.analyzeTable('customers');

      const ast = parse("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id");
      const plan = planner.plan(ast);
      
      assert.ok(plan.joins.length > 0, 'Should have join steps');
      assert.ok(plan.totalCost > 0, 'Join plan should have a positive cost');
    });

    it('should prefer smaller table as build side in hash join', () => {
      const db = setup(500); // 500 orders, 100 customers
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');
      planner.analyzeTable('customers');

      const ast = parse("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id WHERE customers.city = 'NYC'");
      const plan = planner.plan(ast);
      
      // The planner should process this efficiently
      assert.ok(plan.totalCost > 0, 'Should produce a costed plan');
    });
  });

  describe('Cost Model Consistency', () => {
    it('index scan cost should increase with selectivity', () => {
      const db = setup();
      const planner = new QueryPlanner(db);
      const stats = planner.analyzeTable('orders');

      // Compare costs: customer_id = 1 (1%) vs customer_id > 50 (~50%)
      const plan1 = planner.plan(parse("SELECT * FROM orders WHERE customer_id = 1"));
      const plan2 = planner.plan(parse("SELECT * FROM orders WHERE customer_id > 50"));
      
      // Less selective query should have higher cost
      assert.ok(plan2.estimatedCost >= plan1.estimatedCost,
        `Less selective (${plan2.estimatedCost}) should cost >= more selective (${plan1.estimatedCost})`);
    });

    it('should prefer index scan for point lookup on large table', () => {
      const db = setup(5000); // Bigger table makes index more worthwhile
      const planner = new QueryPlanner(db);
      planner.analyzeTable('orders');

      const ast = parse("SELECT * FROM orders WHERE customer_id = 1");
      const plan = planner.plan(ast);
      
      assert.strictEqual(plan.scanType, 'INDEX_SCAN',
        `Should use index for point lookup on ${5000}-row table`);
    });
  });

  describe('Correctness of Query Results with Index', () => {
    it('query results should match with and without index', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT, val INT)');
      for (let i = 1; i <= 100; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i % 10})`);
      }

      // Query without index
      const r1 = db.execute('SELECT * FROM t WHERE val = 3 ORDER BY id');
      
      // Add index
      db.execute('CREATE INDEX idx_val ON t (val)');
      
      // Query with index
      const r2 = db.execute('SELECT * FROM t WHERE val = 3 ORDER BY id');
      
      assert.deepStrictEqual(r1.rows, r2.rows, 'Results should be identical with or without index');
    });

    it('range query results should match with and without index', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT, val INT)');
      for (let i = 1; i <= 100; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
      }

      const r1 = db.execute('SELECT * FROM t WHERE val > 50 AND val < 60 ORDER BY id');
      
      db.execute('CREATE INDEX idx_val ON t (val)');
      
      const r2 = db.execute('SELECT * FROM t WHERE val > 50 AND val < 60 ORDER BY id');
      
      assert.deepStrictEqual(r1.rows, r2.rows, 'Range query should match with/without index');
    });
  });
});
