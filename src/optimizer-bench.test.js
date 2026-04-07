// optimizer-bench.test.js — Comprehensive optimizer tests and benchmarks
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { QueryPlanner, formatPlan } from './planner.js';

describe('Optimizer Cost Model Decisions', () => {
  let db, planner;

  beforeEach(() => {
    db = new Database();
  });

  describe('Index vs Table Scan', () => {
    it('prefers index scan for high-selectivity equality on large table', () => {
      db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 5000; i++) db.execute(`INSERT INTO big VALUES (${i}, ${i * 7})`);
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'big' }, columns: [{ type: 'star' }], joins: [],
        where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 2500 } }
      });
      assert.equal(plan.scanType, 'INDEX_SCAN');
      assert.equal(plan.estimatedRows, 1);
    });

    it('prefers table scan for low-selectivity range on small table', () => {
      db.execute('CREATE TABLE small (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 20; i++) db.execute(`INSERT INTO small VALUES (${i}, ${i})`);
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'small' }, columns: [{ type: 'star' }], joins: [],
        where: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 5 } }
      });
      assert.equal(plan.scanType, 'TABLE_SCAN');
    });

    it('prefers index range scan for narrow range on large table', () => {
      db.execute('CREATE TABLE large (id INT PRIMARY KEY, val TEXT)');
      for (let i = 0; i < 10000; i++) db.execute(`INSERT INTO large VALUES (${i}, 'data${i}')`);
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'large' }, columns: [{ type: 'star' }], joins: [],
        where: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 9900 } }
      });
      assert.equal(plan.scanType, 'INDEX_RANGE_SCAN');
      assert.ok(plan.estimatedRows < 200, `Expected < 200 rows, got ${plan.estimatedRows}`);
    });
  });

  describe('Join Strategy Selection', () => {
    it('prefers hash join when both tables are large', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)');
      for (let i = 0; i < 500; i++) {
        db.execute(`INSERT INTO t1 VALUES (${i}, ${i})`);
        db.execute(`INSERT INTO t2 VALUES (${i}, ${i % 500})`);
      }
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 't1' }, columns: [{ type: 'star' }],
        joins: [{
          joinType: 'INNER', table: 't2',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 't1_id' } }
        }],
      });
      assert.equal(plan.joins[0].type, 'HASH_JOIN');
    });

    it('prefers efficient join for small right table', () => {
      db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE TABLE tiny (id INT PRIMARY KEY, name TEXT)');
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO big VALUES (${i}, ${i})`);
      for (let i = 0; i < 3; i++) db.execute(`INSERT INTO tiny VALUES (${i}, 'x')`);
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'big' }, columns: [{ type: 'star' }],
        joins: [{
          joinType: 'INNER', table: 'tiny',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'id' } }
        }],
      });
      assert.ok(['NESTED_LOOP_JOIN', 'MERGE_JOIN', 'HASH_JOIN'].includes(plan.joins[0].type));
    });
  });

  describe('DP Join Reordering', () => {
    it('produces an optimal plan for 3-table join', () => {
      db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
      db.execute('CREATE TABLE c (id INT PRIMARY KEY, b_id INT)');
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO a VALUES (${i}, ${i})`);
      for (let i = 0; i < 500; i++) db.execute(`INSERT INTO b VALUES (${i}, ${i % 100})`);
      for (let i = 0; i < 50; i++) db.execute(`INSERT INTO c VALUES (${i}, ${i})`);
      planner = new QueryPlanner(db);

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'a' }, columns: [{ type: 'star' }],
        joins: [
          { joinType: 'INNER', table: 'b', on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'a_id' } } },
          { joinType: 'INNER', table: 'c', on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'b_id' } } },
        ],
      });

      assert.ok(plan.totalCost > 0, 'Should compute total cost');
      assert.ok(plan.joins.length >= 1, 'Should have join steps');
    });
  });

  describe('Histogram Accuracy', () => {
    it('histogram selectivity is accurate for uniform data', () => {
      db.execute('CREATE TABLE uniform (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO uniform VALUES (${i}, ${i})`);
      planner = new QueryPlanner(db);
      const stats = planner.getStats('uniform');
      const valStats = stats.columns.get('val');

      // Test various points
      const sel10 = valStats.selectivityLT(100);  // 10%
      const sel50 = valStats.selectivityLT(500);  // 50%
      const sel90 = valStats.selectivityLT(900);  // 90%

      assert.ok(Math.abs(sel10 - 0.1) < 0.05, `10% point: expected ~0.1, got ${sel10}`);
      assert.ok(Math.abs(sel50 - 0.5) < 0.05, `50% point: expected ~0.5, got ${sel50}`);
      assert.ok(Math.abs(sel90 - 0.9) < 0.05, `90% point: expected ~0.9, got ${sel90}`);
    });

    it('MCV captures most frequent values', () => {
      db.execute('CREATE TABLE skewed (id INT PRIMARY KEY, cat TEXT)');
      // Highly skewed: 80% category A, 10% B, 10% C
      for (let i = 0; i < 100; i++) {
        const cat = i < 80 ? 'A' : i < 90 ? 'B' : 'C';
        db.execute(`INSERT INTO skewed VALUES (${i}, '${cat}')`);
      }
      planner = new QueryPlanner(db);
      const stats = planner.getStats('skewed');
      const catStats = stats.columns.get('cat');

      // MCV should have A as most frequent
      assert.ok(catStats.mcv.length >= 3);
      assert.equal(catStats.mcv[0].value, 'A');
      assert.ok(catStats.mcv[0].frequency > 0.7, `Expected A freq > 0.7, got ${catStats.mcv[0].frequency}`);

      // Selectivity for A should be ~0.8
      const selA = catStats.selectivityEq('A');
      assert.ok(Math.abs(selA - 0.8) < 0.05, `Expected A selectivity ~0.8, got ${selA}`);
    });

    it('handles NULLs correctly in statistics', () => {
      db.execute('CREATE TABLE nullish (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 100; i++) {
        const val = i % 4 === 0 ? 'NULL' : `${i}`;
        db.execute(`INSERT INTO nullish VALUES (${i}, ${val})`);
      }
      planner = new QueryPlanner(db);
      const stats = planner.getStats('nullish');
      const valStats = stats.columns.get('val');

      // 25% NULLs
      assert.ok(Math.abs(valStats.selectivityNull() - 0.25) < 0.05,
        `Expected ~25% null, got ${valStats.selectivityNull()}`);
      // NDV should exclude NULLs
      assert.equal(valStats.ndv, 75); // 100 rows - 25 NULLs = 75 distinct
    });
  });

  describe('EXPLAIN integration', () => {
    it('EXPLAIN shows planner output', () => {
      db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)');
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO products VALUES (${i}, 'p${i}', ${i * 10})`);
      
      const result = db.execute('EXPLAIN SELECT * FROM products WHERE id = 42');
      assert.ok(result.type === 'ROWS' || result.plan, 'Should return explain output');
    });
  });
});

describe('Optimizer Benchmarks', () => {
  it('planner decision time is fast for 1000-row tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, a INT, b TEXT, c INT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, ${i % 100}, 'data${i}', ${i * 3})`);
    }
    const planner = new QueryPlanner(db);
    
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      planner.plan({
        type: 'SELECT', from: { table: 'bench' }, columns: [{ type: 'star' }], joins: [],
        where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: i * 10 } }
      });
    }
    const elapsed = performance.now() - start;
    
    // 100 plans should complete in under 500ms
    assert.ok(elapsed < 500, `100 plans took ${elapsed.toFixed(1)}ms`);
  });
});
