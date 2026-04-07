import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { QueryPlanner, formatPlan } from './planner.js';

describe('Query Planner', () => {
  let db, planner;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${20 + i % 50})`);
    }
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    for (let i = 0; i < 2000; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 1000}, ${100 + i * 10})`);
    }
    planner = new QueryPlanner(db);
  });

  describe('Table statistics', () => {
    it('analyzes table with histograms', () => {
      const stats = planner.analyzeTable('users');
      assert.equal(stats.rowCount, 1000);
      assert.ok(stats.avgRowWidth > 0);
      assert.ok(stats.indexedColumns.includes('id'));
      // Check column-level stats
      const idStats = stats.columns.get('id');
      assert.ok(idStats);
      assert.equal(idStats.ndv, 1000); // 1000 unique IDs
      assert.equal(idStats.min, 0);
      assert.equal(idStats.max, 999);
      assert.ok(idStats.histogram.length > 0);
      assert.ok(idStats.mcv.length > 0);
    });

    it('analyzes columns with repeated values', () => {
      const stats = planner.getStats('users');
      const ageStats = stats.columns.get('age');
      assert.equal(ageStats.ndv, 50); // 1000 users but only 50 unique ages (20-69)
      assert.equal(ageStats.min, 20);
      assert.equal(ageStats.max, 69);
      // Each age appears 2 times → MCV should reflect that
      assert.ok(ageStats.mcv.length > 0);
    });

    it('analyzes empty table', () => {
      db.execute('CREATE TABLE empty (id INT)');
      const stats = planner.analyzeTable('empty');
      assert.equal(stats.rowCount, 0);
    });
  });

  describe('Scan type selection', () => {
    it('chooses table scan without WHERE', () => {
      const plan = planner.plan({ type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }], joins: [] });
      assert.equal(plan.scanType, 'TABLE_SCAN');
    });

    it('chooses index scan for PK equality', () => {
      const plan = planner.plan({
        type: 'SELECT',
        from: { table: 'users' },
        columns: [{ type: 'star' }],
        joins: [],
        where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 42 } }
      });
      assert.equal(plan.scanType, 'INDEX_SCAN');
      assert.equal(plan.indexColumn, 'id');
    });

    it('chooses index range scan for narrow PK range', () => {
      const plan = planner.plan({
        type: 'SELECT',
        from: { table: 'users' },
        columns: [{ type: 'star' }],
        joins: [],
        where: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 950 } }
      });
      assert.equal(plan.scanType, 'INDEX_RANGE_SCAN');
    });

    it('falls back to table scan for non-indexed columns', () => {
      const plan = planner.plan({
        type: 'SELECT',
        from: { table: 'users' },
        columns: [{ type: 'star' }],
        joins: [],
        where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'name' }, right: { type: 'literal', value: 'test' } }
      });
      assert.equal(plan.scanType, 'TABLE_SCAN');
    });
  });

  describe('Histogram-based selectivity', () => {
    it('equality selectivity uses NDV', () => {
      const stats = planner.getStats('users');
      const ageStats = stats.columns.get('age');
      // 50 unique ages, so equality selectivity ≈ 1/50 = 2%
      const sel = ageStats.selectivityEq(25);
      assert.ok(sel > 0 && sel < 0.1, `Expected small selectivity, got ${sel}`);
    });

    it('range selectivity uses histogram', () => {
      const stats = planner.getStats('users');
      const idStats = stats.columns.get('id');
      // id < 500 should be ~50% (uniform distribution 0-999)
      const sel = idStats.selectivityLT(500);
      assert.ok(sel > 0.3 && sel < 0.7, `Expected ~50% selectivity, got ${sel}`);
    });

    it('AND reduces selectivity multiplicatively', () => {
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }], joins: [],
        where: {
          type: 'AND',
          left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 25 } },
          right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 40 } },
        }
      });
      assert.ok(plan.estimatedRows < 1000 && plan.estimatedRows > 0,
        `Expected reduced rows, got ${plan.estimatedRows}`);
    });

    it('OR combines selectivity with inclusion-exclusion', () => {
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }], joins: [],
        where: {
          type: 'OR',
          left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 25 } },
          right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 30 } },
        }
      });
      // Two equalities on age: each ~2%, OR ~4% = ~40 rows
      assert.ok(plan.estimatedRows >= 20 && plan.estimatedRows <= 60,
        `Expected ~40 rows for OR, got ${plan.estimatedRows}`);
    });

    it('NULL selectivity reflects actual data', () => {
      db.execute('CREATE TABLE nulls (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO nulls VALUES (1, 10)');
      db.execute('INSERT INTO nulls VALUES (2, NULL)');
      db.execute('INSERT INTO nulls VALUES (3, 30)');
      db.execute('INSERT INTO nulls VALUES (4, NULL)');
      const stats = planner.analyzeTable('nulls');
      const valStats = stats.columns.get('val');
      assert.equal(valStats.nullCount, 2);
      assert.ok(Math.abs(valStats.selectivityNull() - 0.5) < 0.01);
    });
  });

  describe('Join planning', () => {
    it('chooses hash join for large tables', () => {
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }],
        joins: [{
          type: 'JOIN', joinType: 'INNER', table: 'orders',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'user_id' } }
        }],
      });
      assert.equal(plan.joins[0].type, 'HASH_JOIN');
    });

    it('chooses merge or hash join over nested loop for equijoin', () => {
      db.execute('CREATE TABLE tiny (id INT, val INT)');
      for (let i = 0; i < 5; i++) db.execute(`INSERT INTO tiny VALUES (${i}, ${i})`);
      planner.analyzeTable('tiny');

      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }],
        joins: [{
          type: 'JOIN', joinType: 'INNER', table: 'tiny',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'id' } }
        }],
      });
      // With merge join available, should prefer it or hash join over nested loop
      assert.ok(['HASH_JOIN', 'MERGE_JOIN'].includes(plan.joins[0].type),
        `Expected HASH or MERGE join, got ${plan.joins[0].type}`);
    });

    it('DP reorders 3-table join', () => {
      db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT)');
      for (let i = 0; i < 50; i++) db.execute(`INSERT INTO products VALUES (${i}, 'prod${i}')`);
      
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }],
        joins: [
          {
            type: 'JOIN', joinType: 'INNER', table: 'orders',
            on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'user_id' } }
          },
          {
            type: 'JOIN', joinType: 'INNER', table: 'products',
            on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'id' } }
          },
        ],
      });
      // Should produce a plan with total cost
      assert.ok(plan.totalCost > 0, 'DP should compute total cost');
      assert.ok(plan.joins.length >= 1, 'Should have join steps');
    });
  });

  describe('Hash join execution', () => {
    it('produces correct results', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, data TEXT)');
      db.execute("INSERT INTO t1 VALUES (1, 'a')");
      db.execute("INSERT INTO t1 VALUES (2, 'b')");
      db.execute("INSERT INTO t2 VALUES (1, 1, 'x')");
      db.execute("INSERT INTO t2 VALUES (2, 1, 'y')");
      db.execute("INSERT INTO t2 VALUES (3, 2, 'z')");

      const leftRows = [
        { id: 1, 't1.id': 1, val: 'a', 't1.val': 'a' },
        { id: 2, 't1.id': 2, val: 'b', 't1.val': 'b' },
      ];
      const t2Table = db.tables.get('t2');
      const joinOn = {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 't1_id' },
        right: { type: 'column_ref', name: 'id' },
      };
      const result = planner.executeHashJoin(leftRows, t2Table, joinOn, t2Table.schema, 't2');
      assert.equal(result.length, 3);
    });
  });

  describe('Plan formatting', () => {
    it('formats plan as text', () => {
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'users' }, columns: [{ type: 'star' }], joins: [],
      });
      const text = formatPlan(plan);
      assert.ok(text.includes('TABLE_SCAN'));
      assert.ok(text.includes('users'));
    });
  });
});
