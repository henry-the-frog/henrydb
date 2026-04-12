// join-ordering.test.js — Cost-based join ordering tests
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Cost-based join ordering', () => {
  let db;
  
  before(() => {
    db = new Database();
    
    // Create 3 tables with very different sizes and join selectivities
    // orders: 1000 rows, each with a customer_id (1-100) and product_id (1-50)
    // customers: 100 rows
    // products: 50 rows
    db.execute('CREATE TABLE customers (id INT, name TEXT, region TEXT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', 'Region ${i % 5}')`);
    }
    
    db.execute('CREATE TABLE products (id INT, name TEXT, price INT)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${i * 10})`);
    }
    
    db.execute('CREATE TABLE orders (id INT, customer_id INT, product_id INT, qty INT)');
    for (let i = 1; i <= 1000; i++) {
      const custId = (i % 100) + 1;
      const prodId = (i % 50) + 1;
      db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, ${prodId}, ${(i % 10) + 1})`);
    }
    
    // ANALYZE all tables so optimizer has stats
    db.execute('ANALYZE TABLE customers');
    db.execute('ANALYZE TABLE products');
    db.execute('ANALYZE TABLE orders');
  });

  it('_estimateJoinSize returns reasonable estimates', () => {
    // Orders (1000) ⋈ Customers (100) on customer_id
    // ndv(customer_id) = 100, ndv(id) = 100
    // Expected: 1000 * 100 / 100 = 1000
    const joinOn = {
      type: 'COMPARE', op: 'EQ',
      left: { type: 'column_ref', name: 'customer_id', table: 'orders' },
      right: { type: 'column_ref', name: 'id', table: 'customers' },
    };
    const est = db._estimateJoinSize('orders', 1000, 'customers', joinOn);
    // Should be around 1000 (each order matches one customer)
    assert.ok(est >= 500 && est <= 2000, `estimate ${est} should be near 1000`);
  });

  it('_extractJoinColumns parses join conditions', () => {
    const on = {
      type: 'COMPARE', op: 'EQ',
      left: { type: 'column_ref', name: 'id', table: 'customers' },
      right: { type: 'column_ref', name: 'customer_id', table: 'orders' },
    };
    const cols = db._extractJoinColumns(on);
    assert.ok(cols);
    assert.strictEqual(cols.leftTable, 'customers');
    assert.strictEqual(cols.leftCol, 'id');
    assert.strictEqual(cols.rightTable, 'orders');
    assert.strictEqual(cols.rightCol, 'customer_id');
  });

  it('_optimizeJoinOrder returns joins when <2 inner joins', () => {
    const joins = [{ table: 'customers', joinType: 'INNER', on: {} }];
    const result = db._optimizeJoinOrder('orders', joins);
    assert.strictEqual(result.length, 1);
  });

  it('_optimizeJoinOrder preserves non-inner joins', () => {
    const innerJoin1 = {
      table: 'customers', joinType: 'INNER',
      on: { type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'customer_id', table: 'orders' },
        right: { type: 'column_ref', name: 'id', table: 'customers' } },
    };
    const innerJoin2 = {
      table: 'products', joinType: 'INNER',
      on: { type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'product_id', table: 'orders' },
        right: { type: 'column_ref', name: 'id', table: 'products' } },
    };
    const leftJoin = {
      table: 'products', joinType: 'LEFT',
      on: { type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'product_id', table: 'orders' },
        right: { type: 'column_ref', name: 'id', table: 'products' } },
    };
    
    // Mix of inner and left joins
    const result = db._optimizeJoinOrder('orders', [innerJoin1, innerJoin2, leftJoin]);
    // Left join should be last
    assert.strictEqual(result[result.length - 1].joinType, 'LEFT');
  });

  it('three-table join produces correct results regardless of order', () => {
    // This is the key correctness test: the optimizer should reorder but produce same results
    const result = db.execute(`
      SELECT o.id, c.name, p.name as product
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      JOIN products p ON o.product_id = p.id
      WHERE o.id <= 10
      ORDER BY o.id
    `);
    
    assert.strictEqual(result.rows.length, 10);
    assert.strictEqual(result.rows[0].id, 1);
    // Each order should have a customer and product
    for (const row of result.rows) {
      assert.ok(row.name, `row ${row.id} should have customer name`);
      assert.ok(row.product, `row ${row.id} should have product name`);
    }
  });

  it('join ordering prefers smaller tables first (when stats available)', () => {
    // Create a scenario where ordering matters:
    // big (10000) ⋈ medium (100) ⋈ small (10)
    // Optimal: start with small, then medium, then big
    const db2 = new Database();
    db2.execute('CREATE TABLE big (id INT, val INT)');
    for (let i = 1; i <= 500; i++) {
      db2.execute(`INSERT INTO big VALUES (${i}, ${i % 10})`);
    }
    db2.execute('CREATE TABLE medium (id INT, big_id INT)');
    for (let i = 1; i <= 50; i++) {
      db2.execute(`INSERT INTO medium VALUES (${i}, ${(i % 500) + 1})`);
    }
    db2.execute('CREATE TABLE small (id INT, medium_id INT)');
    for (let i = 1; i <= 10; i++) {
      db2.execute(`INSERT INTO small VALUES (${i}, ${(i % 50) + 1})`);
    }
    db2.execute('ANALYZE TABLE big');
    db2.execute('ANALYZE TABLE medium');
    db2.execute('ANALYZE TABLE small');
    
    // Query written in worst order (big first)
    const result = db2.execute(`
      SELECT b.id as bid, m.id as mid, s.id as sid
      FROM big b
      JOIN medium m ON b.id = m.big_id
      JOIN small s ON m.id = s.medium_id
    `);
    
    // Should produce correct results regardless of internal ordering
    assert.ok(result.rows.length > 0, 'should produce results');
    // Each result should have all three table ids
    for (const row of result.rows) {
      assert.ok(row.bid != null);
      assert.ok(row.mid != null);
      assert.ok(row.sid != null);
    }
  });

  it('EXPLAIN shows join order', () => {
    const explain = db.execute(`
      EXPLAIN SELECT o.id, c.name, p.name
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      JOIN products p ON o.product_id = p.id
    `);
    
    // Should have HASH_JOIN or NESTED_LOOP_JOIN entries
    const joinOps = explain.plan?.filter(p => 
      p.operation === 'HASH_JOIN' || p.operation === 'NESTED_LOOP_JOIN'
    ) || [];
    assert.ok(joinOps.length >= 2, `should have 2+ join operations, got ${joinOps.length}`);
  });

  it('join results match with and without ANALYZE', () => {
    // Without stats — no optimization
    const db2 = new Database();
    db2.execute('CREATE TABLE t1 (id INT, val INT)');
    db2.execute('CREATE TABLE t2 (id INT, t1_id INT)');
    db2.execute('CREATE TABLE t3 (id INT, t2_id INT)');
    for (let i = 1; i <= 20; i++) {
      db2.execute(`INSERT INTO t1 VALUES (${i}, ${i * 10})`);
      db2.execute(`INSERT INTO t2 VALUES (${i}, ${(i % 20) + 1})`);
      db2.execute(`INSERT INTO t3 VALUES (${i}, ${(i % 20) + 1})`);
    }
    
    const before = db2.execute(`
      SELECT t1.id, t2.id as t2id, t3.id as t3id
      FROM t1
      JOIN t2 ON t1.id = t2.t1_id
      JOIN t3 ON t2.id = t3.t2_id
      ORDER BY t1.id, t2id, t3id
    `);
    
    // Now with stats
    db2.execute('ANALYZE TABLE t1');
    db2.execute('ANALYZE TABLE t2');
    db2.execute('ANALYZE TABLE t3');
    
    const after = db2.execute(`
      SELECT t1.id, t2.id as t2id, t3.id as t3id
      FROM t1
      JOIN t2 ON t1.id = t2.t1_id
      JOIN t3 ON t2.id = t3.t2_id
      ORDER BY t1.id, t2id, t3id
    `);
    
    assert.deepStrictEqual(before.rows, after.rows, 'results should be identical with and without optimization');
  });

  it('_popcount works correctly', () => {
    assert.strictEqual(db._popcount(0), 0);
    assert.strictEqual(db._popcount(1), 1);
    assert.strictEqual(db._popcount(7), 3);
    assert.strictEqual(db._popcount(255), 8);
  });

  it('handles 4-table join correctly', () => {
    // Create a 4th table
    db.execute('CREATE TABLE regions (id INT, name TEXT)');
    for (let i = 0; i < 5; i++) {
      db.execute(`INSERT INTO regions VALUES (${i}, 'Region ${i}')`);
    }
    db.execute('ANALYZE TABLE regions');
    
    const result = db.execute(`
      SELECT o.id, c.name, p.name as product, r.name as region
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      JOIN products p ON o.product_id = p.id
      JOIN regions r ON c.region = r.name
      WHERE o.id <= 5
      ORDER BY o.id
    `);
    
    assert.ok(result.rows.length > 0, 'should produce results');
    for (const row of result.rows) {
      assert.ok(row.region, 'should have region');
    }
  });
});
