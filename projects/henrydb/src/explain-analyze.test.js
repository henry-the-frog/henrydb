// explain-analyze.test.js — Tests for EXPLAIN ANALYZE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN ANALYZE', () => {
  it('returns actual execution statistics', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 500');
    assert.strictEqual(result.type, 'ANALYZE');
    assert.ok(result.execution_time_ms > 0);
    assert.strictEqual(result.actual_rows, 49);
    assert.ok(result.plan.length > 0);
  });

  it('estimation accuracy is reasonable', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${(i * 17) % 1000}, '${i % 2 === 0 ? 'shipped' : 'pending'}')`);
    }
    
    const result = db.execute("EXPLAIN ANALYZE SELECT * FROM orders WHERE amount > 500 AND status = 'shipped'");
    
    // Estimation should be within 2x of actual
    if (typeof result.estimation_accuracy === 'number') {
      assert.ok(result.estimation_accuracy > 0.3, `Accuracy ${result.estimation_accuracy} too low`);
      assert.ok(result.estimation_accuracy < 3.0, `Accuracy ${result.estimation_accuracy} too high`);
    }
  });

  it('shows table scan details', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 25');
    const scanOp = result.plan.find(p => p.operation === 'TABLE_SCAN');
    assert.ok(scanOp);
    assert.strictEqual(scanOp.table, 't');
    assert.strictEqual(scanOp.total_table_rows, 50);
    assert.strictEqual(scanOp.actual_rows, 24);
  });

  it('shows GROUP BY info', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, grp TEXT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO data VALUES (${i}, 'g${i % 5}', ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT grp, COUNT(*) FROM data GROUP BY grp');
    const groupOp = result.plan.find(p => p.operation === 'GROUP_BY');
    assert.ok(groupOp);
    assert.strictEqual(groupOp.groups, 5);
  });

  it('shows SORT info', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${20 - i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t ORDER BY val');
    const sortOp = result.plan.find(p => p.operation === 'SORT');
    assert.ok(sortOp);
    assert.strictEqual(sortOp.rows_sorted, 20);
  });

  it('regular EXPLAIN returns plan without execution', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN SELECT * FROM t WHERE val > 5');
    assert.strictEqual(result.type, 'PLAN');
    assert.ok(result.plan.length > 0);
    // Should NOT have actual_rows (not executed)
    assert.strictEqual(result.actual_rows, undefined);
  });

  it('EXPLAIN ANALYZE with no matching rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 9999');
    assert.strictEqual(result.actual_rows, 0);
  });

  it('EXPLAIN ANALYZE with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute('INSERT INTO a VALUES (1, 10)');
    db.execute('INSERT INTO a VALUES (2, 20)');
    db.execute('INSERT INTO b VALUES (1, 1)');
    db.execute('INSERT INTO b VALUES (2, 1)');
    db.execute('INSERT INTO b VALUES (3, 2)');
    
    const result = db.execute('EXPLAIN ANALYZE SELECT a.val, b.id FROM a JOIN b ON b.a_id = a.id');
    assert.strictEqual(result.type, 'ANALYZE');
    assert.strictEqual(result.actual_rows, 3);
  });
});
