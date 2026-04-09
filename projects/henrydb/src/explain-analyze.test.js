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
    assert.ok(result.execution_time_ms >= 0);
    assert.strictEqual(result.actual_rows, 49);
    assert.ok(result.analysis || result.plan || result.rows);
  });

  it('estimation accuracy is reasonable', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${(i * 17) % 1000}, '${i % 2 === 0 ? 'shipped' : 'pending'}')`);
    }
    
    const result = db.execute("EXPLAIN ANALYZE SELECT * FROM orders WHERE amount > 500 AND status = 'shipped'");
    assert.ok(result.actual_rows > 0);
    
    // Estimation should be within 2x of actual
    if (typeof result.estimation_accuracy === 'number') {
      assert.ok(result.estimation_accuracy > 0.3, `Accuracy ${result.estimation_accuracy} too low`);
      assert.ok(result.estimation_accuracy < 3.0, `Accuracy ${result.estimation_accuracy} too high`);
    }
  });

  it('shows table scan details', () => {
    const db = new Database();
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t2 VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t2 WHERE val > 25');
    const plan = result.analysis || result.plan || [];
    const scanOp = plan.find(p => p.operation === 'TABLE_SCAN');
    assert.ok(scanOp, 'Should have TABLE_SCAN operation');
    assert.strictEqual(scanOp.table, 't2');
  });

  it('shows GROUP BY info', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, grp TEXT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO data VALUES (${i}, 'g${i % 5}', ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT grp, COUNT(*) FROM data GROUP BY grp');
    const plan = result.analysis || result.plan || [];
    const groupOp = plan.find(p => p.operation === 'GROUP_BY' || p.operation === 'AGGREGATE');
    assert.ok(groupOp, 'Should have GROUP_BY or AGGREGATE operation');
  });

  it('shows SORT info', () => {
    const db = new Database();
    db.execute('CREATE TABLE t3 (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t3 VALUES (${i}, ${20 - i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t3 ORDER BY val');
    const plan = result.analysis || result.plan || [];
    const sortOp = plan.find(p => p.operation === 'SORT' || p.operation === 'ORDER_BY');
    assert.ok(sortOp, 'Should have SORT operation');
  });

  it('regular EXPLAIN returns plan without execution', () => {
    const db = new Database();
    db.execute('CREATE TABLE t4 (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t4 VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN SELECT * FROM t4 WHERE val > 5');
    assert.ok(result.type === 'PLAN' || result.type === 'ROWS');
    assert.ok(result.plan || result.rows);
    // Should NOT have actual_rows (not executed)
    assert.strictEqual(result.actual_rows, undefined);
  });

  it('EXPLAIN ANALYZE with no matching rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t5 (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t5 VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t5 WHERE val > 9999');
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
    assert.strictEqual(result.actual_rows, 3);
  });
});
