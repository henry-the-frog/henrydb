// explain-analyze.test.js — EXPLAIN ANALYZE output validation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN ANALYZE', () => {
  it('basic: shows estimated vs actual rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('ANALYZE t');
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 50');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    
    // Should show estimated and actual
    assert.ok(plan.includes('est='), 'should show estimated rows');
    assert.ok(plan.includes('actual=50'), 'should show actual rows');
    assert.ok(plan.includes('Execution Time'), 'should show execution time');
    assert.ok(plan.includes('Rows Returned: 50'), 'should show rows returned');
  });

  it('EXPLAIN (not ANALYZE) shows plan without executing', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    
    // Should NOT show actual rows or execution time
    assert.ok(!plan.includes('Execution Time'), 'EXPLAIN should not execute');
  });

  it('index scan detected in EXPLAIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, category TEXT, val INT)');
    db.execute('CREATE INDEX idx_cat ON t(category)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 5}', ${i * 10})`);
    }
    
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 25');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    assert.ok(plan.toLowerCase().includes('index'), 'should detect index scan for PK lookup');
  });

  it('EXPLAIN with JOIN shows join info', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)');
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob')");
    db.execute('INSERT INTO b VALUES (1, 1, 100), (2, 2, 200)');
    
    const r = db.execute('EXPLAIN ANALYZE SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('JOIN'), 'should show JOIN in plan');
    assert.ok(plan.includes('Rows Returned: 2'), 'should return 2 rows');
  });

  it('EXPLAIN ANALYZE with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('north', 100),('north', 200),('south', 150),('south', 250),('east', 300)");
    
    const r = db.execute('EXPLAIN ANALYZE SELECT region, SUM(amount) as total FROM sales GROUP BY region');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Rows Returned: 3'), 'should return 3 groups');
  });

  it('estimation improves after ANALYZE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 10})`);
    
    // Before ANALYZE — uses defaults
    const r1 = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 500');
    const plan1 = r1.rows.map(row => row['QUERY PLAN']).join('\n');
    
    // After ANALYZE — should be more accurate
    db.execute('ANALYZE t');
    const r2 = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 500');
    const plan2 = r2.rows.map(row => row['QUERY PLAN']).join('\n');
    
    // Extract estimated rows from both
    const estMatch1 = plan1.match(/est=(\d+)/);
    const estMatch2 = plan2.match(/est=(\d+)/);
    assert.ok(estMatch1, 'should have estimate before ANALYZE');
    assert.ok(estMatch2, 'should have estimate after ANALYZE');
    
    // After ANALYZE, estimate should be closer to 500
    const est2 = parseInt(estMatch2[1]);
    assert.ok(Math.abs(est2 - 500) <= 50, `After ANALYZE, estimate should be ~500, got ${est2}`);
  });

  it('EXPLAIN ANALYZE shows buffers/IO stats', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t');
    const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Buffers') || plan.includes('buffer'), 'should show buffer/IO stats');
  });
});
