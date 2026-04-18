// join-cost.test.js — Tests for cost-based join method selection
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Join Cost Model', () => {
  test('_compareJoinCosts returns expected structure', () => {
    const db = new Database();
    const result = db._compareJoinCosts(100, 500, true, true);
    assert.ok(['hash', 'index_nl', 'nested_loop'].includes(result.method));
    assert.ok(typeof result.bestCost === 'number');
    assert.ok('costs' in result);
    assert.ok('hash' in result.costs);
    assert.ok('index_nl' in result.costs);
    assert.ok('nested_loop' in result.costs);
  });

  test('nested loop is cheapest without equi-join or index', () => {
    const db = new Database();
    const result = db._compareJoinCosts(10, 10, false, false);
    // Only nested_loop available
    assert.equal(result.method, 'nested_loop');
    assert.ok(!('hash' in result.costs));
    assert.ok(!('index_nl' in result.costs));
  });

  test('hash join available for equi-join without index', () => {
    const db = new Database();
    const result = db._compareJoinCosts(100, 100, true, false);
    assert.ok('hash' in result.costs);
    assert.ok(!('index_nl' in result.costs));
    // Hash should be cheaper than nested loop
    assert.ok(result.costs.hash < result.costs.nested_loop);
  });

  test('index NL available when index exists', () => {
    const db = new Database();
    const result = db._compareJoinCosts(100, 1000, true, true);
    assert.ok('index_nl' in result.costs);
    assert.ok('hash' in result.costs);
  });

  test('large joins: hash wins over nested loop', () => {
    const db = new Database();
    const result = db._compareJoinCosts(1000, 1000, true, false);
    assert.equal(result.method, 'hash', 'Hash should win for large equi-join');
    assert.ok(result.costs.hash < result.costs.nested_loop);
  });

  test('actual join produces correct results with cost-based selection', () => {
    const db = new Database();
    
    db.execute('CREATE TABLE depts (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE emps (id INT PRIMARY KEY, dept_id INT, name TEXT, salary INT)');
    db.execute('CREATE INDEX idx_dept ON emps (dept_id)');
    
    db.execute("INSERT INTO depts VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')");
    db.execute("INSERT INTO emps VALUES (1, 1, 'Alice', 100), (2, 1, 'Bob', 90), (3, 2, 'Carol', 80), (4, 3, 'Dave', 70)");
    
    const r = db.execute('SELECT d.name as dept, e.name as emp FROM depts d JOIN emps e ON d.id = e.dept_id ORDER BY d.name, e.name');
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].dept, 'Engineering');
    assert.equal(r.rows[0].emp, 'Alice');
  });

  test('LEFT JOIN works with cost-based method selection', () => {
    const db = new Database();
    
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, info TEXT)');
    
    db.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    db.execute("INSERT INTO b VALUES (1, 1, 'info1'), (2, 1, 'info2')");
    
    const r = db.execute('SELECT a.val, b.info FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id');
    assert.equal(r.rows.length, 4);  // 2 for id=1, 1 NULL for id=2, 1 NULL for id=3
    assert.equal(r.rows[2].info, null); // id=2 has no match
    assert.equal(r.rows[3].info, null); // id=3 has no match
  });

  test('multi-table join with cost-based selection', () => {
    const db = new Database();
    
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)');
    db.execute('CREATE TABLE t3 (id INT PRIMARY KEY, t2_id INT)');
    
    db.execute('INSERT INTO t1 VALUES (1, 10), (2, 20)');
    db.execute('INSERT INTO t2 VALUES (1, 1), (2, 1), (3, 2)');
    db.execute('INSERT INTO t3 VALUES (1, 1), (2, 2), (3, 3)');
    
    const r = db.execute('SELECT t1.val, t3.id as t3_id FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id ORDER BY t3_id');
    assert.equal(r.rows.length, 3);
  });

  test('EXPLAIN shows join cost information', () => {
    const db = new Database();
    
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO b VALUES (1, 1)");
    
    const e = db.execute('EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id');
    const lines = e.rows.map(r => r['QUERY PLAN']);
    
    // Should have some join-related plan output
    assert.ok(lines.some(l => l.includes('Join') || l.includes('join') || l.includes('JOIN')),
      `Expected join in plan: ${lines.join(', ')}`);
  });

  test('cost model: nested loop becomes impractical for large cross products', () => {
    const db = new Database();
    const result = db._compareJoinCosts(10000, 10000, true, true);
    
    // Nested loop would be 10000 * 10000 * 0.0025 = 250000
    // Hash would be much cheaper
    assert.ok(result.costs.nested_loop > result.costs.hash * 10,
      'Nested loop should be >10x more expensive than hash for large tables');
  });
});
