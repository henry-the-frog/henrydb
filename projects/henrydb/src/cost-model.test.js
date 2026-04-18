// cost-model.test.js — Tests for parametric cost model
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function q(db, sql) { return db.execute(sql).rows || []; }
function plan(db, sql) { return db.execute('EXPLAIN ' + sql).rows.map(r => r['QUERY PLAN']); }

describe('Parametric Cost Model', () => {
  test('_compareScanCosts returns correct structure', () => {
    const db = new Database();
    const result = db._compareScanCosts(1000, 10);
    assert.ok('useIndex' in result);
    assert.ok('seqCost' in result);
    assert.ok('indexCost' in result);
    assert.ok('selectivity' in result);
    assert.equal(typeof result.useIndex, 'boolean');
    assert.equal(typeof result.seqCost, 'number');
  });

  test('index scan preferred for low selectivity (< 10%)', () => {
    const db = new Database();
    const result = db._compareScanCosts(10000, 100); // 1% selectivity
    assert.equal(result.useIndex, true, `Expected index scan for 1% selectivity, costs: idx=${result.indexCost} seq=${result.seqCost}`);
  });

  test('seq scan preferred for very high selectivity (> 80%)', () => {
    const db = new Database();
    const result = db._compareScanCosts(10000, 9000); // 90% selectivity
    assert.equal(result.useIndex, false, `Expected seq scan for 90% selectivity, costs: idx=${result.indexCost} seq=${result.seqCost}`);
  });

  test('crossover point exists between low and high selectivity', () => {
    const db = new Database();
    
    // At 5% — index should win
    const r5 = db._compareScanCosts(10000, 500);
    assert.equal(r5.useIndex, true, '5% should use index');
    
    // At 95% — seq should win
    const r95 = db._compareScanCosts(10000, 9500);
    assert.equal(r95.useIndex, false, '95% should use seq scan');
  });

  test('EXPLAIN shows index scan for single row lookup', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'val_${i}')`);
    }
    db.execute('ANALYZE t');
    
    const p = plan(db, 'SELECT * FROM t WHERE id = 42');
    const firstLine = p[0];
    assert.ok(firstLine.includes('Index Scan') || firstLine.includes('BTree PK Lookup'), 
      `Expected index scan for PK lookup, got: ${firstLine}`);
  });

  test('EXPLAIN shows cost numbers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i % 10})`);
    }
    db.execute('ANALYZE t');
    
    const p = plan(db, 'SELECT * FROM t WHERE val = 3');
    const firstLine = p[0];
    // Should contain cost= format
    assert.ok(firstLine.includes('cost='), `Expected cost in EXPLAIN, got: ${firstLine}`);
  });

  test('actual execution uses cost-based choice with large dataset', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT PRIMARY KEY, flag INT)');
    db.execute('CREATE INDEX idx_flag ON big (flag)');
    
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${i < 250 ? 0 : 1})`);
    }
    db.execute('ANALYZE big');
    
    // flag = 0 matches 250/500 = 50% — should use seq scan
    // But the query should still return correct results regardless of scan method
    const r1 = q(db, 'SELECT COUNT(*) as cnt FROM big WHERE flag = 0');
    assert.equal(r1[0].cnt, 250);
    
    // flag = 1 matches 250/500 = 50% — same
    const r2 = q(db, 'SELECT COUNT(*) as cnt FROM big WHERE flag = 1');
    assert.equal(r2[0].cnt, 250);
    
    // id = 42 matches 1/500 = 0.2% — should use index, result correct
    const r3 = q(db, 'SELECT * FROM big WHERE id = 42');
    assert.equal(r3.length, 1);
    assert.equal(r3[0].id, 42);
  });

  test('COST_MODEL parameters are accessible', () => {
    assert.equal(Database.COST_MODEL.seq_page_cost, 1.0);
    assert.equal(Database.COST_MODEL.random_page_cost, 1.1);
    assert.equal(Database.COST_MODEL.cpu_tuple_cost, 0.01);
    assert.equal(Database.COST_MODEL.cpu_index_tuple_cost, 0.005);
    assert.equal(Database.COST_MODEL.cpu_operator_cost, 0.0025);
  });

  test('very small tables: seq scan may be cheaper', () => {
    const db = new Database();
    const r = db._compareScanCosts(5, 1);
    // For a 5-row table, seq scan is marginally cheaper (no tree traversal)
    assert.equal(typeof r.useIndex, 'boolean');
    // Both costs should be very close
    assert.ok(Math.abs(r.seqCost - r.indexCost) < 2, 'Costs should be similar for tiny table');
  });

  test('selectivity calculation is correct', () => {
    const db = new Database();
    
    const r1 = db._compareScanCosts(1000, 100);
    assert.equal(r1.selectivity, 0.1, 'Should report 10% selectivity');
    
    const r2 = db._compareScanCosts(1000, 500);
    assert.equal(r2.selectivity, 0.5, 'Should report 50% selectivity');
    
    const r3 = db._compareScanCosts(1000, 1);
    assert.equal(r3.selectivity, 0.001, 'Should report 0.1% selectivity');
  });
});
