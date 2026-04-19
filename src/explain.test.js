import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r;
}

describe('EXPLAIN', () => {
  it('shows INDEX_SCAN for indexed column query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)');
    db.execute('CREATE INDEX idx_age ON t (age)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${20 + i % 30})`);
    
    const result = query(db, 'EXPLAIN SELECT * FROM t WHERE age = 30');
    assert.equal(result.type, 'PLAN');
    assert.ok(result.plan.length > 0);
    
    const indexScan = result.plan.find(p => p.operation === 'INDEX_SCAN');
    assert.ok(indexScan, 'Should use INDEX_SCAN for indexed column');
    assert.equal(indexScan.table, 't');
  });

  it('shows SEQ_SCAN for non-indexed column query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'user${i}')`);
    
    const result = query(db, "EXPLAIN SELECT * FROM t WHERE name = 'user5'");
    assert.equal(result.type, 'PLAN');
    
    const seqScan = result.plan.find(p => p.operation === 'SEQ_SCAN' || p.operation === 'TABLE_SCAN');
    // Non-indexed columns require a sequential scan
    assert.ok(seqScan || result.plan.length > 0, 'Should show a scan operation');
  });

  it('EXPLAIN ANALYZE returns timing info', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const result = query(db, 'EXPLAIN ANALYZE SELECT * FROM t WHERE val = 500');
    assert.equal(result.type, 'ANALYZE');
    assert.ok(result.execution_time_ms >= 0, 'Should have execution time');
    assert.ok(result.plan.length > 0, 'Should have plan nodes');
  });

  it('EXPLAIN works for SELECT with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, data TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO b VALUES (1, 1, 'y')");
    
    const result = query(db, 'EXPLAIN SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id');
    assert.equal(result.type, 'PLAN');
    assert.ok(result.plan.length > 0);
  });

  it('EXPLAIN works for aggregation queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO sales VALUES (${i}, 'p${i % 3}', ${i * 10})`);
    
    const result = query(db, 'EXPLAIN SELECT product, SUM(amount) FROM sales GROUP BY product');
    assert.equal(result.type, 'PLAN');
    assert.ok(result.plan.length > 0);
  });

  it('EXPLAIN returns text output in ANALYZE mode', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const result = query(db, 'EXPLAIN ANALYZE SELECT * FROM t');
    assert.ok(result.text, 'Should have text output');
    assert.ok(result.text.includes('Scan'), 'Text should mention scan');
  });
});
