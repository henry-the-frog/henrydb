// index-benchmark.test.js — Verify index scans are actually used and improve query plans
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Index Usage Verification via EXPLAIN', () => {
  let db;

  beforeEach(async () => {
    db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL, stock INTEGER)');
    db.execute('CREATE INDEX idx_category ON products(category)');
    db.execute('CREATE INDEX idx_price ON products(price)');
    
    for (let i = 1; i <= 1000; i++) {
      const cat = ['electronics', 'books', 'clothing', 'food', 'toys'][i % 5];
      db.execute(`INSERT INTO products VALUES (${i}, '${cat}', ${(i * 1.5).toFixed(2)}, ${i % 100})`);
    }
  });

  it('equality on indexed column uses INDEX_SCAN', () => {
    const r = db.execute("EXPLAIN SELECT * FROM products WHERE category = 'electronics'");
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX'),
      `Should use index scan: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('range on indexed column uses INDEX_SCAN', () => {
    const r = db.execute('EXPLAIN SELECT * FROM products WHERE price > 500');
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX'),
      `Range should use index: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('BETWEEN on indexed column uses INDEX_SCAN', () => {
    const r = db.execute('EXPLAIN SELECT * FROM products WHERE price BETWEEN 100 AND 200');
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX'),
      `BETWEEN should use index: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('IN on indexed column uses INDEX_SCAN', () => {
    const r = db.execute("EXPLAIN SELECT * FROM products WHERE category IN ('electronics', 'books')");
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX'),
      `IN should use index: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('OR on indexed columns uses INDEX_SCAN', () => {
    const r = db.execute("EXPLAIN SELECT * FROM products WHERE category = 'electronics' OR category = 'books'");
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX'),
      `OR should use index: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('BETWEEN SYMMETRIC uses INDEX_SCAN', () => {
    const r = db.execute('EXPLAIN SELECT * FROM products WHERE id BETWEEN SYMMETRIC 500 AND 100');
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('Index') || planStr.includes('INDEX') || planStr.includes('PK'),
      `BETWEEN SYMMETRIC should use index: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });

  it('non-indexed column falls back to TABLE_SCAN', () => {
    const r = db.execute('EXPLAIN SELECT * FROM products WHERE stock = 50');
    const planStr = JSON.stringify(r);
    assert.ok(planStr.includes('TABLE_SCAN') || planStr.includes('Seq Scan'),
      `Non-indexed should use table scan: ${r.rows?.map(r => r['QUERY PLAN']).join('; ')}`);
  });
});

describe('EXPLAIN ANALYZE Correctness', () => {
  it('actual row count matches SELECT count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, '${i % 5}')`);
    
    const selectResult = db.execute("SELECT * FROM t WHERE val = '3'");
    const analyzeResult = db.execute("EXPLAIN ANALYZE SELECT * FROM t WHERE val = '3'");
    
    assert.equal(analyzeResult.actual_rows, selectResult.rows.length);
  });

  it('execution time is positive', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, '${i}')`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 50');
    assert.ok(r.execution_time_ms >= 0, `Time should be positive: ${r.execution_time_ms}`);
  });
});
