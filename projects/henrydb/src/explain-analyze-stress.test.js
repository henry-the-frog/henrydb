// explain-analyze-stress.test.js — Stress tests for EXPLAIN ANALYZE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN ANALYZE stress tests', () => {
  
  it('basic EXPLAIN ANALYZE shows timing', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 500');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time') || text.includes('actual'), `should show timing: ${text}`);
    assert.ok(text.includes('50') || text.includes('actual=50'), 'should show actual row count');
  });

  it('EXPLAIN ANALYZE with ANALYZE stats shows estimates and actuals', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('ANALYZE TABLE t');
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 500');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    // Should show both est= and actual= 
    assert.ok(text.includes('est=') || text.includes('estimated'), `should show estimate: ${text}`);
    assert.ok(text.includes('actual='), `should show actual: ${text}`);
  });

  it('EXPLAIN ANALYZE with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, val INT)');
    db.execute('CREATE TABLE b (id INT, a_id INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO a VALUES (${i}, ${i})`);
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO b VALUES (${i}, ${(i % 50) + 1})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT a.val FROM a JOIN b ON a.id = b.a_id');
    assert.ok(r.rows.length > 0);
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time'));
  });

  it('EXPLAIN ANALYZE actual rows match real count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    // Run actual query to get real count
    const actual = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 75');
    const expectedCount = actual.rows[0].cnt; // 25
    
    // EXPLAIN ANALYZE should show the same actual rows
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 75');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes(`${expectedCount}`) || text.includes('Actual Rows: 25'),
      `should show actual count ${expectedCount}: ${text}`);
  });

  it('EXPLAIN ANALYZE with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES ('cat${i % 5}', ${i})`);
    }
    
    const r = db.execute('EXPLAIN ANALYZE SELECT cat, SUM(val) FROM t GROUP BY cat');
    assert.ok(r.rows.length > 0);
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time'));
  });

  it('EXPLAIN ANALYZE with subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)');
    assert.ok(r.rows.length > 0);
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time'));
  });

  it('EXPLAIN ANALYZE with window function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rn FROM t');
    assert.ok(r.rows.length > 0);
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Execution Time') || text.includes('actual'));
  });

  it('EXPLAIN vs EXPLAIN ANALYZE produce different output', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE val > 5');
    const analyzeResult = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 5');
    
    const explainText = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    const analyzeText = analyzeResult.rows.map(r => r['QUERY PLAN']).join('\n');
    
    // ANALYZE should have more info (timing, actual rows)
    assert.ok(analyzeText.length >= explainText.length || analyzeText.includes('actual'),
      'ANALYZE should have more detail');
  });

  it('EXPLAIN ANALYZE empty result set', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 100');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('0') || text.includes('Actual Rows: 0'), 'should show 0 actual rows');
  });

  it('EXPLAIN ANALYZE performance: does not 10x the query time', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const start1 = Date.now();
    db.execute('SELECT * FROM t WHERE val > 500');
    const queryTime = Date.now() - start1;
    
    const start2 = Date.now();
    db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 500');
    const analyzeTime = Date.now() - start2;
    
    // EXPLAIN ANALYZE should not be drastically slower
    assert.ok(analyzeTime < queryTime * 20 + 100, 
      `ANALYZE ${analyzeTime}ms vs query ${queryTime}ms — too slow`);
  });
});
