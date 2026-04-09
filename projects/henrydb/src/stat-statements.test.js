// stat-statements.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { StatStatements } from './stat-statements.js';

let ss;

describe('StatStatements', () => {
  beforeEach(() => {
    ss = new StatStatements();
  });

  test('records basic execution', () => {
    ss.record('SELECT * FROM users WHERE id = 1', { executionTimeMs: 5, rows: 1 });
    const entry = ss.get('SELECT * FROM users WHERE id = 1');
    assert.ok(entry);
    assert.equal(entry.calls, 1);
    assert.equal(entry.rows, 1);
  });

  test('normalizes queries', () => {
    ss.record('SELECT * FROM users WHERE id = 1');
    ss.record('SELECT * FROM users WHERE id = 42');
    
    // Both should map to the same normalized query
    const all = ss.getAll();
    assert.equal(all.length, 1);
    assert.equal(all[0].calls, 2);
  });

  test('tracks timing statistics', () => {
    ss.record('SELECT 1', { executionTimeMs: 10 });
    ss.record('SELECT 1', { executionTimeMs: 20 });
    ss.record('SELECT 1', { executionTimeMs: 30 });
    
    const entry = ss.getAll()[0];
    assert.equal(entry.calls, 3);
    assert.equal(entry.totalTimeMs, 60);
    assert.equal(entry.meanTimeMs, 20);
    assert.equal(entry.minTimeMs, 10);
    assert.equal(entry.maxTimeMs, 30);
  });

  test('calculates standard deviation', () => {
    ss.record('SELECT 1', { executionTimeMs: 10 });
    ss.record('SELECT 1', { executionTimeMs: 20 });
    ss.record('SELECT 1', { executionTimeMs: 30 });
    
    const entry = ss.getAll()[0];
    assert.ok(entry.stddevTimeMs > 0);
    assert.ok(entry.stddevTimeMs < 15); // stddev of [10,20,30] = 10
  });

  test('tracks rows', () => {
    ss.record('SELECT * FROM big', { rows: 100 });
    ss.record('SELECT * FROM big', { rows: 200 });
    
    const entry = ss.getAll()[0];
    assert.equal(entry.rows, 300);
    assert.equal(entry.avgRows, 150);
  });

  test('sort by total_time', () => {
    ss.record('fast query', { executionTimeMs: 1 });
    ss.record('slow query', { executionTimeMs: 100 });
    
    const top = ss.getAll({ sortBy: 'total_time' });
    assert.ok(top[0].totalTimeMs >= top[1].totalTimeMs);
  });

  test('sort by calls', () => {
    ss.record('rare', { executionTimeMs: 1 });
    ss.record('frequent', { executionTimeMs: 1 });
    ss.record('frequent', { executionTimeMs: 1 });
    ss.record('frequent', { executionTimeMs: 1 });
    
    const top = ss.getAll({ sortBy: 'calls' });
    assert.ok(top[0].calls >= top[1].calls);
  });

  test('topN returns top queries', () => {
    for (let i = 0; i < 10; i++) {
      const table = `table_${String.fromCharCode(97 + i)}`; // table_a, table_b, etc.
      ss.record(`SELECT * FROM ${table}`, { executionTimeMs: i * 10 });
    }
    
    const top3 = ss.topN(3, 'total_time');
    assert.equal(top3.length, 3);
    assert.ok(top3[0].totalTimeMs >= top3[1].totalTimeMs);
  });

  test('reset clears all stats', () => {
    ss.record('q1');
    ss.record('q2');
    
    const count = ss.reset();
    assert.equal(count, 2);
    assert.equal(ss.getAll().length, 0);
  });

  test('resetQuery clears specific query', () => {
    ss.record('keep this');
    ss.record('delete this');
    
    ss.resetQuery('delete this');
    assert.equal(ss.getAll().length, 1);
  });

  test('getSummary aggregates', () => {
    ss.record('q1', { executionTimeMs: 10, rows: 5 });
    ss.record('q2', { executionTimeMs: 20, rows: 10 });
    ss.record('q1', { executionTimeMs: 15, rows: 5 });
    
    const summary = ss.getSummary();
    assert.equal(summary.uniqueQueries, 2);
    assert.equal(summary.totalCalls, 3);
    assert.equal(summary.totalTimeMs, 45);
    assert.equal(summary.totalRows, 20);
  });

  test('capacity eviction', () => {
    const small = new StatStatements({ maxStatements: 3 });
    small.record('q1');
    small.record('q2');
    small.record('q3');
    
    // q1 has 1 call, add q4 should evict least used
    small.record('q2'); // q2 now has 2 calls
    small.record('q3'); // q3 now has 2 calls
    small.record('q4'); // Should evict q1 (1 call)
    
    assert.equal(small.getAll().length, 3);
  });

  test('block stats tracked', () => {
    ss.record('q1', { blksHit: 10, blksRead: 5 });
    ss.record('q1', { blksHit: 20, blksRead: 3 });
    
    const entry = ss.getAll()[0];
    assert.equal(entry.sharedBlksHit, 30);
    assert.equal(entry.sharedBlksRead, 8);
  });
});
