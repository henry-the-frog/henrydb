// profiler.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryProfiler } from './profiler.js';
import { Database } from './db.js';

describe('QueryProfiler', () => {
  it('profiles a successful query', () => {
    const profiler = new QueryProfiler();
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    
    const entry = profiler.profile('SELECT * FROM t', () => db.execute('SELECT * FROM t'));
    assert.ok(entry.success);
    assert.equal(entry.rowsReturned, 1);
    assert.ok(entry.totalTimeMs >= 0);
  });

  it('profiles a failed query', () => {
    const profiler = new QueryProfiler();
    const db = new Database();
    
    const entry = profiler.profile('SELECT * FROM nope', () => db.execute('SELECT * FROM nope'));
    assert.ok(!entry.success);
    assert.ok(entry.error);
  });

  it('tracks multiple queries', () => {
    const profiler = new QueryProfiler();
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    
    for (let i = 0; i < 10; i++) {
      profiler.profile(`INSERT INTO t VALUES (${i})`, () => db.execute(`INSERT INTO t VALUES (${i})`));
    }
    
    assert.equal(profiler.queryCount, 10);
  });

  it('provides statistics', () => {
    const profiler = new QueryProfiler();
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    
    for (let i = 0; i < 20; i++) {
      profiler.profile('INSERT', () => db.execute(`INSERT INTO t VALUES (${i})`));
    }
    profiler.profile('SELECT', () => db.execute('SELECT * FROM t'));
    
    const stats = profiler.stats();
    assert.equal(stats.totalQueries, 21);
    assert.equal(stats.successCount, 21);
    assert.ok(stats.avgTimeMs >= 0);
    assert.ok(stats.maxTimeMs >= stats.minTimeMs);
  });

  it('identifies slow queries', () => {
    const profiler = new QueryProfiler();
    profiler.setSlowThreshold(0); // Everything is slow
    
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    profiler.profile('SELECT', () => db.execute('SELECT * FROM t'));
    
    assert.ok(profiler.slowQueries().length >= 1);
  });

  it('reset clears history', () => {
    const profiler = new QueryProfiler();
    profiler.profile('test', () => 42);
    assert.equal(profiler.queryCount, 1);
    profiler.reset();
    assert.equal(profiler.queryCount, 0);
  });

  it('percentile calculation', () => {
    const profiler = new QueryProfiler();
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    
    for (let i = 0; i < 100; i++) {
      profiler.profile('INSERT', () => db.execute(`INSERT INTO t VALUES (${i})`));
    }
    
    const stats = profiler.stats();
    assert.ok(stats.p95TimeMs >= 0); // p95 should be non-negative
    assert.ok(stats.p99TimeMs >= stats.p95TimeMs);
  });
});
