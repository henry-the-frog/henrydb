// query-stats.test.js — Tests for query statistics collection
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryStatsCollector } from './query-stats.js';
import { Database } from './db.js';

describe('QueryStatsCollector', () => {
  it('records query execution statistics', () => {
    const collector = new QueryStatsCollector();
    collector.record('SELECT * FROM users', 5.5, 100);
    collector.record('SELECT * FROM users', 3.2, 100);
    
    const stats = collector.getAll();
    assert.equal(stats.length, 1); // Normalized to same key
    assert.equal(stats[0].calls, 2);
    assert.ok(stats[0].total_time_ms > 8);
    assert.ok(stats[0].mean_time_ms > 4);
  });

  it('normalizes similar queries', () => {
    const collector = new QueryStatsCollector();
    collector.record('SELECT * FROM t WHERE id = 1', 1, 1);
    collector.record('SELECT * FROM t WHERE id = 2', 2, 1);
    collector.record('SELECT * FROM t WHERE id = 3', 3, 1);
    
    const stats = collector.getAll();
    assert.equal(stats.length, 1); // All normalized to same pattern
    assert.equal(stats[0].calls, 3);
  });

  it('tracks min/max time and rows', () => {
    const collector = new QueryStatsCollector();
    collector.record('SELECT 1', 1.0, 10);
    collector.record('SELECT 1', 5.0, 50);
    collector.record('SELECT 1', 2.0, 20);
    
    const stats = collector.getAll();
    assert.equal(stats[0].min_time_ms, 1.0);
    assert.equal(stats[0].max_time_ms, 5.0);
    assert.equal(stats[0].total_rows, 80);
  });

  it('sorts by total time by default', () => {
    const collector = new QueryStatsCollector();
    collector.record('fast query', 1, 1);
    collector.record('slow query', 100, 1);
    
    const stats = collector.getAll();
    assert.ok(stats[0].total_time_ms >= stats[1].total_time_ms);
  });

  it('getSlowest returns mean-time ordered', () => {
    const collector = new QueryStatsCollector();
    collector.record('SELECT * FROM a', 1, 1);
    collector.record('SELECT * FROM b', 10, 1);
    
    const slow = collector.getSlowest(2);
    assert.ok(slow[0].mean_time_ms >= slow[1].mean_time_ms);
  });

  it('provides summary statistics', () => {
    const collector = new QueryStatsCollector();
    collector.record('q1', 5, 10);
    collector.record('q2', 3, 20);
    
    const summary = collector.summary();
    assert.equal(summary.uniqueQueries, 2);
    assert.equal(summary.totalCalls, 2);
    assert.ok(summary.totalTimeMs > 0);
  });

  it('reset clears all statistics', () => {
    const collector = new QueryStatsCollector();
    collector.record('q1', 5, 10);
    collector.reset();
    assert.equal(collector.getAll().length, 0);
  });

  it('can be disabled and re-enabled', () => {
    const collector = new QueryStatsCollector();
    collector.disable();
    collector.record('q1', 5, 10);
    assert.equal(collector.getAll().length, 0);
    
    collector.enable();
    collector.record('q2', 3, 5);
    assert.equal(collector.getAll().length, 1);
  });

  it('records errors', () => {
    const collector = new QueryStatsCollector();
    collector.recordError('bad query');
    const stats = collector.getAll();
    assert.equal(stats[0].errors, 1);
    assert.equal(stats[0].calls, 1);
  });
});

describe('SHOW QUERY STATS integration', () => {
  it('SHOW QUERY STATS returns collected stats', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    // Run some queries
    db.execute('SELECT * FROM t');
    db.execute('SELECT * FROM t WHERE id = 5');
    db.execute('SELECT * FROM t WHERE id = 10');
    
    const result = db.execute('SHOW QUERY STATS');
    assert.ok(result.rows.length > 0);
    
    // Should have timing info
    for (const row of result.rows) {
      assert.ok(row.calls > 0);
      assert.ok(row.total_time_ms >= 0);
    }
  });

  it('SHOW SLOW QUERIES returns slowest queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    db.execute('SELECT * FROM t');
    
    const result = db.execute('SHOW SLOW QUERIES');
    assert.ok(result.rows);
  });

  it('RESET QUERY STATS clears everything', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    db.execute('SELECT * FROM t');
    
    const before = db.execute('SHOW QUERY STATS');
    assert.ok(before.rows.length > 0);
    
    db.execute('RESET QUERY STATS');
    
    const after = db.execute('SHOW QUERY STATS');
    assert.equal(after.rows.length, 0);
  });
});
