// plan-cache.test.js — Query plan cache tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PlanCache } from './plan-cache.js';
import { Database } from './db.js';

describe('PlanCache', () => {
  it('caches and retrieves parsed ASTs', () => {
    const cache = new PlanCache(10);
    const ast = { type: 'SELECT', columns: ['*'] };
    cache.put('SELECT * FROM t', ast);
    
    const cached = cache.get('SELECT * FROM t');
    assert.deepEqual(cached, ast);
  });

  it('returns null for cache miss', () => {
    const cache = new PlanCache(10);
    assert.equal(cache.get('SELECT 1'), null);
  });

  it('LRU eviction when cache is full', () => {
    const cache = new PlanCache(3);
    cache.put('q1', { type: 'q1' });
    cache.put('q2', { type: 'q2' });
    cache.put('q3', { type: 'q3' });
    cache.put('q4', { type: 'q4' }); // Should evict q1 (LRU)
    
    assert.equal(cache.get('q1'), null); // Evicted
    assert.ok(cache.get('q2') !== null); // Still present
  });

  it('tracks hit rate', () => {
    const cache = new PlanCache(10);
    cache.put('q1', { type: 'q1' });
    cache.get('q1'); // hit
    cache.get('q1'); // hit
    cache.get('q2'); // miss
    
    const stats = cache.stats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 1);
    assert.ok(stats.hitRate > 0.6);
  });

  it('clear empties the cache', () => {
    const cache = new PlanCache(10);
    cache.put('q1', { type: 'q1' });
    cache.put('q2', { type: 'q2' });
    cache.clear();
    
    assert.equal(cache.get('q1'), null);
    assert.equal(cache.stats().size, 0);
  });
});

describe('Plan Cache Integration', () => {
  it('database caches SELECT queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    // First execution: cache miss (for SELECT)
    db.execute('SELECT val FROM t WHERE id = 1');
    // Second execution: cache hit
    db.execute('SELECT val FROM t WHERE id = 1');
    
    const stats = db.planCacheStats();
    assert.ok(stats.hits >= 1);
    assert.ok(stats.size >= 1);
  });

  it('DDL invalidates cache', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SELECT * FROM t'); // Cached
    db.execute('SELECT * FROM t'); // Hit
    
    assert.equal(db.planCacheStats().hits, 1);
    
    db.execute('ALTER TABLE t ADD COLUMN val INT');
    
    // Cache should be cleared
    assert.equal(db.planCacheStats().size, 0);
  });

  it('DML is not cached', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    
    // INSERTs should not be cached
    assert.equal(db.planCacheStats().size, 0);
  });
});
