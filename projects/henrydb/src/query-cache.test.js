// query-cache.test.js — Tests for query result cache
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache } from './query-cache.js';

describe('Query Result Cache', () => {
  let cache;

  beforeEach(() => {
    cache = new QueryCache({ maxEntries: 5, defaultTTL: 10000 });
  });

  it('caches and retrieves results', () => {
    cache.set('SELECT * FROM users', ['users'], [{ id: 1 }]);
    const result = cache.get('SELECT * FROM users');
    assert.deepEqual(result, [{ id: 1 }]);
  });

  it('returns null for cache miss', () => {
    assert.equal(cache.get('SELECT * FROM nonexistent'), null);
  });

  it('normalizes SQL (case, whitespace)', () => {
    cache.set('SELECT  *  FROM  users', ['users'], [{ id: 1 }]);
    const result = cache.get('select * from users');
    assert.deepEqual(result, [{ id: 1 }]);
  });

  it('invalidates on table change', () => {
    cache.set('SELECT * FROM users', ['users'], [{ id: 1 }]);
    cache.set('SELECT * FROM posts', ['posts'], [{ id: 2 }]);
    
    cache.invalidate('users');
    assert.equal(cache.get('SELECT * FROM users'), null);
    assert.deepEqual(cache.get('SELECT * FROM posts'), [{ id: 2 }]);
  });

  it('invalidates queries depending on multiple tables', () => {
    cache.set('SELECT * FROM users JOIN posts ON users.id = posts.user_id', ['users', 'posts'], []);
    cache.invalidate('posts');
    assert.equal(cache.get('SELECT * FROM users JOIN posts ON users.id = posts.user_id'), null);
  });

  it('invalidateAll clears everything', () => {
    cache.set('Q1', ['t1'], []);
    cache.set('Q2', ['t2'], []);
    const count = cache.invalidateAll();
    assert.equal(count, 2);
    assert.equal(cache.size, 0);
  });

  it('respects TTL', () => {
    cache = new QueryCache({ defaultTTL: 1 }); // 1ms TTL
    cache.set('SELECT 1', [], [{ v: 1 }]);
    // Wait for expiration
    const start = Date.now();
    while (Date.now() - start < 5) {} // Busy wait 5ms
    assert.equal(cache.get('SELECT 1'), null);
  });

  it('evicts LRU when full', () => {
    for (let i = 0; i < 5; i++) {
      cache.set(`SELECT ${i}`, [`t${i}`], [{ v: i }]);
    }
    assert.equal(cache.size, 5);
    
    // Access first few to make them recently used
    cache.get('SELECT 1');
    cache.get('SELECT 2');
    
    // Add new entry — should evict LRU (SELECT 0 which wasn't accessed)
    cache.set('SELECT 99', ['t99'], [{ v: 99 }]);
    assert.equal(cache.size, 5);
    assert.equal(cache.get('SELECT 0'), null); // Evicted
    assert.deepEqual(cache.get('SELECT 1'), [{ v: 1 }]); // Still there
  });

  it('tracks statistics', () => {
    cache.set('Q1', ['t1'], []);
    cache.get('Q1'); // hit
    cache.get('Q1'); // hit
    cache.get('Q2'); // miss
    
    const stats = cache.stats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 1);
    assert.equal(stats.sets, 1);
    assert.equal(stats.hitRate, 66.7);
  });

  it('prune removes expired entries', () => {
    cache = new QueryCache({ defaultTTL: 1 });
    cache.set('Q1', ['t'], []);
    cache.set('Q2', ['t'], []);
    const start = Date.now();
    while (Date.now() - start < 5) {}
    const pruned = cache.prune();
    assert.equal(pruned, 2);
    assert.equal(cache.size, 0);
  });

  it('custom TTL per query', () => {
    cache.set('fast', ['t'], [], 1); // 1ms TTL
    cache.set('slow', ['t'], [], 100000); // 100s TTL
    const start = Date.now();
    while (Date.now() - start < 5) {}
    assert.equal(cache.get('fast'), null);
    assert.ok(cache.get('slow') !== null);
  });

  it('updates existing entry', () => {
    cache.set('Q', ['t'], [{ v: 1 }]);
    cache.set('Q', ['t'], [{ v: 2 }]);
    assert.deepEqual(cache.get('Q'), [{ v: 2 }]);
    assert.equal(cache.size, 1);
  });
});
