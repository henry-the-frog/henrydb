// plan-cache.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { PlanCache, normalizeSQL } from './plan-cache.js';

let cache;

describe('PlanCache', () => {
  beforeEach(() => {
    cache = new PlanCache({ maxEntries: 5 });
  });

  test('cache miss returns null', () => {
    assert.equal(cache.get('SELECT * FROM users'), null);
    assert.equal(cache.getStats().misses, 1);
  });

  test('cache hit returns plan', () => {
    const plan = { type: 'SeqScan', table: 'users' };
    cache.put('SELECT * FROM users', plan, ['users']);
    
    const result = cache.get('SELECT * FROM users');
    assert.deepEqual(result, plan);
    assert.equal(cache.getStats().hits, 1);
  });

  test('SQL normalization (whitespace, case)', () => {
    cache.put('SELECT  *  FROM  users', { type: 'scan' }, ['users']);
    assert.ok(cache.get('select * from users'));
    assert.ok(cache.get('SELECT * FROM users'));
  });

  test('DDL invalidation', () => {
    cache.put('SELECT * FROM users', { plan: 1 }, ['users']);
    cache.put('SELECT * FROM orders', { plan: 2 }, ['orders']);
    cache.put('SELECT * FROM users JOIN orders', { plan: 3 }, ['users', 'orders']);
    
    const invalidated = cache.invalidateTable('users');
    assert.equal(invalidated, 2); // users query + join query
    
    assert.equal(cache.get('SELECT * FROM users'), null);
    assert.ok(cache.get('SELECT * FROM orders')); // Still cached
    assert.equal(cache.get('SELECT * FROM users JOIN orders'), null);
  });

  test('invalidateAll clears everything', () => {
    cache.put('q1', { p: 1 }, ['t1']);
    cache.put('q2', { p: 2 }, ['t2']);
    
    const count = cache.invalidateAll();
    assert.equal(count, 2);
    assert.equal(cache.getStats().entries, 0);
  });

  test('LRU eviction at capacity', async () => {
    cache.put('q1', { p: 1 }, ['t1']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q2', { p: 2 }, ['t2']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q3', { p: 3 }, ['t3']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q4', { p: 4 }, ['t4']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q5', { p: 5 }, ['t5']);
    
    // Access q1 to make it recently used
    cache.get('q1');
    
    // Adding q6 should evict q2 (LRU)
    cache.put('q6', { p: 6 }, ['t6']);
    
    assert.ok(cache.get('q1')); // Recently accessed
    assert.equal(cache.get('q2'), null); // Evicted
    assert.ok(cache.get('q6'));
  });

  test('hit count tracking', () => {
    cache.put('q1', { p: 1 }, ['t1']);
    cache.get('q1');
    cache.get('q1');
    cache.get('q1');
    
    const entries = cache.getEntries();
    assert.equal(entries[0].hitCount, 3);
  });

  test('hit rate calculation', () => {
    cache.put('q1', { p: 1 }, ['t1']);
    cache.get('q1'); // hit
    cache.get('q1'); // hit
    cache.get('q2'); // miss
    
    const stats = cache.getStats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 1);
    assert.ok(stats.hitRate > 60);
    assert.ok(stats.hitRate < 70);
  });

  test('getEntries sorted by hits', () => {
    cache.put('popular', { p: 1 }, ['t1']);
    cache.put('rare', { p: 2 }, ['t2']);
    
    cache.get('popular');
    cache.get('popular');
    cache.get('popular');
    cache.get('rare');
    
    const entries = cache.getEntries({ sortBy: 'hits' });
    assert.equal(entries[0].hitCount, 3);
  });

  test('getEntries with limit', () => {
    for (let i = 0; i < 5; i++) {
      cache.put(`q${i}`, { p: i }, [`t${i}`]);
    }
    
    const entries = cache.getEntries({ limit: 3 });
    assert.equal(entries.length, 3);
  });

  test('replacing entry updates plan', () => {
    cache.put('q1', { plan: 'v1' }, ['t1']);
    cache.put('q1', { plan: 'v2' }, ['t1']);
    
    assert.deepEqual(cache.get('q1'), { plan: 'v2' });
    assert.equal(cache.getStats().entries, 1);
  });
});

describe('normalizeSQL', () => {
  test('replaces number literals', () => {
    const result = normalizeSQL('SELECT * FROM users WHERE id = 42');
    assert.ok(result.includes('$'));
    assert.ok(!result.includes('42'));
  });

  test('replaces string literals', () => {
    const result = normalizeSQL("SELECT * FROM users WHERE name = 'Alice'");
    assert.ok(result.includes('$'));
    assert.ok(!result.includes('alice'));
  });

  test('normalizes whitespace and case', () => {
    const a = normalizeSQL('SELECT  *  FROM  users  WHERE  id = 1');
    const b = normalizeSQL('select * from users where id = 1');
    assert.equal(a, b);
  });
});
