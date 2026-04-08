// query-cache.test.js — Tests for query result caching
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache } from './query-cache.js';

describe('QueryCache', () => {

  it('set and get', () => {
    const cache = new QueryCache();
    cache.set('SELECT * FROM t', { rows: [{ id: 1 }] }, ['t']);
    
    const result = cache.get('SELECT * FROM t');
    assert.deepEqual(result, { rows: [{ id: 1 }] });
  });

  it('miss returns null', () => {
    const cache = new QueryCache();
    assert.equal(cache.get('SELECT * FROM unknown'), null);
  });

  it('TTL expiration', async () => {
    const cache = new QueryCache({ defaultTTLMs: 50 });
    cache.set('q1', { rows: [] }, ['t']);

    assert.ok(cache.get('q1'));
    await new Promise(r => setTimeout(r, 100));
    assert.equal(cache.get('q1'), null);
  });

  it('table invalidation', () => {
    const cache = new QueryCache();
    cache.set('SELECT * FROM users', { rows: [] }, ['users']);
    cache.set('SELECT * FROM orders', { rows: [] }, ['orders']);
    cache.set('SELECT * FROM users JOIN orders', { rows: [] }, ['users', 'orders']);

    const invalidated = cache.invalidateTable('users');
    assert.equal(invalidated, 2); // users + users JOIN orders
    assert.equal(cache.get('SELECT * FROM users'), null);
    assert.ok(cache.get('SELECT * FROM orders')); // Not invalidated
  });

  it('LRU eviction when full', () => {
    const cache = new QueryCache({ maxEntries: 3 });
    cache.set('q1', { rows: [1] }, ['t']);
    cache.set('q2', { rows: [2] }, ['t']);
    cache.set('q3', { rows: [3] }, ['t']);
    
    // Access q1 to make it recently used
    cache.get('q1');
    
    // Insert q4 → should evict q2 (least recently used)
    cache.set('q4', { rows: [4] }, ['t']);
    
    assert.ok(cache.get('q1')); // Recently accessed
    assert.equal(cache.get('q2'), null); // Evicted
    assert.ok(cache.get('q3'));
    assert.ok(cache.get('q4'));
  });

  it('invalidateAll', () => {
    const cache = new QueryCache();
    cache.set('q1', { rows: [] }, ['t']);
    cache.set('q2', { rows: [] }, ['t']);

    cache.invalidateAll();
    assert.equal(cache.size, 0);
    assert.equal(cache.get('q1'), null);
  });

  it('extractTables: FROM', () => {
    const tables = QueryCache.extractTables('SELECT * FROM users WHERE id = 1');
    assert.deepEqual(tables, ['users']);
  });

  it('extractTables: JOIN', () => {
    const tables = QueryCache.extractTables('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');
    assert.ok(tables.includes('users'));
    assert.ok(tables.includes('orders'));
  });

  it('hit rate tracking', () => {
    const cache = new QueryCache();
    cache.set('q1', { rows: [] }, ['t']);

    cache.get('q1'); // hit
    cache.get('q1'); // hit
    cache.get('q2'); // miss

    const stats = cache.getStats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 1);
    assert.equal(stats.hitRate, '66.7%');
  });

  it('custom TTL per query', () => {
    const cache = new QueryCache({ defaultTTLMs: 60000 });
    cache.set('fast', { rows: [] }, ['t'], 10); // 10ms TTL
    cache.set('slow', { rows: [] }, ['t'], 60000); // 60s TTL

    // Both should be present immediately
    assert.ok(cache.get('fast'));
    assert.ok(cache.get('slow'));
  });

  it('benchmark: cache lookup on 10K queries', () => {
    const cache = new QueryCache({ maxEntries: 5000 });
    
    // Fill cache
    for (let i = 0; i < 5000; i++) {
      cache.set(`SELECT * FROM t WHERE id = ${i}`, { rows: [{ id: i }] }, ['t']);
    }

    // Benchmark lookups
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      cache.get(`SELECT * FROM t WHERE id = ${i % 5000}`);
    }
    const ms = Date.now() - t0;

    console.log(`    10K cache lookups: ${ms}ms (${(10000 / Math.max(ms, 0.1)).toFixed(0)} lookups/ms)`);
    assert.ok(cache.getStats().hits >= 5000);
  });
});
