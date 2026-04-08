// clock-sweep.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ClockSweepCache } from './clock-sweep.js';

describe('ClockSweepCache', () => {
  it('basic get/insert', () => {
    const cache = new ClockSweepCache(4);
    const data = cache.get(1, () => 'page-1');
    assert.equal(data, 'page-1');
    assert.equal(cache.size, 1);
  });

  it('cache hit', () => {
    const cache = new ClockSweepCache(4);
    cache.get(1, () => 'page-1');
    const data = cache.get(1, () => 'should-not-load');
    assert.equal(data, 'page-1');
    assert.equal(cache.stats.hits, 1);
    assert.equal(cache.stats.misses, 1);
  });

  it('eviction when full', () => {
    const cache = new ClockSweepCache(3);
    cache.get(1, () => 'a');
    cache.get(2, () => 'b');
    cache.get(3, () => 'c');
    cache.get(4, () => 'd'); // Should evict one
    assert.equal(cache.size, 3);
    assert.equal(cache.stats.evictions, 1);
  });

  it('clock sweep favors used pages', () => {
    const cache = new ClockSweepCache(3);
    cache.get(1, () => 'a');
    cache.get(2, () => 'b');
    cache.get(3, () => 'c');
    // Access page 1 multiple times to increase usage
    cache.get(1);
    cache.get(1);
    cache.get(4, () => 'd'); // Should evict 2 or 3, not 1
    assert.equal(cache.get(1), 'a'); // Page 1 should survive
  });

  it('pin prevents eviction', () => {
    const cache = new ClockSweepCache(2);
    cache.get(1, () => 'a');
    cache.get(2, () => 'b');
    cache.pin(1);
    cache.pin(2);
    assert.throws(() => cache.get(3, () => 'c')); // All pinned
  });

  it('unpin allows eviction', () => {
    const cache = new ClockSweepCache(2);
    cache.get(1, () => 'a');
    cache.get(2, () => 'b');
    cache.pin(1);
    cache.unpin(1);
    cache.get(3, () => 'c');
    assert.equal(cache.size, 2);
  });

  it('dirty tracking', () => {
    const cache = new ClockSweepCache(4);
    cache.get(1, () => 'a');
    cache.get(2, () => 'b');
    cache.markDirty(1);
    assert.deepEqual(cache.getDirtyPages(), [1]);
  });

  it('flush writes dirty pages', () => {
    const cache = new ClockSweepCache(4);
    cache.get(1, () => 'a');
    cache.markDirty(1);
    const written = [];
    cache.flush((id, data) => written.push({ id, data }));
    assert.equal(written.length, 1);
    assert.deepEqual(cache.getDirtyPages(), []);
  });

  it('hit rate calculation', () => {
    const cache = new ClockSweepCache(10);
    cache.get(1, () => 'a');
    cache.get(1); // hit
    cache.get(1); // hit
    cache.get(2, () => 'b'); // miss
    assert.equal(cache.hitRate, 0.5); // 2 hits, 2 misses
  });

  it('benchmark: 10K accesses with Zipf-like access', () => {
    const cache = new ClockSweepCache(100);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      const pageId = Math.floor(Math.random() ** 2 * 500); // Zipf-like
      cache.get(pageId, (id) => `page-${id}`);
    }
    console.log(`    Clock sweep 10K: ${Date.now() - t0}ms, hit rate=${(cache.hitRate * 100).toFixed(1)}%`);
    assert.ok(cache.hitRate > 0.1);
  });
});
