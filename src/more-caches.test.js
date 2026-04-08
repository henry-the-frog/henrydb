// more-caches.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ARCCache, TwoQCache } from './more-caches.js';

describe('ARCCache', () => {
  it('basic put/get', () => {
    const arc = new ARCCache(3);
    arc.put(1, 'a'); arc.put(2, 'b'); arc.put(3, 'c');
    assert.equal(arc.get(1), 'a');
    assert.equal(arc.get(2), 'b');
  });

  it('promotes to frequent on second access', () => {
    const arc = new ARCCache(3);
    arc.put(1, 'a');
    arc.get(1); // Promotes to T2
    arc.put(2, 'b'); arc.put(3, 'c'); arc.put(4, 'd'); // Might evict
    assert.equal(arc.get(1), 'a'); // Should survive (in T2)
  });

  it('eviction under capacity', () => {
    const arc = new ARCCache(3);
    for (let i = 0; i < 10; i++) arc.put(i, `v${i}`);
    assert.ok(arc.size <= 3);
  });

  it('hit rate tracking', () => {
    const arc = new ARCCache(10);
    arc.put(1, 'a');
    arc.get(1); // hit
    arc.get(2); // miss
    assert.equal(arc.stats.hits, 1);
    assert.equal(arc.stats.misses, 1);
  });

  it('benchmark: Zipf workload', () => {
    const arc = new ARCCache(100);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      const key = Math.floor(Math.random() ** 2 * 500);
      if (arc.get(key) === undefined) arc.put(key, `v${key}`);
    }
    console.log(`    ARC 10K: ${Date.now() - t0}ms, hitRate=${(arc.hitRate * 100).toFixed(1)}%`);
    assert.ok(arc.hitRate > 0);
  });
});

describe('TwoQCache', () => {
  it('basic put/get', () => {
    const cache = new TwoQCache(10);
    cache.put(1, 'a'); cache.put(2, 'b');
    assert.equal(cache.get(1), 'a');
    assert.equal(cache.get(2), 'b');
  });

  it('promotes from A1out to Am', () => {
    const cache = new TwoQCache(4); // Kin=1, Km=3
    cache.put(1, 'a'); // → A1in
    cache.put(2, 'b'); // → A1in (1 evicted to A1out)
    cache.put(1, 'x'); // In A1out → promote to Am
    assert.equal(cache.get(1), 'x');
  });

  it('respects capacity', () => {
    const cache = new TwoQCache(5);
    for (let i = 0; i < 20; i++) cache.put(i, `v${i}`);
    assert.ok(cache.size <= 5);
  });

  it('hit rate tracking', () => {
    const cache = new TwoQCache(10);
    cache.put(1, 'a');
    cache.get(1); // hit
    cache.get(2); // miss
    assert.ok(cache.hitRate > 0);
  });

  it('benchmark: Zipf workload', () => {
    const cache = new TwoQCache(100);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      const key = Math.floor(Math.random() ** 2 * 500);
      if (cache.get(key) === undefined) cache.put(key, `v${key}`);
    }
    console.log(`    2Q 10K: ${Date.now() - t0}ms, hitRate=${(cache.hitRate * 100).toFixed(1)}%`);
    assert.ok(cache.hitRate > 0);
  });
});
