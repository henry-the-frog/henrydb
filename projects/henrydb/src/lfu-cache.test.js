// lfu-cache.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LFUCache } from './lfu-cache.js';

describe('LFUCache', () => {
  it('basic get and put', () => {
    const c = new LFUCache(3);
    c.put(1, 'a'); c.put(2, 'b'); c.put(3, 'c');
    assert.equal(c.get(1), 'a');
    assert.equal(c.get(2), 'b');
  });

  it('evicts least frequently used', () => {
    const c = new LFUCache(2);
    c.put(1, 'a');
    c.put(2, 'b');
    c.get(1); // freq(1) = 2, freq(2) = 1
    c.put(3, 'c'); // Evicts key 2 (least frequent)
    
    assert.equal(c.get(1), 'a');
    assert.equal(c.get(2), undefined); // Evicted
    assert.equal(c.get(3), 'c');
  });

  it('tie-breaking: evicts LRU among same frequency', () => {
    const c = new LFUCache(2);
    c.put(1, 'a');
    c.put(2, 'b');
    // Both have freq=1, key 1 is older
    c.put(3, 'c'); // Should evict key 1 (least recently used among freq=1)
    assert.equal(c.get(1), undefined);
    assert.equal(c.get(2), 'b');
  });

  it('update existing key', () => {
    const c = new LFUCache(2);
    c.put(1, 'old');
    c.put(1, 'new');
    assert.equal(c.get(1), 'new');
    assert.equal(c.size, 1);
  });

  it('hit rate tracking', () => {
    const c = new LFUCache(100);
    for (let i = 0; i < 100; i++) c.put(i, i);
    
    // All hits
    for (let i = 0; i < 100; i++) c.get(i);
    // All misses
    for (let i = 100; i < 200; i++) c.get(i);
    
    const stats = c.getStats();
    assert.ok(Math.abs(stats.hitRate - 0.5) < 0.01);
  });

  it('stress: 10K operations', () => {
    const c = new LFUCache(100);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) {
      c.put(i % 200, i);
    }
    for (let i = 0; i < 10000; i++) {
      c.get(i % 200);
    }
    const elapsed = performance.now() - t0;
    
    console.log(`  20K ops (10K put + 10K get): ${elapsed.toFixed(1)}ms`);
    console.log(`  Hit rate: ${(c.getStats().hitRate * 100).toFixed(1)}%`);
    assert.ok(elapsed < 200);
  });
});
