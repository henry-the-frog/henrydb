// lru-k.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LRUK } from './lru-k.js';

describe('LRU-K', () => {
  it('basic access and get', () => {
    const cache = new LRUK(3, 2);
    cache.access('p1', 'data1');
    assert.equal(cache.get('p1'), 'data1');
  });

  it('evicts page with oldest K-th access', () => {
    const cache = new LRUK(2, 2);
    cache.access('p1', 'a'); // p1: [1]
    cache.access('p2', 'b'); // p2: [2]
    cache.access('p1', 'a'); // p1: [1,3] — accessed twice
    cache.access('p3', 'c'); // Evict p2 (only 1 access, K-th=0)
    
    assert.equal(cache.get('p1'), 'a');
    assert.equal(cache.get('p3'), 'c');
    assert.equal(cache.get('p2'), undefined); // Evicted
  });

  it('scan resistance: single-access pages evicted first', () => {
    const cache = new LRUK(3, 2);
    cache.access('hot', 'h');
    cache.access('hot', 'h'); // Hot page: 2 accesses
    cache.access('warm', 'w');
    cache.access('warm', 'w'); // Warm: 2 accesses
    cache.access('cold', 'c'); // Cold: 1 access
    
    // Now fill to evict
    cache.access('new', 'n');
    assert.equal(cache.get('cold'), undefined); // Cold evicted (1 access)
    assert.equal(cache.get('hot'), 'h'); // Hot survived
  });

  it('hit/miss tracking', () => {
    const cache = new LRUK(4, 2);
    const r1 = cache.access('p1', 'x');
    assert.ok(!r1.hit);
    const r2 = cache.access('p1', 'x');
    assert.ok(r2.hit);
  });

  it('capacity respected', () => {
    const cache = new LRUK(5, 2);
    for (let i = 0; i < 20; i++) cache.access(`p${i}`, i);
    assert.equal(cache.size, 5);
  });
});
