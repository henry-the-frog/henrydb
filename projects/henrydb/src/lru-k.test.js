// lru-k.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LRUK } from './lru-k.js';

describe('LRU-K', () => {
  it('K=2: cold pages (1 access) evicted before warm (2+ accesses)', () => {
    const lru = new LRUK(2);
    lru.record(1); lru.record(2); lru.record(3);
    lru.record(1); // Page 1 now warm (2 accesses)
    
    // Pages 2,3 are cold (1 access). Evict oldest cold first.
    assert.equal(lru.evict(), 2);
    assert.equal(lru.evict(), 3);
    assert.equal(lru.evict(), 1); // Last: warm page
  });

  it('K=2: among warm pages, evict oldest K-th access', () => {
    const lru = new LRUK(2);
    lru.record(1); lru.record(2);
    lru.record(1); lru.record(2); // Both warm
    
    // Page 1's K-th access is older
    assert.equal(lru.evict(), 1);
  });

  it('resists sequential flooding', () => {
    const lru = new LRUK(2);
    // Hot page accessed many times
    for (let i = 0; i < 10; i++) lru.record(0);
    
    // Sequential scan: each page accessed once
    for (let i = 1; i <= 5; i++) lru.record(i);
    
    // Cold scan pages should be evicted before hot page
    const evicted = [];
    for (let i = 0; i < 5; i++) evicted.push(lru.evict());
    
    assert.ok(!evicted.includes(0), 'Hot page should survive sequential flooding');
    assert.equal(evicted.sort((a,b) => a-b).join(','), '1,2,3,4,5');
  });

  it('pin prevents eviction', () => {
    const lru = new LRUK(2);
    lru.record(1); lru.record(2);
    lru.pin(1);
    
    assert.equal(lru.evict(), 2);
    assert.equal(lru.evict(), null); // 1 is pinned
    
    lru.unpin(1);
    assert.equal(lru.evict(), 1);
  });

  it('getStats', () => {
    const lru = new LRUK(2);
    lru.record(1); lru.record(2);
    lru.record(1); // 1 is warm, 2 is cold
    lru.pin(1);
    
    const s = lru.getStats();
    assert.equal(s.cold, 1);
    assert.equal(s.warm, 1);
    assert.equal(s.pinned, 1);
  });

  it('K=3: needs 3 accesses to be warm', () => {
    const lru = new LRUK(3);
    lru.record(1); lru.record(1); // 2 accesses — still cold
    lru.record(2); lru.record(2); lru.record(2); // 3 — warm
    
    assert.equal(lru.evict(), 1); // Cold evicted first
  });

  it('performance: 10K frames', () => {
    const lru = new LRUK(2);
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) {
      lru.record(i);
      if (i % 3 === 0) lru.record(i); // Make some warm
    }
    for (let i = 0; i < 5000; i++) lru.evict();
    const elapsed = performance.now() - t0;
    
    console.log(`  10K record + 5K evict: ${elapsed.toFixed(1)}ms`);
    assert.ok(elapsed < 2000);
  });
});
