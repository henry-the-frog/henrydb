// lru-replacer.test.js — Tests for LRU page replacement policy
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LRUReplacer } from './lru-replacer.js';

describe('LRUReplacer', () => {
  it('evict returns LRU frame', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    r.record(2);
    r.record(3);
    
    // Frame 0 was recorded first — it's the LRU
    assert.equal(r.evict(), 0);
    assert.equal(r.evict(), 1);
    assert.equal(r.evict(), 2);
    assert.equal(r.evict(), 3);
    assert.equal(r.evict(), -1); // Empty
  });

  it('record moves frame to MRU position', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    r.record(2);
    
    // Access frame 0 again — moves to MRU
    r.record(0);
    
    // Now LRU order is: 1, 2, 0
    assert.equal(r.evict(), 1);
    assert.equal(r.evict(), 2);
    assert.equal(r.evict(), 0);
  });

  it('size tracks evictable frames', () => {
    const r = new LRUReplacer(4);
    assert.equal(r.size(), 0);
    
    r.record(0);
    assert.equal(r.size(), 1);
    
    r.record(1);
    assert.equal(r.size(), 2);
    
    r.evict();
    assert.equal(r.size(), 1);
  });

  it('pin prevents eviction', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    r.record(2);
    
    // Pin frame 0
    r.pin(0);
    assert.equal(r.size(), 2); // Only 1 and 2 are evictable
    
    // Evict should skip pinned frame 0
    assert.equal(r.evict(), 1);
    assert.equal(r.evict(), 2);
    assert.equal(r.evict(), -1); // Frame 0 is pinned, can't evict
  });

  it('unpin makes frame evictable again', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    
    r.pin(0);
    assert.equal(r.size(), 1);
    
    r.unpin(0);
    assert.equal(r.size(), 2);
    
    // Frame 0 was just unpinned — it's at MRU position
    assert.equal(r.evict(), 1); // 1 is LRU
    assert.equal(r.evict(), 0); // 0 was re-added at MRU
  });

  it('remove eliminates frame entirely', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    r.record(2);
    
    r.remove(1);
    assert.equal(r.size(), 2);
    
    assert.equal(r.evict(), 0);
    assert.equal(r.evict(), 2);
    assert.equal(r.evict(), -1);
  });

  it('remove pinned frame', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.pin(0);
    
    r.remove(0);
    assert.equal(r.isPinned(0), false);
    assert.equal(r.size(), 0);
  });

  it('pin then record does not add to evictable', () => {
    const r = new LRUReplacer(4);
    r.pin(0);
    r.record(0); // Should not add to evictable list
    
    assert.equal(r.size(), 0);
    assert.equal(r.evict(), -1);
  });

  it('multiple pin/unpin cycles', () => {
    const r = new LRUReplacer(4);
    r.record(0);
    r.record(1);
    
    // Pin and unpin multiple times
    r.pin(0);
    r.unpin(0);
    r.pin(0);
    r.unpin(0);
    
    assert.equal(r.size(), 2);
    // Frame 0 should be at MRU after last unpin
    assert.equal(r.evict(), 1);
    assert.equal(r.evict(), 0);
  });

  it('stress: 10K frames with eviction', () => {
    const r = new LRUReplacer(10000);
    
    // Add 10K frames
    for (let i = 0; i < 10000; i++) r.record(i);
    assert.equal(r.size(), 10000);
    
    // Pin every other frame
    for (let i = 0; i < 10000; i += 2) r.pin(i);
    assert.equal(r.size(), 5000);
    
    // Evict all unpinned
    const evicted = [];
    for (let i = 0; i < 5000; i++) {
      const frame = r.evict();
      assert.ok(frame >= 0);
      assert.ok(frame % 2 === 1, `Expected odd frame, got ${frame}`);
      evicted.push(frame);
    }
    assert.equal(evicted[0], 1); // First odd frame is LRU
    assert.equal(r.evict(), -1); // All unpinned evicted
    
    // Unpin remaining
    for (let i = 0; i < 10000; i += 2) r.unpin(i);
    assert.equal(r.size(), 5000);
  });

  it('isEvictable and isPinned', () => {
    const r = new LRUReplacer(4);
    
    assert.equal(r.isEvictable(0), false);
    assert.equal(r.isPinned(0), false);
    
    r.record(0);
    assert.equal(r.isEvictable(0), true);
    assert.equal(r.isPinned(0), false);
    
    r.pin(0);
    assert.equal(r.isEvictable(0), false);
    assert.equal(r.isPinned(0), true);
  });

  it('evict performance: O(1)', () => {
    const r = new LRUReplacer(100000);
    for (let i = 0; i < 100000; i++) r.record(i);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) r.evict();
    const elapsed = performance.now() - t0;
    
    console.log(`  100K evictions: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
    // Should be well under 100ms for 100K evictions
    assert.ok(elapsed < 500, `Expected <500ms, got ${elapsed.toFixed(1)}ms`);
  });

  it('record performance: O(1)', () => {
    const r = new LRUReplacer(100000);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) r.record(i);
    const elapsed = performance.now() - t0;
    
    console.log(`  100K records: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 500, `Expected <500ms, got ${elapsed.toFixed(1)}ms`);
  });

  it('LRU access pattern: working set stays in cache', () => {
    // Simulate a buffer pool scenario: 4 frames, pages accessed in pattern
    const r = new LRUReplacer(4);
    
    // Working set: pages 0-3 in a 4-frame pool
    r.record(0); r.record(1); r.record(2); r.record(3);
    
    // Access pattern: repeatedly touch 0 and 1 (hot pages)
    for (let i = 0; i < 10; i++) {
      r.record(0);
      r.record(1);
    }
    
    // Pages 2 and 3 are LRU (cold). Evict should return them first.
    assert.equal(r.evict(), 2);
    assert.equal(r.evict(), 3);
    
    // Hot pages 0 and 1 survive
    assert.equal(r.size(), 2);
    assert.equal(r.isEvictable(0), true);
    assert.equal(r.isEvictable(1), true);
  });
});
