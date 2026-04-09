// clock-replacer.test.js — Tests for Clock (Second Chance) page replacement
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ClockReplacer } from './clock-replacer.js';

describe('ClockReplacer', () => {
  it('evict returns frame with lowest usage', () => {
    const r = new ClockReplacer(4, 5);
    r.record(0);
    r.record(1);
    r.record(2);
    
    // All have usage_count=1. First eviction sweeps:
    // frame 0: count 1 → 0, skip
    // frame 1: count 1 → 0, skip
    // frame 2: count 1 → 0, skip
    // frame 0: count 0 → evict!
    assert.equal(r.evict(), 0);
  });

  it('frequently accessed frames survive eviction', () => {
    const r = new ClockReplacer(4, 5);
    r.record(0); // count=1
    r.record(1); // count=1
    r.record(2); // count=1
    
    // Access frame 2 multiple times (build up heat)
    r.record(2); // count=2
    r.record(2); // count=3
    r.record(2); // count=4
    
    // Evict: should evict 0 and 1 before 2
    assert.equal(r.evict(), 0);
    assert.equal(r.evict(), 1);
    assert.equal(r.evict(), 2); // Finally evict 2
  });

  it('pin prevents eviction', () => {
    const r = new ClockReplacer(4, 5);
    r.record(0);
    r.record(1);
    r.record(2);
    
    r.pin(0);
    assert.equal(r.size(), 2);
    
    // Evict should skip pinned frame 0
    const evicted = [];
    evicted.push(r.evict());
    evicted.push(r.evict());
    assert.equal(r.evict(), -1); // Only 0 left but pinned
    
    assert.ok(!evicted.includes(0), 'Pinned frame 0 should not be evicted');
  });

  it('unpin makes frame evictable', () => {
    const r = new ClockReplacer(4, 5);
    r.record(0);
    r.pin(0);
    assert.equal(r.size(), 0);
    
    r.unpin(0);
    assert.equal(r.size(), 1);
    assert.equal(r.evict(), 0);
  });

  it('usage count capped at maxUsage', () => {
    const r = new ClockReplacer(4, 3);
    r.record(0);
    for (let i = 0; i < 100; i++) r.record(0);
    
    assert.equal(r.getUsageCount(0), 3);
  });

  it('remove eliminates frame entirely', () => {
    const r = new ClockReplacer(4, 5);
    r.record(0);
    r.record(1);
    
    r.remove(0);
    assert.equal(r.size(), 1);
    assert.equal(r.evict(), 1);
  });

  it('stress: 10K frames with mixed access', () => {
    const r = new ClockReplacer(10000, 5);
    
    for (let i = 0; i < 10000; i++) r.record(i);
    
    // Hot set: access frames 0-99 many times
    for (let round = 0; round < 10; round++) {
      for (let i = 0; i < 100; i++) r.record(i);
    }
    
    // Evict 9000 frames — hot set should survive
    const evicted = new Set();
    for (let i = 0; i < 9000; i++) {
      const frame = r.evict();
      assert.ok(frame >= 0);
      evicted.add(frame);
    }
    
    // Hot frames (0-99) should mostly NOT be in evicted set
    let hotEvicted = 0;
    for (let i = 0; i < 100; i++) {
      if (evicted.has(i)) hotEvicted++;
    }
    
    console.log(`  Clock sweep 10K: ${hotEvicted}/100 hot frames evicted (lower is better)`);
    // With usage_count=5, hot frames have count~5 vs cold frames count~1
    // Clock should evict most cold frames before hot ones
    assert.ok(hotEvicted < 50, `Expected most hot frames to survive, but ${hotEvicted} were evicted`);
  });

  it('sequential flooding resistance (Clock vs LRU)', () => {
    const clock = new ClockReplacer(10, 5);
    
    // Working set: 10 pages, accessed repeatedly
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < 10; i++) clock.record(i);
    }
    // Working set frames now have high usage_count
    
    // Sequential scan: 20 cold pages
    for (let i = 10; i < 30; i++) clock.record(i);
    
    // How many working set frames survived?
    let survived = 0;
    for (let i = 0; i < 10; i++) {
      if (clock.isEvictable(i) || clock.isPinned(i)) survived++;
    }
    
    console.log(`  Sequential flood: ${survived}/10 working set frames survived in Clock`);
    // Clock should preserve more working set frames than LRU would
    // because working set has high usage_count
  });

  it('evict performance', () => {
    const r = new ClockReplacer(100000, 5);
    for (let i = 0; i < 100000; i++) r.record(i);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) r.evict();
    const elapsed = performance.now() - t0;
    
    console.log(`  100K evictions: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 2000, `Expected <2s, got ${elapsed.toFixed(1)}ms`);
  });

  it('record performance', () => {
    const r = new ClockReplacer(100000, 5);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) r.record(i);
    const elapsed = performance.now() - t0;
    
    console.log(`  100K records: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 2000, `Expected <2s, got ${elapsed.toFixed(1)}ms`);
  });
});
