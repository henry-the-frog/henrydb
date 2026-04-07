// streaming.test.js — Count-Min Sketch and HyperLogLog tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CountMinSketch, HyperLogLog } from './streaming.js';

describe('Count-Min Sketch', () => {
  it('estimates frequency of single item', () => {
    const cms = new CountMinSketch(1024, 5);
    for (let i = 0; i < 100; i++) cms.add('hello');
    
    assert.ok(cms.estimate('hello') >= 100);
  });

  it('never underestimates', () => {
    const cms = new CountMinSketch(256, 4);
    const counts = { a: 50, b: 30, c: 20 };
    
    for (const [item, count] of Object.entries(counts)) {
      for (let i = 0; i < count; i++) cms.add(item);
    }
    
    assert.ok(cms.estimate('a') >= 50);
    assert.ok(cms.estimate('b') >= 30);
    assert.ok(cms.estimate('c') >= 20);
  });

  it('unseen items have low estimates', () => {
    const cms = new CountMinSketch(1024, 5);
    cms.add('seen', 100);
    
    // Unseen items may have small non-zero estimates due to collisions
    assert.ok(cms.estimate('unseen') < 20);
  });

  it('supports bulk addition', () => {
    const cms = new CountMinSketch();
    cms.add('item', 1000);
    assert.ok(cms.estimate('item') >= 1000);
  });

  it('tracks total count', () => {
    const cms = new CountMinSketch();
    cms.add('a', 10);
    cms.add('b', 20);
    assert.equal(cms.totalCount, 30);
  });
});

describe('HyperLogLog', () => {
  it('estimates cardinality of small set', () => {
    const hll = new HyperLogLog(14);
    const items = new Set();
    for (let i = 0; i < 100; i++) {
      const item = `item_${i}`;
      items.add(item);
      hll.add(item);
    }
    
    const estimate = hll.estimate();
    // Should be within 20% of actual
    assert.ok(estimate > 80 && estimate < 120, `Expected ~100, got ${estimate}`);
  });

  it('handles duplicates correctly', () => {
    const hll = new HyperLogLog(14);
    // Add same 10 items many times
    for (let round = 0; round < 100; round++) {
      for (let i = 0; i < 10; i++) {
        hll.add(`item_${i}`);
      }
    }
    
    const estimate = hll.estimate();
    assert.ok(estimate > 5 && estimate < 20, `Expected ~10, got ${estimate}`);
  });

  it('estimates larger cardinalities', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 10000; i++) {
      hll.add(`item_${i}`);
    }
    
    const estimate = hll.estimate();
    // Within 10% for large sets with p=14
    assert.ok(estimate > 8000 && estimate < 12000, `Expected ~10000, got ${estimate}`);
  });

  it('merge combines two HLLs', () => {
    const hll1 = new HyperLogLog(14);
    const hll2 = new HyperLogLog(14);
    
    for (let i = 0; i < 500; i++) hll1.add(`a_${i}`);
    for (let i = 0; i < 500; i++) hll2.add(`b_${i}`);
    
    hll1.merge(hll2);
    const estimate = hll1.estimate();
    assert.ok(estimate > 600 && estimate < 1500, `Expected ~1000, got ${estimate}`);
  });

  it('empty HLL estimates 0', () => {
    const hll = new HyperLogLog(10);
    assert.equal(hll.estimate(), 0);
  });
});
