// count-min-sketch.test.js — Tests for Count-Min Sketch
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CountMinSketch } from './count-min-sketch.js';

describe('CountMinSketch', () => {
  it('basic counting', () => {
    const cms = new CountMinSketch(1024, 5);
    cms.add('apple', 3);
    cms.add('banana', 1);
    cms.add('apple', 2);
    
    assert.ok(cms.estimate('apple') >= 5);
    assert.ok(cms.estimate('banana') >= 1);
    assert.equal(cms.totalCount, 6);
  });

  it('never underestimates', () => {
    const cms = new CountMinSketch(2048, 7);
    const counts = new Map();
    
    // Add random counts for 100 items
    for (let i = 0; i < 100; i++) {
      const key = `item-${i}`;
      const count = Math.floor(Math.random() * 100) + 1;
      counts.set(key, count);
      cms.add(key, count);
    }
    
    // Verify: estimate >= actual for all items
    for (const [key, actual] of counts) {
      const estimate = cms.estimate(key);
      assert.ok(estimate >= actual, `Underestimate for ${key}: got ${estimate}, expected >= ${actual}`);
    }
  });

  it('accuracy with 10K distinct items', () => {
    const cms = new CountMinSketch(4096, 5);
    const actuals = new Map();
    
    for (let i = 0; i < 10000; i++) {
      // Zipfian-like: some items much more frequent
      const key = `key-${i % 1000}`;
      actuals.set(key, (actuals.get(key) || 0) + 1);
      cms.add(key);
    }
    
    // Measure error
    let totalError = 0;
    let maxError = 0;
    for (const [key, actual] of actuals) {
      const estimate = cms.estimate(key);
      const error = estimate - actual;
      totalError += error;
      maxError = Math.max(maxError, error);
    }
    
    const avgError = totalError / actuals.size;
    console.log(`  10K items: avg error=${avgError.toFixed(2)}, max error=${maxError}`);
    
    // Average error should be small relative to total count
    assert.ok(avgError < cms.totalCount * 0.01, `Average error too high: ${avgError}`);
  });

  it('withErrorBounds creates appropriately sized sketch', () => {
    const cms = CountMinSketch.withErrorBounds(0.001, 0.01);
    const stats = cms.getStats();
    
    assert.ok(stats.width > 2000); // ceil(e/0.001) ≈ 2719
    assert.ok(stats.depth >= 4);   // ceil(ln(100)) ≈ 5
    console.log(`  ε=0.001, δ=0.01: width=${stats.width}, depth=${stats.depth}, ${stats.bytesUsed} bytes`);
  });

  it('estimate for missing key is 0 or near-0', () => {
    const cms = new CountMinSketch(4096, 5);
    for (let i = 0; i < 100; i++) cms.add(`item-${i}`);
    
    const estimate = cms.estimate('definitely-not-in-sketch');
    assert.ok(estimate >= 0);
    // Should be close to 0 for a sparse sketch
    console.log(`  Missing key estimate: ${estimate}`);
  });

  it('merge combines two sketches', () => {
    const cms1 = new CountMinSketch(1024, 5);
    const cms2 = new CountMinSketch(1024, 5);
    
    cms1.add('shared', 5);
    cms1.add('only1', 3);
    cms2.add('shared', 7);
    cms2.add('only2', 4);
    
    const merged = cms1.merge(cms2);
    assert.ok(merged.estimate('shared') >= 12);
    assert.ok(merged.estimate('only1') >= 3);
    assert.ok(merged.estimate('only2') >= 4);
  });

  it('clear resets all counters', () => {
    const cms = new CountMinSketch(1024, 5);
    cms.add('test', 100);
    cms.clear();
    assert.equal(cms.totalCount, 0);
    assert.equal(cms.estimate('test'), 0);
  });

  it('performance: 100K add + 100K estimate', () => {
    const cms = new CountMinSketch(4096, 5);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) cms.add(i);
    const addMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 100000; i++) cms.estimate(i);
    const estMs = performance.now() - t1;
    
    console.log(`  100K add: ${addMs.toFixed(1)}ms (${(addMs/100000*1000).toFixed(3)}µs avg)`);
    console.log(`  100K estimate: ${estMs.toFixed(1)}ms (${(estMs/100000*1000).toFixed(3)}µs avg)`);
    assert.ok(addMs < 500);
    assert.ok(estMs < 500);
  });

  it('use case: heavy hitter detection', () => {
    const cms = new CountMinSketch(2048, 5);
    
    // Simulate: 90% of traffic from 10 keys, 10% from 990 keys
    for (let round = 0; round < 100; round++) {
      for (let i = 0; i < 10; i++) cms.add(`hot-${i}`, 9);
      for (let i = 0; i < 990; i++) cms.add(`cold-${i}`);
    }
    
    // Hot keys should have much higher estimates
    const hotEstimate = cms.estimate('hot-0');
    const coldEstimate = cms.estimate('cold-0');
    
    console.log(`  Hot key estimate: ${hotEstimate} (actual: 900)`);
    console.log(`  Cold key estimate: ${coldEstimate} (actual: 100)`);
    
    assert.ok(hotEstimate > coldEstimate, 'Hot keys should have higher estimates');
    assert.ok(hotEstimate >= 900, 'Should not underestimate hot key');
  });

  it('getStats', () => {
    const cms = new CountMinSketch(1024, 5);
    const stats = cms.getStats();
    assert.equal(stats.width, 1024);
    assert.equal(stats.depth, 5);
    assert.equal(stats.bytesUsed, 1024 * 5 * 4);
    assert.ok(stats.epsilon > 0);
    assert.ok(stats.delta > 0 && stats.delta < 1);
  });
});
