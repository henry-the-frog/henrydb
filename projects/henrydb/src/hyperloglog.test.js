// hyperloglog.test.js — Tests for HyperLogLog cardinality estimation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HyperLogLog } from './hyperloglog.js';

describe('HyperLogLog', () => {
  it('basic cardinality estimation', () => {
    const hll = new HyperLogLog(14);
    
    // Add 1000 distinct elements
    for (let i = 0; i < 1000; i++) hll.add(`user-${i}`);
    
    const estimate = hll.estimate();
    const error = Math.abs(estimate - 1000) / 1000;
    
    console.log(`  1K distinct: estimate=${estimate}, error=${(error*100).toFixed(2)}%`);
    assert.ok(error < 0.1, `Error too high: ${(error*100).toFixed(2)}%`);
  });

  it('handles duplicates correctly', () => {
    const hll = new HyperLogLog(14);
    
    // Add same 100 elements 100 times each = 10K total, 100 distinct
    for (let round = 0; round < 100; round++) {
      for (let i = 0; i < 100; i++) hll.add(`item-${i}`);
    }
    
    const estimate = hll.estimate();
    const error = Math.abs(estimate - 100) / 100;
    
    console.log(`  100 distinct x 100 rounds: estimate=${estimate}, error=${(error*100).toFixed(2)}%`);
    assert.ok(error < 0.3, `Error too high: ${(error*100).toFixed(2)}%`);
  });

  it('10K distinct elements', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 10000; i++) hll.add(i);
    
    const estimate = hll.estimate();
    const error = Math.abs(estimate - 10000) / 10000;
    
    console.log(`  10K distinct: estimate=${estimate}, error=${(error*100).toFixed(2)}%`);
    assert.ok(error < 0.05, `Error too high: ${(error*100).toFixed(2)}%`);
  });

  it('100K distinct elements', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 100000; i++) hll.add(`key-${i}`);
    
    const estimate = hll.estimate();
    const error = Math.abs(estimate - 100000) / 100000;
    
    console.log(`  100K distinct: estimate=${estimate}, error=${(error*100).toFixed(2)}%`);
    assert.ok(error < 0.05, `Error too high: ${(error*100).toFixed(2)}%`);
  });

  it('empty estimator returns 0', () => {
    const hll = new HyperLogLog(14);
    assert.equal(hll.estimate(), 0);
  });

  it('merge combines two estimators', () => {
    const hll1 = new HyperLogLog(14);
    const hll2 = new HyperLogLog(14);
    
    // Set A: 0-999
    for (let i = 0; i < 1000; i++) hll1.add(i);
    // Set B: 500-1499 (overlaps with A)
    for (let i = 500; i < 1500; i++) hll2.add(i);
    
    const merged = hll1.merge(hll2);
    const estimate = merged.estimate();
    
    // Union: 0-1499 = 1500 distinct
    const error = Math.abs(estimate - 1500) / 1500;
    console.log(`  Merge (union 1500): estimate=${estimate}, error=${(error*100).toFixed(2)}%`);
    assert.ok(error < 0.1);
  });

  it('clear resets', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 1000; i++) hll.add(i);
    hll.clear();
    assert.equal(hll.estimate(), 0);
  });

  it('memory usage is constant regardless of input size', () => {
    const hll = new HyperLogLog(14);
    const stats1 = hll.getStats();
    
    for (let i = 0; i < 1000000; i++) hll.add(i);
    const stats2 = hll.getStats();
    
    assert.equal(stats1.bytesUsed, stats2.bytesUsed);
    console.log(`  Memory: ${stats2.bytesUsed} bytes (${(stats2.bytesUsed/1024).toFixed(1)}KB) for any input size`);
    console.log(`  Standard error: ${stats2.standardError}`);
  });

  it('different precisions', () => {
    for (const p of [4, 8, 12, 14, 16]) {
      const hll = new HyperLogLog(p);
      const n = 10000;
      for (let i = 0; i < n; i++) hll.add(i);
      
      const estimate = hll.estimate();
      const error = Math.abs(estimate - n) / n;
      const stats = hll.getStats();
      
      console.log(`  p=${p}: ${stats.registers} regs, ${stats.bytesUsed}B, est=${estimate}, err=${(error*100).toFixed(1)}%, SE=${stats.standardError}`);
    }
    assert.ok(true);
  });

  it('performance: 1M add', () => {
    const hll = new HyperLogLog(14);
    
    const t0 = performance.now();
    for (let i = 0; i < 1000000; i++) hll.add(i);
    const elapsed = performance.now() - t0;
    
    console.log(`  1M add: ${elapsed.toFixed(1)}ms (${(elapsed/1000000*1000).toFixed(3)}µs avg)`);
    console.log(`  Estimate: ${hll.estimate()} (actual: 1000000)`);
    assert.ok(elapsed < 5000);
  });

  it('getStats', () => {
    const hll = new HyperLogLog(14);
    const stats = hll.getStats();
    assert.equal(stats.precision, 14);
    assert.equal(stats.registers, 16384);
    assert.equal(stats.bytesUsed, 16384);
    assert.ok(stats.standardError.includes('%'));
  });
});
