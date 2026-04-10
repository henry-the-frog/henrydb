// hyperloglog.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HyperLogLog } from './hyperloglog.js';

describe('HyperLogLog — Accuracy', () => {
  it('estimates small cardinalities', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 100; i++) hll.add(`key-${i}`);
    
    const estimate = hll.count();
    const error = Math.abs(estimate - 100) / 100;
    console.log(`    100 distinct: estimate=${estimate}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.2, `Error ${error} too high for 100 elements`);
  });

  it('estimates medium cardinalities', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 10000; i++) hll.add(`key-${i}`);
    
    const estimate = hll.count();
    const error = Math.abs(estimate - 10000) / 10000;
    console.log(`    10K distinct: estimate=${estimate}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.15, `Error ${error} too high for 10K elements`);
  });

  it('estimates large cardinalities', () => {
    const hll = new HyperLogLog(14);
    for (let i = 0; i < 1000000; i++) hll.add(`key-${i}`);
    
    const estimate = hll.count();
    const error = Math.abs(estimate - 1000000) / 1000000;
    console.log(`    1M distinct: estimate=${estimate}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.05, `Error ${error} too high for 1M elements`);
  });

  it('handles duplicates correctly', () => {
    const hll = new HyperLogLog(14);
    // Add 100 distinct keys, each 10 times
    for (let rep = 0; rep < 10; rep++) {
      for (let i = 0; i < 100; i++) hll.add(`key-${i}`);
    }
    
    const estimate = hll.count();
    const error = Math.abs(estimate - 100) / 100;
    console.log(`    100 distinct × 10 reps: estimate=${estimate}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.2);
  });
});

describe('HyperLogLog — Properties', () => {
  it('memory usage is fixed regardless of cardinality', () => {
    const hll = new HyperLogLog(14);
    const memBefore = hll.memoryBytes;
    for (let i = 0; i < 100000; i++) hll.add(`key-${i}`);
    assert.equal(hll.memoryBytes, memBefore, 'Memory should not grow');
    console.log(`    Memory: ${hll.memoryBytes} bytes for any cardinality`);
  });

  it('standard error matches theory', () => {
    const hll = new HyperLogLog(14);
    const se = hll.standardError();
    console.log(`    p=14: standard error = ${(se * 100).toFixed(2)}% (theory: 0.81%)`);
    assert.ok(Math.abs(se - 0.0081) < 0.001, 'Should be close to 1.04/sqrt(16384)');
  });

  it('lower precision = higher error', () => {
    const hll10 = new HyperLogLog(10);
    const hll14 = new HyperLogLog(14);
    assert.ok(hll10.standardError() > hll14.standardError());
    console.log(`    p=10: ${(hll10.standardError() * 100).toFixed(2)}%, p=14: ${(hll14.standardError() * 100).toFixed(2)}%`);
  });
});

describe('HyperLogLog — Merge', () => {
  it('merge combines two HLLs', () => {
    const a = new HyperLogLog(14);
    const b = new HyperLogLog(14);
    
    for (let i = 0; i < 5000; i++) a.add(`a-${i}`);
    for (let i = 0; i < 5000; i++) b.add(`b-${i}`);
    
    a.merge(b);
    const estimate = a.count();
    const error = Math.abs(estimate - 10000) / 10000;
    console.log(`    Merged: estimate=${estimate}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.15);
  });

  it('merge handles overlapping sets', () => {
    const a = new HyperLogLog(14);
    const b = new HyperLogLog(14);
    
    // 50% overlap
    for (let i = 0; i < 10000; i++) a.add(`key-${i}`);
    for (let i = 5000; i < 15000; i++) b.add(`key-${i}`);
    
    a.merge(b);
    const estimate = a.count();
    const error = Math.abs(estimate - 15000) / 15000;
    console.log(`    Overlapping merge: estimate=${estimate} (expected 15000), error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.15);
  });
});

describe('HyperLogLog — Performance', () => {
  it('benchmark: 1M adds', () => {
    const hll = new HyperLogLog(14);
    const N = 1_000_000;
    
    const t0 = performance.now();
    for (let i = 0; i < N; i++) hll.add(`element-${i}`);
    const elapsed = performance.now() - t0;
    
    console.log(`    ${N} adds: ${elapsed.toFixed(1)}ms (${(N / elapsed * 1000) | 0}/sec)`);
    console.log(`    Estimate: ${hll.count()}, Memory: ${hll.memoryBytes} bytes`);
    
    assert.ok(elapsed < 5000);
  });
});
