// hyperloglog.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HyperLogLog } from './hyperloglog.js';

describe('HyperLogLog', () => {
  it('basic cardinality estimation', () => {
    const hll = new HyperLogLog(10); // 1024 registers
    for (let i = 0; i < 10000; i++) hll.add(i);

    const est = hll.estimate();
    const error = Math.abs(est - 10000) / 10000;
    console.log(`    10K distinct: estimated ${est} (error: ${(error * 100).toFixed(1)}%)`);
    assert.ok(error < 0.1, `Error too high: ${(error * 100).toFixed(1)}%`);
  });

  it('handles duplicates correctly', () => {
    const hll = new HyperLogLog(10);
    for (let i = 0; i < 10000; i++) hll.add(i % 100); // Only 100 distinct

    const est = hll.estimate();
    const error = Math.abs(est - 100) / 100;
    assert.ok(error < 0.2, `Error too high: ${(error * 100).toFixed(1)}%`);
  });

  it('large cardinality: 1M distinct', () => {
    const hll = new HyperLogLog(12); // 4096 registers for ~1.6% error
    for (let i = 0; i < 1000000; i++) hll.add(i);

    const est = hll.estimate();
    const error = Math.abs(est - 1000000) / 1000000;
    console.log(`    1M distinct: estimated ${est} (error: ${(error * 100).toFixed(1)}%, memory: ${hll.getStats().memoryBytes} bytes)`);
    assert.ok(error < 0.1);
  });

  it('merge two HLLs', () => {
    const a = new HyperLogLog(10);
    const b = new HyperLogLog(10);
    
    for (let i = 0; i < 5000; i++) a.add(i);
    for (let i = 3000; i < 8000; i++) b.add(i); // Overlap: 3000-4999

    a.merge(b);
    const est = a.estimate();
    const error = Math.abs(est - 8000) / 8000;
    assert.ok(error < 0.15, `Merged error: ${(error * 100).toFixed(1)}%`);
  });

  it('empty HLL returns 0', () => {
    const hll = new HyperLogLog(10);
    assert.equal(hll.estimate(), 0);
  });

  it('single element', () => {
    const hll = new HyperLogLog(10);
    hll.add('only-one');
    assert.ok(hll.estimate() >= 1 && hll.estimate() <= 3);
  });

  it('stats report expected error', () => {
    const hll = new HyperLogLog(10);
    const stats = hll.getStats();
    assert.equal(stats.registers, 1024);
    assert.equal(stats.memoryBytes, 1024);
    assert.ok(stats.expectedError.includes('%'));
  });
});
