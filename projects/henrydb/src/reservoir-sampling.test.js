// reservoir-sampling.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ReservoirSampler } from './reservoir-sampling.js';

describe('ReservoirSampler', () => {
  it('returns k elements', () => {
    const rs = new ReservoirSampler(5);
    for (let i = 0; i < 100; i++) rs.add(i);
    assert.equal(rs.getSample().length, 5);
  });

  it('uniform distribution (statistical)', () => {
    const counts = new Array(100).fill(0);
    for (let trial = 0; trial < 10000; trial++) {
      const rs = new ReservoirSampler(10);
      for (let i = 0; i < 100; i++) rs.add(i);
      for (const v of rs.getSample()) counts[v]++;
    }
    // Each element should appear ~1000 times (10/100 * 10000)
    const avg = counts.reduce((a, b) => a + b) / 100;
    const maxDev = Math.max(...counts.map(c => Math.abs(c - avg)));
    console.log(`  Avg: ${avg.toFixed(0)}, Max deviation: ${maxDev}`);
    assert.ok(maxDev < avg * 0.15, 'Distribution too skewed');
  });

  it('works with small streams', () => {
    const rs = new ReservoirSampler(10);
    for (let i = 0; i < 3; i++) rs.add(i);
    assert.equal(rs.getSample().length, 3); // Less than k
  });
});
