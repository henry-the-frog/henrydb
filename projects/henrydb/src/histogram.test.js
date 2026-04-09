// histogram.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EquiWidthHistogram } from './histogram.js';

describe('EquiWidthHistogram', () => {
  it('selectivity estimation', () => {
    const data = Array.from({ length: 1000 }, (_, i) => i);
    const h = new EquiWidthHistogram(data, 10);
    
    // Full range should be ~1.0
    const full = h.selectivity(0, 1000);
    assert.ok(full > 0.9);
    
    // Half range should be ~0.5
    const half = h.selectivity(0, 500);
    assert.ok(half > 0.4 && half < 0.6);
  });

  it('bucket count matches', () => {
    const h = new EquiWidthHistogram([1, 2, 3, 4, 5], 5);
    assert.equal(h.buckets.length, 5);
  });
});
