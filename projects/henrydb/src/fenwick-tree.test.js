// fenwick-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FenwickTree } from './fenwick-tree.js';

describe('FenwickTree', () => {
  it('prefix sum', () => {
    const ft = FenwickTree.fromArray([1, 2, 3, 4, 5]);
    assert.equal(ft.prefixSum(0), 1);
    assert.equal(ft.prefixSum(2), 6);
    assert.equal(ft.prefixSum(4), 15);
  });

  it('range sum', () => {
    const ft = FenwickTree.fromArray([1, 2, 3, 4, 5]);
    assert.equal(ft.rangeSum(1, 3), 9);
    assert.equal(ft.rangeSum(0, 4), 15);
  });

  it('point update', () => {
    const ft = FenwickTree.fromArray([1, 2, 3, 4, 5]);
    ft.update(2, 7); // Add 7 to index 2: [1, 2, 10, 4, 5]
    assert.equal(ft.prefixSum(4), 22);
  });

  it('10K elements performance', () => {
    const arr = Array.from({ length: 10000 }, () => Math.random());
    const ft = FenwickTree.fromArray(arr);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) ft.prefixSum(i);
    const queryMs = performance.now() - t0;
    
    console.log(`  10K prefix sums: ${queryMs.toFixed(1)}ms`);
    assert.ok(queryMs < 100);
  });
});
