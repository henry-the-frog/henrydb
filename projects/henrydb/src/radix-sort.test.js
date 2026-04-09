// radix-sort.test.js — The 50th data structure! 🎉
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { countingSort, radixSort, bucketSort } from './radix-sort.js';

describe('🎉 #50: Radix/Counting/Bucket Sort', () => {
  it('countingSort', () => {
    assert.deepEqual(countingSort([3, 1, 4, 1, 5, 9, 2, 6], 9), [1, 1, 2, 3, 4, 5, 6, 9]);
  });

  it('radixSort', () => {
    assert.deepEqual(radixSort([170, 45, 75, 90, 802, 24, 2, 66]),
      [2, 24, 45, 66, 75, 90, 170, 802]);
  });

  it('bucketSort (floats)', () => {
    const arr = [0.78, 0.17, 0.39, 0.26, 0.72, 0.94, 0.21, 0.12, 0.23, 0.68];
    const sorted = bucketSort(arr);
    for (let i = 1; i < sorted.length; i++) assert.ok(sorted[i] >= sorted[i-1]);
  });

  it('radixSort 10K vs Array.sort', () => {
    const arr = Array.from({ length: 10000 }, () => Math.floor(Math.random() * 1000000));
    
    const copy = [...arr];
    const t0 = performance.now();
    const radixed = radixSort(arr);
    const radixMs = performance.now() - t0;
    
    const t1 = performance.now();
    copy.sort((a, b) => a - b);
    const jsMs = performance.now() - t1;
    
    assert.deepEqual(radixed, copy);
    console.log(`  10K radixSort: ${radixMs.toFixed(1)}ms vs Array.sort: ${jsMs.toFixed(1)}ms (${(jsMs/radixMs).toFixed(1)}x)`);
  });

  it('empty and single', () => {
    assert.deepEqual(radixSort([]), []);
    assert.deepEqual(radixSort([42]), [42]);
  });
});
