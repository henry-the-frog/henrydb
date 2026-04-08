// merge-iterator.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MergeIterator, kWayMerge, DeduplicatingMergeIterator, MinHeap } from './merge-iterator.js';

describe('MinHeap', () => {
  it('basic operations', () => {
    const h = new MinHeap((a, b) => a - b);
    h.push(5); h.push(2); h.push(8); h.push(1);
    assert.equal(h.pop(), 1);
    assert.equal(h.pop(), 2);
    assert.equal(h.pop(), 5);
    assert.equal(h.pop(), 8);
  });
});

describe('MergeIterator', () => {
  it('merge 2 sorted arrays', () => {
    const result = kWayMerge([[1, 3, 5], [2, 4, 6]]);
    assert.deepEqual(result, [1, 2, 3, 4, 5, 6]);
  });

  it('merge 3 sorted arrays', () => {
    const result = kWayMerge([[1, 4, 7], [2, 5, 8], [3, 6, 9]]);
    assert.deepEqual(result, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('merge with empty arrays', () => {
    const result = kWayMerge([[1, 3], [], [2, 4], []]);
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('merge single array', () => {
    assert.deepEqual(kWayMerge([[5, 10, 15]]), [5, 10, 15]);
  });

  it('merge no arrays', () => {
    assert.deepEqual(kWayMerge([]), []);
  });

  it('merge with duplicates', () => {
    const result = kWayMerge([[1, 2, 3], [2, 3, 4]]);
    assert.deepEqual(result, [1, 2, 2, 3, 3, 4]);
  });

  it('custom comparator (descending)', () => {
    const result = kWayMerge(
      [[9, 7, 5], [8, 6, 4]],
      (a, b) => b - a,
    );
    assert.deepEqual(result, [9, 8, 7, 6, 5, 4]);
  });

  it('string merge', () => {
    const result = kWayMerge([['apple', 'cherry'], ['banana', 'date']]);
    assert.deepEqual(result, ['apple', 'banana', 'cherry', 'date']);
  });
});

describe('DeduplicatingMergeIterator', () => {
  it('dedup by key', () => {
    const a = [{ key: 1, val: 'old' }, { key: 3, val: 'a' }];
    const b = [{ key: 1, val: 'new' }, { key: 2, val: 'b' }];
    
    const iter = new DeduplicatingMergeIterator(
      [a, b],
      v => v.key,
      (a, b) => a.key - b.key,
    );
    const result = iter.toArray();
    assert.equal(result.length, 3); // key 1 deduped
    assert.equal(result.find(r => r.key === 1).val, 'new'); // Latest wins
  });
});

describe('Benchmark', () => {
  it('100K 10-way merge', () => {
    const K = 10;
    const N = 10000;
    const arrays = Array.from({ length: K }, () => {
      const arr = Array.from({ length: N }, () => Math.floor(Math.random() * 1000000));
      return arr.sort((a, b) => a - b);
    });

    const t0 = Date.now();
    const result = kWayMerge(arrays);
    const ms = Date.now() - t0;

    console.log(`    100K 10-way merge: ${ms}ms`);
    assert.equal(result.length, K * N);
    // Verify sorted
    for (let i = 1; i < result.length; i++) assert.ok(result[i] >= result[i - 1]);
  });
});
