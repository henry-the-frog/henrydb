// priority-queue.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MinHeap } from './priority-queue.js';

describe('MinHeap', () => {
  it('extracts in sorted order', () => {
    const h = new MinHeap();
    [5, 3, 8, 1, 2].forEach(v => h.push(v));
    assert.deepEqual([h.pop(), h.pop(), h.pop(), h.pop(), h.pop()], [1, 2, 3, 5, 8]);
  });

  it('peek returns min without removal', () => {
    const h = new MinHeap();
    h.push(3); h.push(1); h.push(2);
    assert.equal(h.peek(), 1);
    assert.equal(h.size, 3);
  });

  it('custom comparator (max heap)', () => {
    const h = new MinHeap((a, b) => b - a);
    [1, 5, 3].forEach(v => h.push(v));
    assert.equal(h.pop(), 5);
    assert.equal(h.pop(), 3);
  });

  it('iterator yields sorted order', () => {
    const h = new MinHeap();
    [4, 2, 6, 1].forEach(v => h.push(v));
    assert.deepEqual([...h], [1, 2, 4, 6]);
  });

  it('10K elements performance', () => {
    const h = new MinHeap();
    const t0 = performance.now();
    for (let i = 10000; i >= 1; i--) h.push(i);
    const pushMs = performance.now() - t0;
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) h.pop();
    const popMs = performance.now() - t1;
    console.log(`  10K push: ${pushMs.toFixed(1)}ms | 10K pop: ${popMs.toFixed(1)}ms`);
    assert.ok(pushMs < 100);
  });

  it('use case: k-way merge', () => {
    const h = new MinHeap((a, b) => a.value - b.value);
    const lists = [[1, 4, 7], [2, 5, 8], [3, 6, 9]];
    const ptrs = lists.map(() => 0);
    
    for (let i = 0; i < lists.length; i++) {
      h.push({ value: lists[i][0], listIdx: i });
    }
    
    const merged = [];
    while (!h.isEmpty) {
      const { value, listIdx } = h.pop();
      merged.push(value);
      ptrs[listIdx]++;
      if (ptrs[listIdx] < lists[listIdx].length) {
        h.push({ value: lists[listIdx][ptrs[listIdx]], listIdx });
      }
    }
    assert.deepEqual(merged, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });
});
