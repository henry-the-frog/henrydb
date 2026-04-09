// topk.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TopK } from './topk.js';

describe('TopK', () => {
  it('maintains top-k elements', () => {
    const tk = new TopK(3);
    [5, 1, 9, 3, 7, 2, 8].forEach(n => tk.add(n, n));
    assert.deepEqual(tk.getTop(), [9, 8, 7]);
  });

  it('threshold', () => {
    const tk = new TopK(2);
    tk.add('a', 10); tk.add('b', 20); tk.add('c', 5);
    assert.equal(tk.threshold(), 10);
  });

  it('10K stream', () => {
    const tk = new TopK(10);
    for (let i = 0; i < 10000; i++) tk.add(i, i);
    const top = tk.getTop();
    assert.equal(top[0], 9999);
    assert.equal(top.length, 10);
  });
});
