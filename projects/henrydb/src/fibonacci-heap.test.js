// fibonacci-heap.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FibonacciHeap } from './fibonacci-heap.js';

describe('FibonacciHeap', () => {
  it('insert and peekMin', () => {
    const h = new FibonacciHeap();
    h.insert(5); h.insert(3); h.insert(8);
    assert.equal(h.peekMin().key, 3);
  });

  it('extractMin in order', () => {
    const h = new FibonacciHeap();
    [5, 3, 8, 1, 9].forEach(k => h.insert(k));
    assert.equal(h.extractMin().key, 1);
    assert.equal(h.extractMin().key, 3);
    assert.equal(h.extractMin().key, 5);
  });

  it('decreaseKey', () => {
    const h = new FibonacciHeap();
    h.insert(10, 'a');
    h.insert(20, 'b');
    h.insert(30, 'c');
    
    h.decreaseKey('c', 1); // 30 → 1
    assert.equal(h.peekMin().value, 'c');
    assert.equal(h.extractMin().key, 1);
  });

  it('stress: 1K elements', () => {
    const h = new FibonacciHeap();
    for (let i = 1000; i >= 1; i--) h.insert(i);
    
    let prev = 0;
    let count = 0;
    while (!h.isEmpty) {
      const m = h.extractMin();
      assert.ok(m.key >= prev, `Not sorted: ${m.key} < ${prev}`);
      prev = m.key;
      count++;
    }
    assert.equal(count, 1000);
  });
});
