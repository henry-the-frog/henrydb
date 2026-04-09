// minmax-heap.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MinMaxHeap } from './minmax-heap.js';

describe('MinMaxHeap', () => {
  it('peekMin and peekMax', () => {
    const h = new MinMaxHeap();
    [5, 3, 8, 1, 9, 2].forEach(v => h.push(v));
    assert.equal(h.peekMin(), 1);
    assert.equal(h.peekMax(), 9);
  });

  it('popMin extracts in ascending order', () => {
    const h = new MinMaxHeap();
    [5, 3, 8, 1, 9].forEach(v => h.push(v));
    assert.equal(h.popMin(), 1);
    assert.equal(h.popMin(), 3);
    assert.equal(h.popMin(), 5);
  });

  it('popMax extracts in descending order', () => {
    const h = new MinMaxHeap();
    [5, 3, 8, 1, 9].forEach(v => h.push(v));
    assert.equal(h.popMax(), 9);
    assert.equal(h.popMax(), 8);
    assert.equal(h.popMax(), 5);
  });

  it('mixed min/max operations', () => {
    const h = new MinMaxHeap();
    [4, 2, 6, 1, 8, 3].forEach(v => h.push(v));
    assert.equal(h.popMin(), 1); // Remove min
    assert.equal(h.popMax(), 8); // Remove max
    assert.equal(h.peekMin(), 2);
    assert.equal(h.peekMax(), 6);
  });

  it('stress: 5K elements', () => {
    const h = new MinMaxHeap();
    for (let i = 0; i < 5000; i++) h.push(Math.random() * 10000);
    
    let prev = -Infinity;
    while (!h.isEmpty) {
      const v = h.popMin();
      assert.ok(v >= prev, `Not sorted: ${v} < ${prev}`);
      prev = v;
    }
  });

  it('single element', () => {
    const h = new MinMaxHeap();
    h.push(42);
    assert.equal(h.peekMin(), 42);
    assert.equal(h.peekMax(), 42);
    assert.equal(h.popMax(), 42);
    assert.equal(h.isEmpty, true);
  });
});
