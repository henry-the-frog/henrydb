// interval-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IntervalTree, MinHeap } from './interval-tree.js';

describe('MinHeap', () => {
  it('maintains min order', () => {
    const heap = new MinHeap();
    heap.push(5);
    heap.push(3);
    heap.push(7);
    heap.push(1);
    
    assert.equal(heap.pop(), 1);
    assert.equal(heap.pop(), 3);
    assert.equal(heap.pop(), 5);
    assert.equal(heap.pop(), 7);
  });

  it('peek returns minimum without removal', () => {
    const heap = new MinHeap();
    heap.push(10);
    heap.push(5);
    assert.equal(heap.peek(), 5);
    assert.equal(heap.size, 2);
  });

  it('handles custom comparator (max-heap)', () => {
    const maxHeap = new MinHeap((a, b) => b - a);
    maxHeap.push(1);
    maxHeap.push(5);
    maxHeap.push(3);
    
    assert.equal(maxHeap.pop(), 5);
    assert.equal(maxHeap.pop(), 3);
  });

  it('top-K pattern', () => {
    const heap = new MinHeap();
    const data = [50, 20, 80, 10, 40, 60, 30, 70, 90];
    for (const d of data) heap.push(d);
    
    // Extract top 3 (smallest)
    const top3 = [];
    for (let i = 0; i < 3; i++) top3.push(heap.pop());
    assert.deepEqual(top3, [10, 20, 30]);
  });

  it('handles objects with comparator', () => {
    const heap = new MinHeap((a, b) => a.priority - b.priority);
    heap.push({ task: 'low', priority: 3 });
    heap.push({ task: 'high', priority: 1 });
    heap.push({ task: 'med', priority: 2 });
    
    assert.equal(heap.pop().task, 'high');
    assert.equal(heap.pop().task, 'med');
  });
});

describe('IntervalTree', () => {
  it('point query finds containing intervals', () => {
    const tree = new IntervalTree();
    tree.insert(1, 5, 'A');
    tree.insert(3, 8, 'B');
    tree.insert(10, 15, 'C');
    
    const results = tree.queryPoint(4);
    assert.equal(results.length, 2); // A and B contain 4
  });

  it('range query finds overlapping intervals', () => {
    const tree = new IntervalTree();
    tree.insert(1, 5, 'A');
    tree.insert(3, 8, 'B');
    tree.insert(10, 15, 'C');
    
    const results = tree.queryRange(4, 12);
    assert.equal(results.length, 3); // All overlap with [4, 12]
  });

  it('contained query finds nested intervals', () => {
    const tree = new IntervalTree();
    tree.insert(1, 10, 'outer');
    tree.insert(3, 7, 'inner');
    tree.insert(5, 5, 'point');
    
    const results = tree.queryContained(2, 8);
    assert.equal(results.length, 2); // inner and point
  });

  it('scheduling use case', () => {
    const tree = new IntervalTree();
    // Meeting schedule
    tree.insert(9, 10, 'Standup');
    tree.insert(10, 11, 'Design review');
    tree.insert(14, 15, 'Sprint planning');
    tree.insert(9, 17, 'Available');
    
    // What's happening at 10:30?
    const at1030 = tree.queryPoint(10.5);
    assert.ok(at1030.some(i => i.data === 'Design review'));
    assert.ok(at1030.some(i => i.data === 'Available'));
    
    // What overlaps with lunch (12-13)?
    const lunch = tree.queryRange(12, 13);
    assert.equal(lunch.length, 1); // Only 'Available'
  });

  it('remove intervals', () => {
    const tree = new IntervalTree();
    tree.insert(1, 5, 'A');
    tree.insert(3, 8, 'B');
    
    tree.remove(i => i.data === 'A');
    assert.equal(tree.size, 1);
    assert.equal(tree.queryPoint(4).length, 1);
  });
});
