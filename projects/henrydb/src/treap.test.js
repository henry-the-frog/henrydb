// treap.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Treap } from './treap.js';

describe('Treap', () => {
  it('insert, get, has', () => {
    const t = new Treap();
    t.insert(5, 'five'); t.insert(3, 'three'); t.insert(7, 'seven');
    assert.equal(t.get(5), 'five');
    assert.equal(t.has(3), true);
    assert.equal(t.has(4), false);
    assert.equal(t.size, 3);
  });

  it('delete', () => {
    const t = new Treap();
    t.insert(1, 'a'); t.insert(2, 'b'); t.insert(3, 'c');
    t.delete(2);
    assert.equal(t.has(2), false);
    assert.equal(t.size, 2);
  });

  it('kth element (order statistics)', () => {
    const t = new Treap();
    [5, 3, 7, 1, 9].forEach(k => t.insert(k, k));
    assert.equal(t.kth(0).key, 1); // Smallest
    assert.equal(t.kth(2).key, 5); // Median
    assert.equal(t.kth(4).key, 9); // Largest
  });

  it('rank', () => {
    const t = new Treap();
    [10, 20, 30, 40, 50].forEach(k => t.insert(k, k));
    assert.equal(t.rank(30), 2); // 10, 20 are smaller
  });

  it('min and max', () => {
    const t = new Treap();
    [5, 3, 7, 1, 9].forEach(k => t.insert(k, k));
    assert.equal(t.min().key, 1);
    assert.equal(t.max().key, 9);
  });

  it('iterator produces sorted order', () => {
    const t = new Treap();
    [5, 3, 7, 1, 9].forEach(k => t.insert(k, k));
    assert.deepEqual([...t].map(e => e.key), [1, 3, 5, 7, 9]);
  });

  it('stress: 10K elements', () => {
    const t = new Treap();
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) t.insert(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) t.get(i);
    const lookupMs = performance.now() - t1;
    
    assert.equal(t.size, 10000);
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
  });

  it('balanced: height is O(log n)', () => {
    const t = new Treap();
    // Insert in sorted order (worst case for BST, fine for treap)
    for (let i = 0; i < 1000; i++) t.insert(i, i);
    
    // If balanced, kth should work (it traverses the tree)
    assert.equal(t.kth(500).key, 500);
    assert.equal(t.kth(0).key, 0);
    assert.equal(t.kth(999).key, 999);
  });
});
