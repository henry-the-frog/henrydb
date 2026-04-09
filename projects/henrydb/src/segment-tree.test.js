// segment-tree.test.js — Tests for segment tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SegmentTree } from './segment-tree.js';

describe('SegmentTree', () => {
  it('range sum query', () => {
    const st = SegmentTree.sum([1, 2, 3, 4, 5]);
    assert.equal(st.query(0, 4), 15);
    assert.equal(st.query(1, 3), 9);
    assert.equal(st.query(2, 2), 3);
  });

  it('range min query', () => {
    const st = SegmentTree.min([5, 2, 8, 1, 9, 3]);
    assert.equal(st.query(0, 5), 1);
    assert.equal(st.query(0, 2), 2);
    assert.equal(st.query(3, 5), 1);
  });

  it('range max query', () => {
    const st = SegmentTree.max([5, 2, 8, 1, 9, 3]);
    assert.equal(st.query(0, 5), 9);
    assert.equal(st.query(0, 2), 8);
  });

  it('point update', () => {
    const st = SegmentTree.sum([1, 2, 3, 4, 5]);
    st.update(2, 10); // [1, 2, 10, 4, 5]
    assert.equal(st.query(0, 4), 22);
    assert.equal(st.query(2, 2), 10);
  });

  it('large array: 10K elements', () => {
    const arr = Array.from({ length: 10000 }, (_, i) => i);
    const st = SegmentTree.sum(arr);
    
    assert.equal(st.query(0, 9999), 49995000);
    assert.equal(st.query(0, 99), 4950);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) st.query(0, i);
    const queryMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) st.update(i, i + 1);
    const updateMs = performance.now() - t1;
    
    console.log(`  10K queries: ${queryMs.toFixed(1)}ms | 10K updates: ${updateMs.toFixed(1)}ms`);
  });

  it('single element', () => {
    const st = SegmentTree.sum([42]);
    assert.equal(st.query(0, 0), 42);
    st.update(0, 99);
    assert.equal(st.query(0, 0), 99);
  });

  it('custom combine function', () => {
    // Product segment tree
    const st = new SegmentTree([2, 3, 4, 5], (a, b) => a * b, 1);
    assert.equal(st.query(0, 3), 120);
    assert.equal(st.query(1, 2), 12);
  });
});
