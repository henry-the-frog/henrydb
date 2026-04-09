// red-black-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RedBlackTree } from './red-black-tree.js';

describe('RedBlackTree', () => {
  it('insert and get', () => {
    const rbt = new RedBlackTree();
    rbt.insert(5, 'e'); rbt.insert(3, 'c'); rbt.insert(7, 'g');
    assert.equal(rbt.get(5), 'e');
    assert.equal(rbt.get(3), 'c');
    assert.equal(rbt.size, 3);
  });

  it('sorted iteration', () => {
    const rbt = new RedBlackTree();
    [5, 3, 7, 1, 9, 2, 8].forEach(k => rbt.insert(k, k));
    assert.deepEqual([...rbt].map(e => e.key), [1, 2, 3, 5, 7, 8, 9]);
  });

  it('min and max', () => {
    const rbt = new RedBlackTree();
    [5, 3, 7].forEach(k => rbt.insert(k, k));
    assert.equal(rbt.min().key, 3);
    assert.equal(rbt.max().key, 7);
  });

  it('balanced: height <= 2*log2(n)', () => {
    const rbt = new RedBlackTree();
    // Insert in sorted order (worst case for BST)
    for (let i = 0; i < 10000; i++) rbt.insert(i, i);
    
    const h = rbt.height();
    const maxHeight = 2 * Math.ceil(Math.log2(10001));
    console.log(`  10K sorted insert: height=${h} (max allowed: ${maxHeight})`);
    assert.ok(h <= maxHeight, `Height ${h} exceeds 2*log2(N)=${maxHeight}`);
  });

  it('stress: 10K random insert + lookup', () => {
    const rbt = new RedBlackTree();
    const keys = Array.from({ length: 10000 }, () => Math.floor(Math.random() * 100000));
    
    const t0 = performance.now();
    for (const k of keys) rbt.insert(k, k);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (const k of keys) rbt.get(k);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
  });

  it('upsert', () => {
    const rbt = new RedBlackTree();
    rbt.insert(1, 'old');
    rbt.insert(1, 'new');
    assert.equal(rbt.get(1), 'new');
    assert.equal(rbt.size, 1);
  });
});
