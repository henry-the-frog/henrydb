// avl-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { AVLTree } from './avl-tree.js';

describe('AVLTree', () => {
  it('insert and get', () => {
    const t = new AVLTree();
    t.insert(5, 'e'); t.insert(3, 'c'); t.insert(7, 'g');
    assert.equal(t.get(5), 'e');
    assert.equal(t.get(3), 'c');
  });

  it('strictly balanced: height <= 1.44*log2(n)', () => {
    const t = new AVLTree();
    for (let i = 0; i < 10000; i++) t.insert(i, i);
    const maxH = Math.ceil(1.44 * Math.log2(10001));
    console.log(`  10K sorted: height=${t.height} (max ${maxH})`);
    assert.ok(t.height <= maxH + 2);
  });

  it('delete', () => {
    const t = new AVLTree();
    [3, 1, 5, 2, 4].forEach(k => t.insert(k, k));
    t.delete(3);
    assert.equal(t.has(3), false);
    assert.equal(t.size, 4);
  });

  it('sorted iteration', () => {
    const t = new AVLTree();
    [5, 3, 7, 1, 9].forEach(k => t.insert(k, k));
    assert.deepEqual([...t].map(e => e.key), [1, 3, 5, 7, 9]);
  });

  it('AVL vs RB: AVL shorter height', async () => {
    const { RedBlackTree } = await import('./red-black-tree.js');
    const avl = new AVLTree();
    const rb = new RedBlackTree();
    
    for (let i = 0; i < 10000; i++) {
      avl.insert(i, i);
      rb.insert(i, i);
    }
    
    console.log(`  AVL height: ${avl.height}, RB height: ${rb.height()}`);
    assert.ok(avl.height <= rb.height(), 'AVL should be shorter or equal');
  });

  it('stress: 10K', () => {
    const t = new AVLTree();
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) t.insert(i, i);
    const insertMs = performance.now() - t0;
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) t.get(i);
    const lookupMs = performance.now() - t1;
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
  });
});
