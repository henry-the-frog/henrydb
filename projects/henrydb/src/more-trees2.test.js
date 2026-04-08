// more-trees2.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ScapegoatTree, WeightBalancedTree } from './more-trees2.js';

describe('ScapegoatTree', () => {
  it('insert and search', () => {
    const tree = new ScapegoatTree();
    tree.insert(5, 'five');
    tree.insert(3, 'three');
    tree.insert(7, 'seven');
    assert.equal(tree.search(5), 'five');
    assert.equal(tree.search(3), 'three');
    assert.equal(tree.search(99), undefined);
  });

  it('sequential inserts stay balanced', () => {
    const tree = new ScapegoatTree(0.7);
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    assert.equal(tree.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(tree.search(i), i);
  });

  it('update existing', () => {
    const tree = new ScapegoatTree();
    tree.insert(5, 'old');
    tree.insert(5, 'new');
    assert.equal(tree.search(5), 'new');
  });

  it('benchmark: 10K inserts', () => {
    const tree = new ScapegoatTree();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    console.log(`    Scapegoat 10K: insert=${t1 - t0}ms, search=${Date.now() - t1}ms`);
    assert.equal(tree.size, 10000);
  });
});

describe('WeightBalancedTree', () => {
  it('insert and search', () => {
    const tree = new WeightBalancedTree();
    tree.insert(10, 'a');
    tree.insert(5, 'b');
    tree.insert(15, 'c');
    assert.equal(tree.search(10), 'a');
    assert.equal(tree.search(5), 'b');
    assert.equal(tree.search(99), undefined);
  });

  it('maintains sorted order', () => {
    const tree = new WeightBalancedTree();
    [50, 30, 70, 10, 40, 60, 80].forEach(k => tree.insert(k, k));
    const keys = [...tree.inOrder()].map(e => e.key);
    assert.deepEqual(keys, [10, 30, 40, 50, 60, 70, 80]);
  });

  it('sequential inserts', () => {
    const tree = new WeightBalancedTree();
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    assert.equal(tree.size, 1000);
    assert.equal(tree.search(500), 500);
  });

  it('benchmark: 10K inserts', () => {
    const tree = new WeightBalancedTree();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    console.log(`    WB tree 10K: insert=${t1 - t0}ms, search=${Date.now() - t1}ms`);
    assert.equal(tree.size, 10000);
  });
});
