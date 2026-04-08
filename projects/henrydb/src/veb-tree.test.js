// veb-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VEBTree } from './veb-tree.js';

describe('VEBTree (van Emde Boas)', () => {
  it('insert and has', () => {
    const tree = new VEBTree(16);
    tree.insert(3);
    tree.insert(7);
    tree.insert(12);
    assert.equal(tree.has(3), true);
    assert.equal(tree.has(7), true);
    assert.equal(tree.has(12), true);
    assert.equal(tree.has(5), false);
  });

  it('successor', () => {
    const tree = new VEBTree(16);
    tree.insert(2); tree.insert(5); tree.insert(10);
    assert.equal(tree.successor(2), 5);
    assert.equal(tree.successor(5), 10);
    assert.equal(tree.successor(10), null);
    assert.equal(tree.successor(0), 2);
  });

  it('predecessor', () => {
    const tree = new VEBTree(16);
    tree.insert(2); tree.insert(5); tree.insert(10);
    assert.equal(tree.predecessor(10), 5);
    assert.equal(tree.predecessor(5), 2);
    assert.equal(tree.predecessor(2), null);
    assert.equal(tree.predecessor(15), 10);
  });

  it('min and max', () => {
    const tree = new VEBTree(16);
    tree.insert(5); tree.insert(1); tree.insert(9);
    assert.equal(tree.min, 1);
    assert.equal(tree.max, 9);
  });

  it('larger universe', () => {
    const tree = new VEBTree(256);
    const values = [3, 17, 42, 100, 200, 255];
    for (const v of values) tree.insert(v);
    for (const v of values) assert.equal(tree.has(v), true);
    assert.equal(tree.has(50), false);
  });

  it('sequential successor walk', () => {
    const tree = new VEBTree(64);
    const vals = [5, 10, 15, 20, 25, 30];
    for (const v of vals) tree.insert(v);
    
    const walked = [];
    let cur = tree.min;
    while (cur !== null) {
      walked.push(cur);
      cur = tree.successor(cur);
    }
    assert.deepEqual(walked, vals);
  });

  it('benchmark: 1K inserts + 1K successor queries on u=65536', () => {
    const tree = new VEBTree(65536);
    const t0 = Date.now();
    for (let i = 0; i < 1000; i++) tree.insert(Math.floor(Math.random() * 65536));
    const t1 = Date.now();
    let successors = 0;
    for (let i = 0; i < 1000; i++) {
      if (tree.successor(i * 65) !== null) successors++;
    }
    console.log(`    VEB u=65536: insert=${t1 - t0}ms, successor=${Date.now() - t1}ms`);
    assert.ok(successors > 0);
  });
});
