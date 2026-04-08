// compact-filter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GolombCodedSet, LLRBTree, AATree } from './compact-filter.js';

describe('GolombCodedSet', () => {
  it('basic membership', () => {
    const keys = ['apple', 'banana', 'cherry', 'date', 'elderberry'];
    const gcs = GolombCodedSet.build(keys);
    for (const k of keys) assert.equal(gcs.has(k), true);
  });

  it('rejects non-members (mostly)', () => {
    const keys = Array.from({ length: 100 }, (_, i) => `key_${i}`);
    const gcs = GolombCodedSet.build(keys);
    let fp = 0;
    for (let i = 200; i < 300; i++) if (gcs.has(`key_${i}`)) fp++;
    console.log(`    GCS false positives: ${fp}/100`);
    assert.ok(fp < 20); // FP rate should be low
  });

  it('empty set', () => {
    const gcs = GolombCodedSet.build([]);
    assert.equal(gcs.has('anything'), false);
  });

  it('size estimation', () => {
    const keys = Array.from({ length: 1000 }, (_, i) => `k${i}`);
    const gcs = GolombCodedSet.build(keys);
    console.log(`    GCS 1K keys: ${gcs.sizeBytes} bytes, ${gcs.bitsPerEntry.toFixed(1)} bits/entry`);
    assert.ok(gcs.sizeBytes > 0);
    assert.ok(gcs.bitsPerEntry < 25); // Should be efficient
  });
});

describe('LLRBTree (Left-Leaning Red-Black)', () => {
  it('insert and search', () => {
    const tree = new LLRBTree();
    tree.insert(5, 'five');
    tree.insert(3, 'three');
    tree.insert(7, 'seven');
    assert.equal(tree.search(5), 'five');
    assert.equal(tree.search(3), 'three');
    assert.equal(tree.search(99), undefined);
  });

  it('update existing', () => {
    const tree = new LLRBTree();
    tree.insert(5, 'old');
    tree.insert(5, 'new');
    assert.equal(tree.search(5), 'new');
    assert.equal(tree.size, 1);
  });

  it('maintains balance', () => {
    const tree = new LLRBTree();
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    assert.equal(tree.size, 1000);
    assert.ok(tree.height <= 20); // log2(1000) ≈ 10, so 2×log should hold
  });

  it('in-order traversal', () => {
    const tree = new LLRBTree();
    [5, 3, 7, 1, 4].forEach(k => tree.insert(k, k));
    const keys = [...tree.inOrder()].map(e => e.key);
    assert.deepEqual(keys, [1, 3, 4, 5, 7]);
  });

  it('benchmark: 10K inserts', () => {
    const tree = new LLRBTree();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    console.log(`    LLRB 10K: insert=${t1 - t0}ms, search=${Date.now() - t1}ms, height=${tree.height}`);
    assert.equal(tree.size, 10000);
  });
});

describe('AATree', () => {
  it('insert and search', () => {
    const tree = new AATree();
    tree.insert(10, 'a');
    tree.insert(5, 'b');
    tree.insert(15, 'c');
    assert.equal(tree.search(10), 'a');
    assert.equal(tree.search(5), 'b');
    assert.equal(tree.search(99), undefined);
  });

  it('maintains sorted order', () => {
    const tree = new AATree();
    [50, 30, 70, 10, 40, 60, 80].forEach(k => tree.insert(k, k));
    const keys = [...tree.inOrder()].map(e => e.key);
    assert.deepEqual(keys, [10, 30, 40, 50, 60, 70, 80]);
  });

  it('handles sequential inserts', () => {
    const tree = new AATree();
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    assert.equal(tree.size, 1000);
    assert.equal(tree.search(500), 500);
  });

  it('benchmark: 10K inserts', () => {
    const tree = new AATree();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    console.log(`    AA tree 10K: insert=${t1 - t0}ms, search=${Date.now() - t1}ms`);
    assert.equal(tree.size, 10000);
  });
});
