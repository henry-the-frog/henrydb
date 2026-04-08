// bplus-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BPlusTree } from './bplus-tree.js';

describe('BPlusTree', () => {
  it('basic insert and get', () => {
    const tree = new BPlusTree(4);
    tree.insert(10, 'ten');
    tree.insert(20, 'twenty');
    tree.insert(5, 'five');
    assert.equal(tree.get(10), 'ten');
    assert.equal(tree.get(20), 'twenty');
    assert.equal(tree.get(5), 'five');
    assert.equal(tree.get(15), undefined);
  });

  it('update existing key', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'old');
    tree.insert(1, 'new');
    assert.equal(tree.get(1), 'new');
    assert.equal(tree.size, 1);
  });

  it('delete', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'a');
    tree.insert(2, 'b');
    assert.ok(tree.delete(1));
    assert.equal(tree.get(1), undefined);
    assert.equal(tree.size, 1);
    assert.ok(!tree.delete(999));
  });

  it('splits correctly on many inserts', () => {
    const tree = new BPlusTree(4);
    for (let i = 0; i < 100; i++) tree.insert(i, i * 10);
    
    assert.equal(tree.size, 100);
    for (let i = 0; i < 100; i++) assert.equal(tree.get(i), i * 10);
  });

  it('range scan via leaf chain', () => {
    const tree = new BPlusTree(4);
    for (let i = 0; i < 50; i++) tree.insert(i, i);
    
    const range = tree.range(10, 20);
    assert.equal(range.length, 11); // 10..20 inclusive
    assert.equal(range[0].key, 10);
    assert.equal(range[10].key, 20);
  });

  it('sorted iteration', () => {
    const tree = new BPlusTree(4);
    tree.insert(30, 'c');
    tree.insert(10, 'a');
    tree.insert(20, 'b');
    assert.deepEqual([...tree].map(e => e.key), [10, 20, 30]);
  });

  it('min and max', () => {
    const tree = new BPlusTree(4);
    tree.insert(50, 'c');
    tree.insert(10, 'a');
    tree.insert(90, 'e');
    assert.equal(tree.min().key, 10);
    assert.equal(tree.max().key, 90);
  });

  it('string keys', () => {
    const tree = new BPlusTree(4);
    tree.insert('banana', 2);
    tree.insert('apple', 1);
    tree.insert('cherry', 3);
    assert.equal(tree.get('apple'), 1);
    assert.deepEqual([...tree].map(e => e.key), ['apple', 'banana', 'cherry']);
  });

  it('1000 inserts + lookups', () => {
    const tree = new BPlusTree(64); // Higher order = shallower tree
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    assert.equal(tree.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(tree.get(i), i);
    assert.ok(tree.height <= 4);
  });

  it('reverse insert order', () => {
    const tree = new BPlusTree(4);
    for (let i = 99; i >= 0; i--) tree.insert(i, i);
    assert.equal(tree.size, 100);
    assert.deepEqual([...tree].map(e => e.key).slice(0, 5), [0, 1, 2, 3, 4]);
  });

  it('benchmark: 50K inserts + lookups', () => {
    const n = 50000;
    const tree = new BPlusTree(128);
    
    const t0 = Date.now();
    for (let i = 0; i < n; i++) tree.insert(i, i);
    const buildMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < n; i++) tree.get(i);
    const lookupMs = Date.now() - t1;

    const t2 = Date.now();
    const range = tree.range(10000, 20000);
    const rangeMs = Date.now() - t2;

    console.log(`    Build: ${buildMs}ms, Lookup: ${lookupMs}ms, Range(10K): ${rangeMs}ms, Height: ${tree.height}`);
    assert.equal(range.length, 10001);
  });
});
