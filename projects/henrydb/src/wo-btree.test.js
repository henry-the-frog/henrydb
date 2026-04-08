// wo-btree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WOBTree } from './wo-btree.js';

describe('WOBTree (Write-Optimized B-tree)', () => {
  it('insert and search', () => {
    const tree = new WOBTree(4, 4);
    tree.insert(10, 'a');
    tree.insert(20, 'b');
    tree.insert(30, 'c');
    assert.equal(tree.search(10), 'a');
    assert.equal(tree.search(20), 'b');
    assert.equal(tree.search(30), 'c');
  });

  it('update existing key', () => {
    const tree = new WOBTree(4, 4);
    tree.insert(10, 'old');
    tree.insert(10, 'new');
    assert.equal(tree.search(10), 'new');
  });

  it('search missing key', () => {
    const tree = new WOBTree(4, 4);
    tree.insert(10, 'a');
    assert.equal(tree.search(999), undefined);
  });

  it('buffer flush on overflow', () => {
    const tree = new WOBTree(4, 4);
    for (let i = 0; i < 20; i++) tree.insert(i, `v${i}`);
    // All values should be searchable after buffer flushes
    for (let i = 0; i < 20; i++) assert.equal(tree.search(i), `v${i}`);
    assert.ok(tree.stats.inserts === 20);
  });

  it('range query', () => {
    const tree = new WOBTree(4, 4);
    for (let i = 0; i < 50; i++) tree.insert(i, i * 10);
    const range = tree.range(10, 20);
    assert.equal(range.length, 11);
    assert.equal(range[0][0], 10);
    assert.equal(range[10][0], 20);
  });

  it('size after inserts', () => {
    const tree = new WOBTree(4, 4);
    for (let i = 0; i < 30; i++) tree.insert(i, i);
    assert.equal(tree.size, 30);
  });

  it('reverse order inserts', () => {
    const tree = new WOBTree(4, 4);
    for (let i = 99; i >= 0; i--) tree.insert(i, `v${i}`);
    for (let i = 0; i < 100; i++) assert.equal(tree.search(i), `v${i}`);
  });

  it('stats tracking', () => {
    const tree = new WOBTree(4, 4);
    for (let i = 0; i < 50; i++) tree.insert(i, i);
    tree.search(25);
    assert.equal(tree.stats.inserts, 50);
    assert.equal(tree.stats.searches, 1);
    assert.ok(tree.stats.flushes > 0);
  });

  it('benchmark: 10K inserts', () => {
    const tree = new WOBTree(16, 32);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const insertMs = Date.now() - t0;
    
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    const searchMs = Date.now() - t1;
    
    console.log(`    WO-B-tree 10K: insert=${insertMs}ms, search=${searchMs}ms, flushes=${tree.stats.flushes}`);
    assert.equal(tree.size, 10000);
  });
});
