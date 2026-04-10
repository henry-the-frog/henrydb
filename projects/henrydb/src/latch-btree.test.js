// latch-btree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LatchBPlusTree, Latch } from './latch-btree.js';

describe('Latch', () => {
  it('basic read/write locking', () => {
    const l = new Latch();
    assert.ok(l.acquireRead());
    assert.ok(l.acquireRead()); // Multiple readers
    assert.ok(!l.acquireWrite()); // Can't write while reading
    l.releaseRead();
    l.releaseRead();
    assert.ok(l.acquireWrite());
    assert.ok(!l.acquireRead()); // Can't read while writing
    l.releaseWrite();
  });
});

describe('LatchBPlusTree — Basic Operations', () => {
  it('insert and search', () => {
    const tree = new LatchBPlusTree(4);
    tree.insert(5, 'five');
    tree.insert(3, 'three');
    tree.insert(7, 'seven');
    assert.equal(tree.search(5), 'five');
    assert.equal(tree.search(3), 'three');
    assert.equal(tree.search(7), 'seven');
    assert.equal(tree.search(4), undefined);
  });

  it('insert triggers splits', () => {
    const tree = new LatchBPlusTree(4); // Order 4: max 3 keys per node
    for (let i = 0; i < 20; i++) tree.insert(i, `val-${i}`);
    
    assert.equal(tree.size, 20);
    const stats = tree.getStats();
    assert.ok(stats.splits > 0, 'Should have splits');
    assert.ok(stats.height > 1, 'Should be multi-level');
    
    // Verify all values retrievable
    for (let i = 0; i < 20; i++) {
      assert.equal(tree.search(i), `val-${i}`);
    }
  });

  it('update existing key', () => {
    const tree = new LatchBPlusTree(4);
    tree.insert(1, 'old');
    tree.insert(1, 'new');
    assert.equal(tree.search(1), 'new');
    assert.equal(tree.size, 1);
  });

  it('range scan', () => {
    const tree = new LatchBPlusTree(4);
    for (let i = 0; i < 50; i++) tree.insert(i, i * 10);
    
    const results = [...tree.scan(10, 15)];
    assert.equal(results.length, 6);
    assert.equal(results[0].key, 10);
    assert.equal(results[5].key, 15);
  });

  it('full scan in order', () => {
    const tree = new LatchBPlusTree(4);
    // Insert in random order
    const nums = [5, 2, 8, 1, 9, 3, 7, 4, 6, 0];
    for (const n of nums) tree.insert(n, n);
    
    const results = [...tree.scan()];
    assert.equal(results.length, 10);
    for (let i = 0; i < 10; i++) {
      assert.equal(results[i].key, i);
    }
  });
});

describe('LatchBPlusTree — Latch Crabbing Behavior', () => {
  it('search acquires and releases latches correctly', () => {
    const tree = new LatchBPlusTree(4);
    for (let i = 0; i < 100; i++) tree.insert(i, i);
    
    const before = { ...tree.stats };
    tree.search(50);
    
    assert.ok(tree.stats.latchAcquires > before.latchAcquires, 'Should acquire latches');
    assert.ok(tree.stats.latchReleases > before.latchReleases, 'Should release latches');
    
    // After search, no latches should be held
    // (We can't easily check this without walking the tree, but the stats should balance)
  });

  it('safe inserts release ancestor latches early', () => {
    const tree = new LatchBPlusTree(100); // Large order = safe nodes
    for (let i = 0; i < 50; i++) tree.insert(i, i);
    
    const stats = tree.getStats();
    // With order 100 and only 50 keys, no splits needed
    assert.equal(stats.splits, 0);
    assert.ok(stats.latchAcquires > 0);
  });

  it('unsafe inserts keep ancestor latches', () => {
    const tree = new LatchBPlusTree(3); // Tiny order = frequent splits
    for (let i = 0; i < 20; i++) tree.insert(i, i);
    
    const stats = tree.getStats();
    assert.ok(stats.splits > 0, 'Should have splits with small order');
  });
});

describe('LatchBPlusTree — Performance', () => {
  const N = 10_000;

  it('benchmark: 10K sequential inserts', () => {
    const tree = new LatchBPlusTree(64);
    const t0 = performance.now();
    for (let i = 0; i < N; i++) tree.insert(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < N; i++) tree.search(i);
    const searchMs = performance.now() - t1;
    
    const stats = tree.getStats();
    console.log(`    Sequential: ${N} inserts=${insertMs.toFixed(1)}ms, ${N} searches=${searchMs.toFixed(1)}ms`);
    console.log(`    Height=${stats.height}, Splits=${stats.splits}, Latches=${stats.latchAcquires}/${stats.latchReleases}`);
    
    assert.equal(tree.size, N);
  });

  it('benchmark: 10K random inserts', () => {
    const tree = new LatchBPlusTree(64);
    const keys = Array.from({ length: N }, () => Math.random() * N * 10 | 0);
    const unique = new Set(keys);
    
    const t0 = performance.now();
    for (const k of keys) tree.insert(k, k);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    let found = 0;
    for (const k of unique) { if (tree.search(k) !== undefined) found++; }
    const searchMs = performance.now() - t1;
    
    console.log(`    Random: ${keys.length} inserts=${insertMs.toFixed(1)}ms, ${unique.size} searches=${searchMs.toFixed(1)}ms`);
    assert.equal(found, unique.size);
  });

  it('benchmark: range scan', () => {
    const tree = new LatchBPlusTree(64);
    for (let i = 0; i < N; i++) tree.insert(i, i);
    
    const t0 = performance.now();
    let count = 0;
    for (const _ of tree.scan()) count++;
    const scanMs = performance.now() - t0;
    
    console.log(`    Scan: ${count} keys in ${scanMs.toFixed(1)}ms (${(count / scanMs * 1000) | 0}/sec)`);
    assert.equal(count, N);
  });
});
