// skip-list.test.js — Tests for skip list
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SkipList } from './skip-list.js';

describe('SkipList', () => {
  it('insert and get', () => {
    const sl = new SkipList();
    sl.insert(3, 'three');
    sl.insert(1, 'one');
    sl.insert(2, 'two');
    
    assert.equal(sl.get(1), 'one');
    assert.equal(sl.get(2), 'two');
    assert.equal(sl.get(3), 'three');
    assert.equal(sl.get(4), undefined);
    assert.equal(sl.size, 3);
  });

  it('update existing key', () => {
    const sl = new SkipList();
    sl.insert(1, 'v1');
    sl.insert(1, 'v2');
    assert.equal(sl.get(1), 'v2');
    assert.equal(sl.size, 1);
  });

  it('delete', () => {
    const sl = new SkipList();
    sl.insert(1, 'a');
    sl.insert(2, 'b');
    sl.insert(3, 'c');
    
    assert.equal(sl.delete(2), true);
    assert.equal(sl.get(2), undefined);
    assert.equal(sl.size, 2);
    assert.equal(sl.delete(99), false);
  });

  it('iteration in sorted order', () => {
    const sl = new SkipList();
    sl.insert(5, 'e');
    sl.insert(3, 'c');
    sl.insert(1, 'a');
    sl.insert(4, 'd');
    sl.insert(2, 'b');
    
    const keys = [...sl].map(e => e.key);
    assert.deepEqual(keys, [1, 2, 3, 4, 5]);
  });

  it('range query', () => {
    const sl = new SkipList();
    for (let i = 1; i <= 100; i++) sl.insert(i, `val-${i}`);
    
    const range = [...sl.range(30, 40)];
    assert.equal(range.length, 11);
    assert.equal(range[0].key, 30);
    assert.equal(range[10].key, 40);
  });

  it('min and max', () => {
    const sl = new SkipList();
    sl.insert(50, 'mid');
    sl.insert(10, 'low');
    sl.insert(90, 'high');
    
    assert.equal(sl.min().key, 10);
    assert.equal(sl.max().key, 90);
  });

  it('empty skip list', () => {
    const sl = new SkipList();
    assert.equal(sl.size, 0);
    assert.equal(sl.get(1), undefined);
    assert.equal(sl.min(), null);
    assert.equal(sl.max(), null);
    assert.deepEqual([...sl], []);
  });

  it('string keys with custom comparator', () => {
    const sl = new SkipList((a, b) => a.localeCompare(b));
    sl.insert('banana', 2);
    sl.insert('apple', 1);
    sl.insert('cherry', 3);
    
    const keys = [...sl].map(e => e.key);
    assert.deepEqual(keys, ['apple', 'banana', 'cherry']);
  });

  it('large scale: 10K elements', () => {
    const sl = new SkipList();
    
    // Insert in random order
    const nums = Array.from({ length: 10000 }, (_, i) => i);
    for (let i = nums.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [nums[i], nums[j]] = [nums[j], nums[i]];
    }
    
    for (const n of nums) sl.insert(n, n);
    
    assert.equal(sl.size, 10000);
    
    // Verify sorted order
    const keys = [...sl].map(e => e.key);
    for (let i = 1; i < keys.length; i++) {
      assert.ok(keys[i] > keys[i-1]);
    }
    
    // Spot check lookups
    assert.equal(sl.get(0), 0);
    assert.equal(sl.get(5000), 5000);
    assert.equal(sl.get(9999), 9999);
    
    const stats = sl.getStats();
    console.log(`  10K elements: height=${stats.height}, levels=${JSON.stringify(stats.levelCounts.slice(0, 5))}`);
  });

  it('performance: 10K insert + 10K lookup', () => {
    const sl = new SkipList();
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) sl.insert(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) sl.get(i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms (${(insertMs/10000*1000).toFixed(3)}µs avg)`);
    console.log(`  10K lookup: ${lookupMs.toFixed(1)}ms (${(lookupMs/10000*1000).toFixed(3)}µs avg)`);
    
    assert.ok(insertMs < 500);
    assert.ok(lookupMs < 500);
  });

  it('skip list vs B+tree: comparison', async () => {
    const { BPlusTree } = await import('./bplus-tree.js');
    
    const sl = new SkipList();
    const bt = new BPlusTree(64);
    
    // Insert 10K elements
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) sl.insert(i, i);
    const slInsert = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) bt.insert(i, i);
    const btInsert = performance.now() - t1;
    
    // Lookup 10K elements
    const t2 = performance.now();
    for (let i = 0; i < 10000; i++) sl.get(i);
    const slLookup = performance.now() - t2;
    
    const t3 = performance.now();
    for (let i = 0; i < 10000; i++) bt.get(i);
    const btLookup = performance.now() - t3;
    
    console.log(`  Insert 10K: SkipList ${slInsert.toFixed(1)}ms | B+tree ${btInsert.toFixed(1)}ms`);
    console.log(`  Lookup 10K: SkipList ${slLookup.toFixed(1)}ms | B+tree ${btLookup.toFixed(1)}ms`);
    assert.ok(true);
  });

  it('has method', () => {
    const sl = new SkipList();
    sl.insert(1, 'a');
    assert.equal(sl.has(1), true);
    assert.equal(sl.has(2), false);
  });

  it('insert-delete-insert cycle', () => {
    const sl = new SkipList();
    for (let i = 0; i < 100; i++) sl.insert(i, i);
    for (let i = 0; i < 50; i++) sl.delete(i);
    assert.equal(sl.size, 50);
    
    // Re-insert
    for (let i = 0; i < 50; i++) sl.insert(i, i + 100);
    assert.equal(sl.size, 100);
    assert.equal(sl.get(0), 100);
    assert.equal(sl.get(50), 50);
  });
});
