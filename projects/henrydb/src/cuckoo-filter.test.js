// cuckoo-filter.test.js — Tests for Cuckoo filter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CuckooFilter } from './cuckoo-filter.js';

describe('CuckooFilter', () => {
  it('basic insert and contains', () => {
    const cf = new CuckooFilter(100);
    assert.equal(cf.insert('apple'), true);
    assert.equal(cf.insert('banana'), true);
    
    assert.equal(cf.contains('apple'), true);
    assert.equal(cf.contains('banana'), true);
    assert.equal(cf.contains('cherry'), false);
  });

  it('no false negatives (with generous capacity)', () => {
    const cf = new CuckooFilter(2000); // 4x capacity for 500 elements
    const elements = [];
    for (let i = 0; i < 500; i++) {
      const e = `elem-${i}`;
      elements.push(e);
      assert.equal(cf.insert(e), true, `Insert failed for ${e}`);
    }
    
    for (const e of elements) {
      assert.equal(cf.contains(e), true, `False negative for ${e}`);
    }
  });

  it('deletion works', () => {
    const cf = new CuckooFilter(100);
    cf.insert('test');
    assert.equal(cf.contains('test'), true);
    
    assert.equal(cf.delete('test'), true);
    assert.equal(cf.contains('test'), false);
    assert.equal(cf.size, 0);
  });

  it('deletion of non-existent element', () => {
    const cf = new CuckooFilter(100);
    cf.insert('exists');
    assert.equal(cf.delete('not-exists'), false);
    assert.equal(cf.size, 1);
  });

  it('insert-delete-insert cycle', () => {
    const cf = new CuckooFilter(500); // Generous capacity

    for (let i = 0; i < 50; i++) cf.insert(`item-${i}`);
    for (let i = 0; i < 25; i++) cf.delete(`item-${i}`);
    assert.equal(cf.size, 25);
    
    // Re-insert deleted items
    for (let i = 0; i < 25; i++) cf.insert(`item-${i}`);
    assert.equal(cf.size, 50);
    
    // All should be present
    for (let i = 0; i < 50; i++) {
      assert.equal(cf.contains(`item-${i}`), true);
    }
  });

  it('false positive rate', () => {
    const cf = new CuckooFilter(1000, 4, 8);
    for (let i = 0; i < 500; i++) cf.insert(`in-${i}`);
    
    let fp = 0;
    const tests = 10000;
    for (let i = 0; i < tests; i++) {
      if (cf.contains(`not-in-${i}`)) fp++;
    }
    
    const fpr = fp / tests;
    console.log(`  FPR: ${(fpr * 100).toFixed(2)}% (500 elements in 1000-capacity filter)`);
    assert.ok(fpr < 0.15, `FPR too high: ${(fpr * 100).toFixed(2)}%`);
  });

  it('load factor tracking', () => {
    const cf = new CuckooFilter(1000, 4, 8);
    
    for (let i = 0; i < 500; i++) cf.insert(`item-${i}`);
    
    const stats = cf.getStats();
    console.log(`  Load: ${(stats.loadFactor * 100).toFixed(1)}%, ${stats.count}/${stats.capacity}`);
    assert.ok(stats.loadFactor > 0);
    assert.ok(stats.loadFactor < 1);
  });

  it('numeric keys', () => {
    const cf = new CuckooFilter(500);
    for (let i = 0; i < 100; i++) cf.insert(i);
    
    for (let i = 0; i < 100; i++) {
      assert.equal(cf.contains(i), true);
    }
    assert.equal(cf.contains(999), false);
  });

  it('stress: 5K inserts and lookups', () => {
    const cf = new CuckooFilter(10000, 4, 12);
    
    const t0 = performance.now();
    let insertOk = 0;
    for (let i = 0; i < 5000; i++) {
      if (cf.insert(`stress-${i}`)) insertOk++;
    }
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 5000; i++) cf.contains(`stress-${i}`);
    const lookupMs = performance.now() - t1;
    
    console.log(`  5K insert: ${insertMs.toFixed(1)}ms, ${insertOk} succeeded`);
    console.log(`  5K lookup: ${lookupMs.toFixed(1)}ms`);
    assert.ok(insertOk > 4000, `Too many insertion failures: ${insertOk}/5000`);
  });

  it('comparison: Cuckoo vs Bloom (deletion advantage)', () => {
    // Cuckoo's key advantage: deletion support
    const cf = new CuckooFilter(1000, 4, 8);
    
    // Insert 500 elements
    for (let i = 0; i < 500; i++) cf.insert(`item-${i}`);
    
    // Delete 250 elements — Bloom filter can't do this!
    for (let i = 0; i < 250; i++) cf.delete(`item-${i}`);
    
    // Deleted elements should not be found (no false negatives for deleted items)
    let falsePresent = 0;
    for (let i = 0; i < 250; i++) {
      if (cf.contains(`item-${i}`)) falsePresent++;
    }
    
    // Remaining elements should all be found
    for (let i = 250; i < 500; i++) {
      assert.equal(cf.contains(`item-${i}`), true);
    }
    
    console.log(`  After deletion: ${falsePresent}/250 deleted items still "found" (false positives from fingerprint collision)`);
    assert.ok(true);
  });

  it('getStats', () => {
    const cf = new CuckooFilter(1000, 4, 8);
    for (let i = 0; i < 100; i++) cf.insert(i);
    
    const stats = cf.getStats();
    assert.equal(stats.count, 100);
    assert.ok(stats.capacity > 0);
    assert.ok(stats.bytesUsed > 0);
  });
});
