// extendible-hash.test.js — Tests for extendible hash table
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExtendibleHashTable } from './extendible-hash.js';

describe('ExtendibleHashTable', () => {
  it('insert and get', () => {
    const ht = new ExtendibleHashTable();
    ht.insert('alice', 1);
    ht.insert('bob', 2);
    ht.insert('charlie', 3);
    
    assert.equal(ht.get('alice'), 1);
    assert.equal(ht.get('bob'), 2);
    assert.equal(ht.get('charlie'), 3);
    assert.equal(ht.get('dave'), undefined);
    assert.equal(ht.size, 3);
  });

  it('update existing key', () => {
    const ht = new ExtendibleHashTable();
    ht.insert('key', 'v1');
    ht.insert('key', 'v2');
    
    assert.equal(ht.get('key'), 'v2');
    assert.equal(ht.size, 1);
  });

  it('remove', () => {
    const ht = new ExtendibleHashTable();
    ht.insert('a', 1);
    ht.insert('b', 2);
    
    assert.equal(ht.remove('a'), true);
    assert.equal(ht.get('a'), undefined);
    assert.equal(ht.size, 1);
    assert.equal(ht.remove('a'), false);
  });

  it('triggers bucket split', () => {
    const ht = new ExtendibleHashTable(4, 1); // Small buckets, depth 1
    
    // Insert more than bucket size to trigger splits
    for (let i = 0; i < 20; i++) {
      ht.insert(`key-${i}`, i);
    }
    
    assert.equal(ht.size, 20);
    // Verify all entries
    for (let i = 0; i < 20; i++) {
      assert.equal(ht.get(`key-${i}`), i, `key-${i} should map to ${i}`);
    }
    
    const stats = ht.getStats();
    assert.ok(stats.splits > 0, 'Should have triggered splits');
    assert.ok(stats.globalDepth > 1, 'Global depth should have grown');
  });

  it('triggers directory growth', () => {
    const ht = new ExtendibleHashTable(2, 1); // Very small buckets
    
    for (let i = 0; i < 50; i++) {
      ht.insert(i, `val-${i}`);
    }
    
    const stats = ht.getStats();
    assert.ok(stats.directoryGrowths > 0, 'Directory should have grown');
    assert.ok(stats.directorySlots > 2, 'Directory should have more than 2 slots');
    
    // All entries should be retrievable
    for (let i = 0; i < 50; i++) {
      assert.equal(ht.get(i), `val-${i}`);
    }
  });

  it('entries iterator', () => {
    const ht = new ExtendibleHashTable();
    for (let i = 0; i < 10; i++) ht.insert(i, i * 10);
    
    const all = [...ht.entries()];
    assert.equal(all.length, 10);
    
    const keys = all.map(e => e.key).sort((a, b) => a - b);
    assert.deepEqual(keys, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('numeric keys', () => {
    const ht = new ExtendibleHashTable();
    for (let i = 0; i < 100; i++) {
      ht.insert(i, i * 2);
    }
    
    for (let i = 0; i < 100; i++) {
      assert.equal(ht.get(i), i * 2);
    }
  });

  it('string keys with collision potential', () => {
    const ht = new ExtendibleHashTable(4);
    const keys = [];
    for (let i = 0; i < 1000; i++) {
      const key = `item-${i}-${Math.random().toString(36).slice(2, 8)}`;
      keys.push(key);
      ht.insert(key, i);
    }
    
    assert.equal(ht.size, 1000);
    for (let i = 0; i < 1000; i++) {
      assert.equal(ht.get(keys[i]), i);
    }
  });

  it('large insert: 10K entries', () => {
    const ht = new ExtendibleHashTable(16);
    
    for (let i = 0; i < 10000; i++) {
      ht.insert(i, i);
    }
    
    assert.equal(ht.size, 10000);
    
    const stats = ht.getStats();
    console.log(`  10K entries: depth=${stats.globalDepth}, slots=${stats.directorySlots}, buckets=${stats.uniqueBuckets}, load=${stats.loadFactor.toFixed(2)}`);
    
    // Spot check
    for (const i of [0, 1000, 5000, 9999]) {
      assert.equal(ht.get(i), i);
    }
  });

  it('insert + delete + re-insert', () => {
    const ht = new ExtendibleHashTable();
    
    for (let i = 0; i < 50; i++) ht.insert(i, i);
    for (let i = 0; i < 25; i++) ht.remove(i);
    assert.equal(ht.size, 25);
    
    // Re-insert removed keys
    for (let i = 0; i < 25; i++) ht.insert(i, i + 100);
    assert.equal(ht.size, 50);
    
    // Verify
    for (let i = 0; i < 25; i++) {
      assert.equal(ht.get(i), i + 100); // Updated values
    }
    for (let i = 25; i < 50; i++) {
      assert.equal(ht.get(i), i); // Original values
    }
  });

  it('getStats shows meaningful metrics', () => {
    const ht = new ExtendibleHashTable(8);
    for (let i = 0; i < 100; i++) ht.insert(i, i);
    
    const stats = ht.getStats();
    assert.equal(stats.size, 100);
    assert.ok(stats.globalDepth >= 1);
    assert.ok(stats.loadFactor > 0);
    assert.ok(stats.avgBucketFill > 0);
    assert.ok(stats.maxBucketFill <= 8);
  });

  it('performance: 10K insert + 10K lookup', () => {
    const ht = new ExtendibleHashTable(16);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) ht.insert(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) ht.get(i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms (${(insertMs/10000*1000).toFixed(3)}µs avg)`);
    console.log(`  10K lookup: ${lookupMs.toFixed(1)}ms (${(lookupMs/10000*1000).toFixed(3)}µs avg)`);
    assert.ok(insertMs < 1000);
    assert.ok(lookupMs < 500);
  });

  it('hash distribution quality', () => {
    const ht = new ExtendibleHashTable(16);
    for (let i = 0; i < 1000; i++) ht.insert(i, i);
    
    const stats = ht.getStats();
    // Good distribution: load factor near 0.5-0.8, no single bucket dominating
    assert.ok(stats.loadFactor > 0.1, `Load factor too low: ${stats.loadFactor}`);
    assert.ok(stats.maxBucketFill <= 16, 'No bucket should exceed capacity');
    
    console.log(`  Distribution: load=${stats.loadFactor.toFixed(2)}, avg fill=${stats.avgBucketFill.toFixed(1)}, max fill=${stats.maxBucketFill}`);
  });
});
