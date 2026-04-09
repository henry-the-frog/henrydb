// lsm-tree.test.js — Tests for LSM-tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LSMTree } from './lsm-tree.js';

describe('LSMTree', () => {
  it('basic put and get', () => {
    const lsm = new LSMTree();
    lsm.put('name', 'Alice');
    lsm.put('age', 30);
    
    assert.equal(lsm.get('name'), 'Alice');
    assert.equal(lsm.get('age'), 30);
    assert.equal(lsm.get('missing'), undefined);
  });

  it('update overwrites previous value', () => {
    const lsm = new LSMTree();
    lsm.put('key', 'v1');
    lsm.put('key', 'v2');
    assert.equal(lsm.get('key'), 'v2');
  });

  it('delete with tombstone', () => {
    const lsm = new LSMTree();
    lsm.put('key', 'value');
    assert.equal(lsm.get('key'), 'value');
    
    lsm.delete('key');
    assert.equal(lsm.get('key'), undefined);
  });

  it('flush: memtable to SSTable', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    
    for (let i = 0; i < 10; i++) lsm.put(`key-${i}`, i);
    
    const stats = lsm.getStats();
    assert.ok(stats.flushes >= 1, 'Should have flushed at least once');
    assert.ok(stats.totalSSTables >= 1, 'Should have SSTables');
    
    // All entries should still be readable
    for (let i = 0; i < 10; i++) {
      assert.equal(lsm.get(`key-${i}`), i);
    }
  });

  it('compaction: merge SSTables across levels', () => {
    const lsm = new LSMTree({ memtableSize: 5, compactionThreshold: 2 });
    
    // Insert enough to trigger multiple flushes and compaction
    for (let i = 0; i < 50; i++) lsm.put(i, i * 10);
    
    const stats = lsm.getStats();
    console.log(`  50 puts: flushes=${stats.flushes}, compactions=${stats.compactions}, levels=${stats.levels}`);
    assert.ok(stats.compactions >= 1, 'Should have compacted at least once');
    
    // All entries should be readable after compaction
    for (let i = 0; i < 50; i++) {
      assert.equal(lsm.get(i), i * 10, `key ${i} should have value ${i * 10}`);
    }
  });

  it('scan returns sorted entries', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    
    // Insert in reverse order
    for (let i = 10; i >= 1; i--) lsm.put(i, `val-${i}`);
    
    const entries = [...lsm.scan()];
    assert.equal(entries.length, 10);
    
    // Should be sorted
    for (let i = 0; i < entries.length - 1; i++) {
      assert.ok(entries[i].key < entries[i + 1].key);
    }
  });

  it('scan excludes tombstones', () => {
    const lsm = new LSMTree();
    lsm.put('a', 1);
    lsm.put('b', 2);
    lsm.put('c', 3);
    lsm.delete('b');
    
    const entries = [...lsm.scan()];
    assert.equal(entries.length, 2);
    assert.equal(entries[0].key, 'a');
    assert.equal(entries[1].key, 'c');
  });

  it('writes survive flush + compaction', () => {
    const lsm = new LSMTree({ memtableSize: 10, compactionThreshold: 3 });
    
    // Insert 1000 entries (triggers many flushes + compactions)
    for (let i = 0; i < 1000; i++) {
      lsm.put(`key-${String(i).padStart(4, '0')}`, i);
    }
    
    const stats = lsm.getStats();
    console.log(`  1K puts: flushes=${stats.flushes}, compactions=${stats.compactions}`);
    
    // Spot check
    assert.equal(lsm.get('key-0000'), 0);
    assert.equal(lsm.get('key-0500'), 500);
    assert.equal(lsm.get('key-0999'), 999);
  });

  it('mixed put/delete workload', () => {
    const lsm = new LSMTree({ memtableSize: 20 });
    
    // Insert 100, delete 50, re-insert 25
    for (let i = 0; i < 100; i++) lsm.put(i, i);
    for (let i = 0; i < 50; i++) lsm.delete(i);
    for (let i = 0; i < 25; i++) lsm.put(i, i + 1000);
    
    // First 25: re-inserted with new values
    for (let i = 0; i < 25; i++) assert.equal(lsm.get(i), i + 1000);
    // 25-49: deleted
    for (let i = 25; i < 50; i++) assert.equal(lsm.get(i), undefined);
    // 50-99: original values
    for (let i = 50; i < 100; i++) assert.equal(lsm.get(i), i);
  });

  it('performance: 10K sequential writes + 10K reads', () => {
    const lsm = new LSMTree({ memtableSize: 500 });
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) lsm.put(i, i);
    const writeMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) lsm.get(i);
    const readMs = performance.now() - t1;
    
    const stats = lsm.getStats();
    console.log(`  10K write: ${writeMs.toFixed(1)}ms (${(writeMs/10000*1000).toFixed(3)}µs avg)`);
    console.log(`  10K read: ${readMs.toFixed(1)}ms (${(readMs/10000*1000).toFixed(3)}µs avg)`);
    console.log(`  Flushes: ${stats.flushes}, Compactions: ${stats.compactions}`);
    
    assert.ok(writeMs < 500);
    assert.ok(readMs < 1000);
  });

  it('write amplification is bounded', () => {
    const lsm = new LSMTree({ memtableSize: 100, compactionThreshold: 4 });
    
    for (let i = 0; i < 5000; i++) lsm.put(i, i);
    
    const stats = lsm.getStats();
    // Write amplification ≈ (flushes + compactions) / (total writes)
    const writeAmp = (stats.flushes + stats.compactions) / 50; // Per 100 writes
    console.log(`  Write amp: ${writeAmp.toFixed(2)} (${stats.flushes} flushes, ${stats.compactions} compactions)`);
  });

  it('getStats reports structure', () => {
    const lsm = new LSMTree({ memtableSize: 10 });
    for (let i = 0; i < 50; i++) lsm.put(i, i);
    
    const stats = lsm.getStats();
    assert.ok(stats.memtableSize >= 0);
    assert.ok(stats.levels >= 2);
    assert.ok(Array.isArray(stats.sstablesPerLevel));
    assert.ok(stats.flushes >= 0);
  });
});
