// distributed-kv.test.js — Tests for the distributed KV store
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DistributedKV } from './distributed-kv.js';

describe('DistributedKV', () => {
  function createCluster(n = 3) {
    const kv = new DistributedKV({ replicationFactor: 2, virtualNodes: 50 });
    for (let i = 0; i < n; i++) kv.addPartition(`node-${i}`);
    return kv;
  }

  it('basic put and get', () => {
    const kv = createCluster();
    kv.put('name', 'Alice');
    assert.equal(kv.get('name'), 'Alice');
  });

  it('get returns null for missing key', () => {
    const kv = createCluster();
    assert.equal(kv.get('missing'), null);
  });

  it('delete removes key', () => {
    const kv = createCluster();
    kv.put('x', 42);
    assert.equal(kv.get('x'), 42);
    kv.delete('x');
    assert.equal(kv.get('x'), null);
  });

  it('data survives partition removal (replicated)', () => {
    const kv = new DistributedKV({ replicationFactor: 3, virtualNodes: 50 });
    kv.addPartition('A');
    kv.addPartition('B');
    kv.addPartition('C');
    
    kv.put('key1', 'value1');
    
    // Remove one partition
    kv.removePartition('A');
    
    // Data should still be available from remaining replicas
    const val = kv.get('key1');
    assert.equal(val, 'value1');
  });

  it('bloom filter accelerates negative lookups', () => {
    const kv = createCluster();
    kv.put('exists', 'yes');
    
    // Get a non-existent key — Bloom filter should quickly return null
    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      kv.get(`missing-${i}`);
    }
    const elapsed = performance.now() - start;
    // Should be fast since Bloom filter short-circuits
    assert.ok(elapsed < 2000, `Too slow: ${elapsed}ms for 1000 lookups`);
  });

  it('transactions: commit applies writes', () => {
    const kv = createCluster();
    kv.put('balance', 100);
    
    const result = kv.transaction(ctx => {
      const bal = ctx.get('balance');
      ctx.put('balance', bal - 30);
    });
    
    assert.ok(result.committed);
    assert.equal(kv.get('balance'), 70);
  });

  it('transactions: error causes rollback', () => {
    const kv = createCluster();
    kv.put('x', 42);
    
    const result = kv.transaction(ctx => {
      ctx.put('x', 999);
      throw new Error('abort!');
    });
    
    assert.ok(!result.committed);
    assert.equal(kv.get('x'), 42); // unchanged
  });

  it('cluster stats', () => {
    const kv = createCluster(5);
    for (let i = 0; i < 100; i++) kv.put(`key-${i}`, i);
    
    const stats = kv.getStats();
    assert.equal(stats.partitions, 5);
    assert.equal(stats.replicationFactor, 2);
    assert.equal(stats.partitionStats.length, 5);
    
    // Total keys across partitions should be > 100 (replicated)
    const totalKeys = stats.partitionStats.reduce((s, p) => s + p.keys, 0);
    assert.ok(totalKeys >= 100, `Expected >= 100 keys, got ${totalKeys}`);
  });

  it('many keys distributed across partitions', () => {
    const kv = createCluster(5);
    const N = 1000;
    
    for (let i = 0; i < N; i++) {
      kv.put(`key-${i}`, `value-${i}`);
    }
    
    // All keys should be retrievable
    let found = 0;
    for (let i = 0; i < N; i++) {
      if (kv.get(`key-${i}`) === `value-${i}`) found++;
    }
    assert.equal(found, N, `Only found ${found}/${N} keys`);
  });

  it('anti-entropy sync between partitions', () => {
    const kv = createCluster(3);
    
    // Manually insert different data into two partitions
    const pA = kv._partitions.get('node-0');
    const pB = kv._partitions.get('node-1');
    
    pA.set('only-on-a', 'valueA');
    pB.set('only-on-b', 'valueB');
    
    // Before sync: roots differ
    assert.notEqual(pA.getMerkleRoot(), pB.getMerkleRoot());
    
    // Sync
    const result = kv.antiEntropy('node-0', 'node-1');
    assert.ok(result.synced > 0);
    
    // After sync: both have both keys
    assert.equal(pA.get('only-on-b'), 'valueB');
    assert.equal(pB.get('only-on-a'), 'valueA');
    
    // Roots now match
    assert.equal(pA.getMerkleRoot(), pB.getMerkleRoot());
  });
});
