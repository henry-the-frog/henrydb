// partitioned-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PartitionedHashTable } from './partitioned-hash.js';

describe('PartitionedHashTable', () => {
  it('set and get', () => {
    const ht = new PartitionedHashTable(4);
    ht.set('a', 1);
    ht.set('b', 2);
    assert.equal(ht.get('a'), 1);
    assert.equal(ht.get('b'), 2);
  });

  it('update existing key', () => {
    const ht = new PartitionedHashTable(4);
    ht.set('a', 1);
    ht.set('a', 2);
    assert.equal(ht.get('a'), 2);
    assert.equal(ht.size, 1);
  });

  it('get missing key', () => {
    const ht = new PartitionedHashTable(4);
    assert.equal(ht.get('missing'), undefined);
  });

  it('delete', () => {
    const ht = new PartitionedHashTable(4);
    ht.set('a', 1);
    assert.equal(ht.delete('a'), true);
    assert.equal(ht.get('a'), undefined);
    assert.equal(ht.size, 0);
  });

  it('delete missing returns false', () => {
    const ht = new PartitionedHashTable(4);
    assert.equal(ht.delete('missing'), false);
  });

  it('auto-resize on high load', () => {
    const ht = new PartitionedHashTable(4, 4);
    for (let i = 0; i < 100; i++) ht.set(`k${i}`, i);
    for (let i = 0; i < 100; i++) assert.equal(ht.get(`k${i}`), i);
    assert.equal(ht.size, 100);
  });

  it('stats per partition', () => {
    const ht = new PartitionedHashTable(4);
    for (let i = 0; i < 100; i++) ht.set(`k${i}`, i);
    const stats = ht.stats();
    assert.equal(stats.length, 4);
    assert.ok(stats.every(s => s.size > 0));
  });

  it('benchmark: 50K entries', () => {
    const ht = new PartitionedHashTable(16);
    const t0 = Date.now();
    for (let i = 0; i < 50000; i++) ht.set(`key_${i}`, i);
    const insertMs = Date.now() - t0;
    
    const t1 = Date.now();
    for (let i = 0; i < 50000; i++) ht.get(`key_${i}`);
    const lookupMs = Date.now() - t1;
    
    console.log(`    Partitioned HT 50K: insert=${insertMs}ms, lookup=${lookupMs}ms`);
    assert.equal(ht.size, 50000);
  });
});
