// extendible-hashing.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExtendibleHashTable } from './extendible-hashing.js';

describe('ExtendibleHashTable', () => {
  it('basic set/get', () => {
    const ht = new ExtendibleHashTable(4);
    ht.set('a', 1);
    ht.set('b', 2);
    assert.equal(ht.get('a'), 1);
    assert.equal(ht.get('b'), 2);
    assert.equal(ht.get('c'), undefined);
  });

  it('update existing', () => {
    const ht = new ExtendibleHashTable(4);
    ht.set('k', 'old');
    ht.set('k', 'new');
    assert.equal(ht.get('k'), 'new');
  });

  it('triggers directory doubling', () => {
    const ht = new ExtendibleHashTable(2); // Very small buckets
    for (let i = 0; i < 20; i++) ht.set(`key_${i}`, i);
    
    assert.ok(ht.getStats().globalDepth > 1);
    for (let i = 0; i < 20; i++) assert.equal(ht.get(`key_${i}`), i);
  });

  it('delete', () => {
    const ht = new ExtendibleHashTable(4);
    ht.set('x', 100);
    assert.ok(ht.delete('x'));
    assert.equal(ht.get('x'), undefined);
    assert.ok(!ht.delete('nonexistent'));
  });

  it('integer keys', () => {
    const ht = new ExtendibleHashTable(4);
    for (let i = 0; i < 100; i++) ht.set(i, i * 10);
    for (let i = 0; i < 100; i++) assert.equal(ht.get(i), i * 10);
  });

  it('stats', () => {
    const ht = new ExtendibleHashTable(4);
    for (let i = 0; i < 50; i++) ht.set(i, i);
    
    const stats = ht.getStats();
    assert.ok(stats.globalDepth >= 1);
    assert.equal(stats.entries, 50);
    assert.ok(stats.uniqueBuckets > 1);
  });

  it('benchmark: 10K ops', () => {
    const ht = new ExtendibleHashTable(8);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) ht.set(i, i);
    const buildMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) ht.get(i);
    const lookupMs = Date.now() - t1;

    const stats = ht.getStats();
    console.log(`    10K: build ${buildMs}ms, lookup ${lookupMs}ms, depth ${stats.globalDepth}, buckets ${stats.uniqueBuckets}`);
  });
});
