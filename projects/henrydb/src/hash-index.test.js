// hash-index.test.js — Hash index tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HashIndex } from './hash-index.js';

describe('HashIndex', () => {
  it('insert and find single value', () => {
    const idx = new HashIndex();
    idx.insert(42, { row: 1 });
    const results = idx.find(42);
    assert.equal(results.length, 1);
    assert.deepEqual(results[0], { row: 1 });
  });

  it('find returns empty for missing keys', () => {
    const idx = new HashIndex();
    idx.insert(1, 'a');
    assert.deepEqual(idx.find(999), []);
  });

  it('handles duplicate keys (chaining)', () => {
    const idx = new HashIndex();
    idx.insert('key', 'val1');
    idx.insert('key', 'val2');
    const results = idx.find('key');
    assert.equal(results.length, 2);
  });

  it('delete removes entries', () => {
    const idx = new HashIndex();
    idx.insert(1, 'a');
    idx.insert(2, 'b');
    idx.delete(1);
    assert.deepEqual(idx.find(1), []);
    assert.equal(idx.find(2).length, 1);
  });

  it('has checks existence', () => {
    const idx = new HashIndex();
    idx.insert('key', 'val');
    assert.equal(idx.has('key'), true);
    assert.equal(idx.has('missing'), false);
  });

  it('dynamic resizing on high load factor', () => {
    const idx = new HashIndex(4); // Start small
    for (let i = 0; i < 100; i++) {
      idx.insert(i, `val${i}`);
    }
    
    // All values should still be findable
    for (let i = 0; i < 100; i++) {
      assert.equal(idx.find(i).length, 1);
    }
    
    // Buckets should have doubled multiple times
    assert.ok(idx.stats().buckets > 4);
  });

  it('stats reports useful information', () => {
    const idx = new HashIndex();
    for (let i = 0; i < 50; i++) {
      idx.insert(i, i);
    }
    const stats = idx.stats();
    assert.equal(stats.size, 50);
    assert.ok(stats.loadFactor > 0);
    assert.ok(stats.maxChainLength <= 10); // Good distribution
  });

  it('handles string keys', () => {
    const idx = new HashIndex();
    idx.insert('alice', 1);
    idx.insert('bob', 2);
    idx.insert('charlie', 3);
    
    assert.equal(idx.find('alice')[0], 1);
    assert.equal(idx.find('bob')[0], 2);
    assert.deepEqual(idx.find('david'), []);
  });

  it('handles null and undefined keys', () => {
    const idx = new HashIndex();
    idx.insert(null, 'null_val');
    idx.insert(undefined, 'undef_val');
    
    assert.equal(idx.find(null).length, 1);
    assert.equal(idx.find(undefined).length, 1);
  });
});
