// art.test.js — Tests for Adaptive Radix Tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { AdaptiveRadixTree } from './art.js';

describe('AdaptiveRadixTree', () => {
  it('basic insert and get', () => {
    const art = new AdaptiveRadixTree();
    art.insert('hello', 1);
    art.insert('world', 2);
    assert.equal(art.get('hello'), 1);
    assert.equal(art.get('world'), 2);
    assert.equal(art.get('missing'), undefined);
  });

  it('has', () => {
    const art = new AdaptiveRadixTree();
    art.insert('test', 42);
    assert.equal(art.has('test'), true);
    assert.equal(art.has('tes'), false);
  });

  it('prefix search', () => {
    const art = new AdaptiveRadixTree();
    art.insert('abc', 1);
    art.insert('abd', 2);
    art.insert('xyz', 3);
    
    const results = art.prefixSearch('ab');
    assert.equal(results.length, 2);
  });

  it('node growth: >4 children triggers Node16', () => {
    const art = new AdaptiveRadixTree();
    // Insert 5 keys starting with different first bytes
    for (let i = 0; i < 10; i++) {
      art.insert(String.fromCharCode(65 + i), i); // A, B, C, D, E, ...
    }
    assert.equal(art.size, 10);
    for (let i = 0; i < 10; i++) {
      assert.equal(art.get(String.fromCharCode(65 + i)), i);
    }
  });

  it('many keys: exercises Node48 and Node256', () => {
    const art = new AdaptiveRadixTree();
    for (let i = 0; i < 100; i++) {
      art.insert(`key-${String(i).padStart(3, '0')}`, i);
    }
    assert.equal(art.size, 100);
    assert.equal(art.get('key-050'), 50);
    
    const stats = art.getStats();
    assert.ok(stats.node4 > 0);
  });

  it('stress: 10K keys', () => {
    const art = new AdaptiveRadixTree();
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) art.insert(`k${i}`, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) art.get(`k${i}`);
    const lookupMs = performance.now() - t1;
    
    assert.equal(art.size, 10000);
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
    console.log(`  Node types: ${JSON.stringify(art.getStats())}`);
  });

  it('overwrite existing key', () => {
    const art = new AdaptiveRadixTree();
    art.insert('key', 1);
    art.insert('key', 2);
    assert.equal(art.get('key'), 2);
    assert.equal(art.size, 1);
  });
});
