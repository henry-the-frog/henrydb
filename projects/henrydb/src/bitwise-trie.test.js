// bitwise-trie.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitwiseTrie } from './bitwise-trie.js';

describe('BitwiseTrie', () => {
  it('set and get', () => {
    const bt = new BitwiseTrie();
    bt.set(42, 'answer');
    bt.set(0, 'zero');
    bt.set(1000000, 'million');
    assert.equal(bt.get(42), 'answer');
    assert.equal(bt.get(0), 'zero');
    assert.equal(bt.get(1000000), 'million');
    assert.equal(bt.get(999), undefined);
  });

  it('delete', () => {
    const bt = new BitwiseTrie();
    bt.set(1, 'a');
    assert.equal(bt.delete(1), true);
    assert.equal(bt.has(1), false);
    assert.equal(bt.delete(999), false);
  });

  it('10K elements', () => {
    const bt = new BitwiseTrie();
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) bt.set(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) assert.equal(bt.get(i), i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
  });

  it('large keys', () => {
    const bt = new BitwiseTrie();
    bt.set(0xFFFFFFFF, 'max');
    bt.set(0x80000000, 'half');
    assert.equal(bt.get(0xFFFFFFFF), 'max');
    assert.equal(bt.get(0x80000000), 'half');
  });
});
