// bitset.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitSet } from './bitset.js';

describe('BitSet', () => {
  it('set, get, clear', () => {
    const bs = new BitSet(100);
    bs.set(42);
    assert.equal(bs.get(42), 1);
    bs.clear(42);
    assert.equal(bs.get(42), 0);
  });

  it('AND, OR, XOR', () => {
    const a = new BitSet(64); a.set(1); a.set(2); a.set(3);
    const b = new BitSet(64); b.set(2); b.set(3); b.set(4);
    
    assert.deepEqual(a.and(b).toArray(), [2, 3]);
    assert.deepEqual(a.or(b).toArray(), [1, 2, 3, 4]);
    assert.deepEqual(a.xor(b).toArray(), [1, 4]);
  });

  it('popcount', () => {
    const bs = new BitSet(100);
    for (let i = 0; i < 50; i++) bs.set(i);
    assert.equal(bs.popcount(), 50);
  });

  it('space efficiency', () => {
    const bs = new BitSet(10000);
    assert.ok(bs.bytesUsed < 2000); // ~1.25KB vs 10KB for boolean array
  });

  it('performance: 100K set/get', () => {
    const bs = new BitSet(100000);
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) bs.set(i);
    for (let i = 0; i < 100000; i++) bs.get(i);
    const elapsed = performance.now() - t0;
    console.log(`  200K bit ops: ${elapsed.toFixed(1)}ms`);
  });
});
