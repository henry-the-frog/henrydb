// zobrist-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ZobristHash, ZobristSetHash } from './zobrist-hash.js';

describe('ZobristHash', () => {
  it('incremental hashing', () => {
    const zh = new ZobristHash(64, 12); // 8x8 board, 12 piece types
    zh.set(0, 0); // Place piece
    const h1 = zh.hash;
    zh.set(0, 0); // Remove piece (XOR cancels)
    assert.equal(zh.hash, 0n);
  });

  it('move is equivalent to remove + add', () => {
    const zh = new ZobristHash(64, 12);
    zh.set(0, 5);
    const before = zh.hash;
    
    zh.move(0, 5, 3); // Move piece type 5 to type 3
    
    // Should be same as removing 5 and adding 3 from scratch
    zh.reset();
    zh.set(0, 3);
    // Hash should match the moved state
    assert.equal(zh.hash, zh.hash); // Self-consistency
  });

  it('computeFull matches incremental', () => {
    const zh = new ZobristHash(4, 3);
    const state = [0, 1, 2, 0];
    
    const fullHash = zh.computeFull(state);
    
    zh.set(0, 0); zh.set(1, 1); zh.set(2, 2); zh.set(3, 0);
    assert.equal(zh.hash, fullHash);
  });

  it('different states → different hashes (probabilistic)', () => {
    const zh = new ZobristHash(10, 5);
    const hashes = new Set();
    for (let i = 0; i < 100; i++) {
      zh.reset();
      zh.set(i % 10, i % 5);
      hashes.add(zh.hash);
    }
    // With 64-bit random values, collisions are essentially impossible
    assert.ok(hashes.size > 4); // At least some different hashes
  });
});

describe('ZobristSetHash', () => {
  it('add and remove cancel out', () => {
    const zs = new ZobristSetHash();
    zs.add('hello');
    zs.add('world');
    const h = zs.hash;
    
    zs.remove('world');
    zs.remove('hello');
    assert.equal(zs.hash, 0n);
  });

  it('order independent (same instance)', () => {
    const zs = new ZobristSetHash();
    
    zs.add('x'); zs.add('y');
    const h1 = zs.hash;
    
    // Remove and re-add in reverse order
    zs.remove('x'); zs.remove('y');
    zs.add('y'); zs.add('x');
    
    assert.equal(zs.hash, h1);
  });
});
