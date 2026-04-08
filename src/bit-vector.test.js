// bit-vector.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitVector, BitmapScan } from './bit-vector.js';

describe('BitVector', () => {
  it('set and get', () => {
    const bv = new BitVector(100);
    bv.set(0);
    bv.set(42);
    bv.set(99);
    assert.equal(bv.get(0), 1);
    assert.equal(bv.get(42), 1);
    assert.equal(bv.get(99), 1);
    assert.equal(bv.get(50), 0);
  });

  it('clear', () => {
    const bv = new BitVector(64);
    bv.set(10);
    bv.clear(10);
    assert.equal(bv.get(10), 0);
  });

  it('AND', () => {
    const a = new BitVector(64);
    const b = new BitVector(64);
    a.set(1); a.set(2); a.set(3);
    b.set(2); b.set(3); b.set(4);
    const result = a.and(b);
    assert.equal(result.get(1), 0);
    assert.equal(result.get(2), 1);
    assert.equal(result.get(3), 1);
    assert.equal(result.get(4), 0);
  });

  it('OR', () => {
    const a = new BitVector(64);
    const b = new BitVector(64);
    a.set(1); a.set(2);
    b.set(3); b.set(4);
    const result = a.or(b);
    assert.equal(result.popcount(), 4);
  });

  it('NOT', () => {
    const bv = new BitVector(32);
    bv.set(0);
    const neg = bv.not();
    assert.equal(neg.get(0), 0);
    assert.equal(neg.get(1), 1);
  });

  it('XOR', () => {
    const a = new BitVector(64);
    const b = new BitVector(64);
    a.set(1); a.set(2);
    b.set(2); b.set(3);
    const result = a.xor(b);
    assert.equal(result.get(1), 1);
    assert.equal(result.get(2), 0);
    assert.equal(result.get(3), 1);
  });

  it('popcount', () => {
    const bv = new BitVector(100);
    for (let i = 0; i < 100; i += 2) bv.set(i);
    assert.equal(bv.popcount(), 50);
  });

  it('ones iterator', () => {
    const bv = new BitVector(64);
    bv.set(5); bv.set(10); bv.set(63);
    const positions = [...bv.ones()];
    assert.deepEqual(positions, [5, 10, 63]);
  });

  it('setAll and clearAll', () => {
    const bv = new BitVector(64);
    bv.setAll();
    assert.ok(bv.popcount() >= 64);
    bv.clearAll();
    assert.equal(bv.popcount(), 0);
  });
});

describe('BitmapScan', () => {
  const data = Array.from({ length: 1000 }, (_, i) => ({
    id: i, age: 20 + (i % 40), dept: i % 3 === 0 ? 'eng' : i % 3 === 1 ? 'sales' : 'hr',
  }));

  it('create and combine bitmaps', () => {
    const scanner = new BitmapScan(data.length);
    const ageBitmap = scanner.createBitmap(data, r => r.age > 40);
    const deptBitmap = scanner.createBitmap(data, r => r.dept === 'eng');
    
    const combined = scanner.combine([ageBitmap, deptBitmap], 'AND');
    const results = scanner.fetch(data, combined);
    
    assert.ok(results.length > 0);
    assert.ok(results.every(r => r.age > 40 && r.dept === 'eng'));
  });

  it('OR combination', () => {
    const scanner = new BitmapScan(data.length);
    const a = scanner.createBitmap(data, r => r.dept === 'eng');
    const b = scanner.createBitmap(data, r => r.dept === 'hr');
    
    const combined = scanner.combine([a, b], 'OR');
    const results = scanner.fetch(data, combined);
    assert.ok(results.every(r => r.dept === 'eng' || r.dept === 'hr'));
  });

  it('benchmark: 100K bitmap scan', () => {
    const big = Array.from({ length: 100000 }, (_, i) => ({ val: i, group: i % 100 }));
    const scanner = new BitmapScan(big.length);
    
    const t0 = Date.now();
    const b1 = scanner.createBitmap(big, r => r.val > 50000);
    const b2 = scanner.createBitmap(big, r => r.group < 10);
    const combined = scanner.combine([b1, b2], 'AND');
    const results = scanner.fetch(big, combined);
    console.log(`    Bitmap scan 100K: ${Date.now() - t0}ms, ${results.length} results`);
    
    assert.ok(results.length > 0);
    assert.ok(results.every(r => r.val > 50000 && r.group < 10));
  });
});
