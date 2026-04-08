// bitmap-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitVector, BitmapIndex } from './bitmap-index.js';

describe('BitVector', () => {
  it('set/get/clear', () => {
    const bv = new BitVector(100);
    bv.set(0);
    bv.set(31);
    bv.set(32);
    bv.set(99);
    assert.ok(bv.get(0));
    assert.ok(bv.get(31));
    assert.ok(bv.get(32));
    assert.ok(bv.get(99));
    assert.ok(!bv.get(50));
    bv.clear(31);
    assert.ok(!bv.get(31));
  });

  it('AND operation', () => {
    const a = new BitVector(64);
    const b = new BitVector(64);
    a.set(1); a.set(2); a.set(3);
    b.set(2); b.set(3); b.set(4);
    const result = a.and(b);
    assert.ok(!result.get(1));
    assert.ok(result.get(2));
    assert.ok(result.get(3));
    assert.ok(!result.get(4));
  });

  it('OR operation', () => {
    const a = new BitVector(64);
    const b = new BitVector(64);
    a.set(1); a.set(2);
    b.set(3); b.set(4);
    const result = a.or(b);
    assert.ok(result.get(1));
    assert.ok(result.get(2));
    assert.ok(result.get(3));
    assert.ok(result.get(4));
  });

  it('NOT operation', () => {
    const bv = new BitVector(8);
    bv.set(0); bv.set(2); bv.set(4);
    const inv = bv.not(8);
    assert.ok(!inv.get(0));
    assert.ok(inv.get(1));
    assert.ok(!inv.get(2));
    assert.ok(inv.get(3));
    assert.equal(inv.popcount(), 5);
  });

  it('popcount', () => {
    const bv = new BitVector(100);
    bv.set(0); bv.set(10); bv.set(20); bv.set(99);
    assert.equal(bv.popcount(), 4);
  });

  it('positions iterator', () => {
    const bv = new BitVector(64);
    bv.set(5); bv.set(10); bv.set(63);
    assert.deepEqual([...bv.positions()], [5, 10, 63]);
  });
});

describe('BitmapIndex', () => {
  const statuses = ['active', 'active', 'inactive', 'active', 'banned', 'inactive', 'active', 'banned'];

  it('build and query EQ', () => {
    const idx = new BitmapIndex();
    idx.build(statuses);
    
    const activeRows = idx.getRows('active');
    assert.deepEqual(activeRows, [0, 1, 3, 6]);
    assert.equal(idx.eq('active').popcount(), 4);
  });

  it('query IN', () => {
    const idx = new BitmapIndex();
    idx.build(statuses);
    
    const activeOrBanned = idx.in(['active', 'banned']);
    assert.equal(activeOrBanned.popcount(), 6);
  });

  it('query NOT EQ', () => {
    const idx = new BitmapIndex();
    idx.build(statuses);
    
    const notActive = idx.neq('active');
    assert.equal(notActive.popcount(), 4); // 2 inactive + 2 banned
  });

  it('compound query: AND', () => {
    const idx1 = new BitmapIndex();
    const idx2 = new BitmapIndex();
    idx1.build(['eng', 'eng', 'sales', 'eng', 'sales', 'hr']);
    idx2.build(['senior', 'junior', 'senior', 'senior', 'junior', 'senior']);
    
    // Senior engineers
    const result = idx1.eq('eng').and(idx2.eq('senior'));
    assert.deepEqual([...result.positions()], [0, 3]);
  });

  it('cardinality and distinct values', () => {
    const idx = new BitmapIndex();
    idx.build(statuses);
    assert.equal(idx.cardinality, 3);
    assert.deepEqual(idx.distinctValues.sort(), ['active', 'banned', 'inactive']);
  });

  it('benchmark: 100K rows', () => {
    const idx = new BitmapIndex();
    const values = Array.from({ length: 100000 }, () => 
      ['red', 'green', 'blue', 'yellow'][Math.floor(Math.random() * 4)]
    );

    const t0 = Date.now();
    idx.build(values);
    const buildMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) idx.eq('red');
    const queryMs = Date.now() - t1;

    const t2 = Date.now();
    for (let i = 0; i < 10000; i++) idx.eq('red').and(idx.eq('blue'));
    const andMs = Date.now() - t2;

    console.log(`    100K rows: build ${buildMs}ms, 10K EQ queries ${queryMs}ms, 10K AND queries ${andMs}ms`);
    assert.equal(idx.rowCount, 100000);
  });
});
