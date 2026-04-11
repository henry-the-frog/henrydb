// bloom.test.js — Bloom filter tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomFilter } from './bloom.js';

describe('BloomFilter', () => {
  it('basic add and test', () => {
    const bf = new BloomFilter(100);
    bf.add('hello');
    bf.add('world');
    
    assert.ok(bf.test('hello'));
    assert.ok(bf.test('world'));
    assert.ok(!bf.test('foo'));
  });

  it('no false negatives', () => {
    const bf = new BloomFilter(1000);
    const items = [];
    for (let i = 0; i < 1000; i++) {
      items.push(`item-${i}`);
      bf.add(`item-${i}`);
    }
    
    // All added items MUST be found
    for (const item of items) {
      assert.ok(bf.test(item), `False negative for ${item}`);
    }
  });

  it('false positive rate within expected bounds', () => {
    const n = 1000;
    const fpr = 0.01;
    const bf = new BloomFilter(n, fpr);
    
    for (let i = 0; i < n; i++) bf.add(`item-${i}`);
    
    // Test with items NOT in the set
    let falsePositives = 0;
    const tests = 10000;
    for (let i = 0; i < tests; i++) {
      if (bf.test(`not-in-set-${i}`)) falsePositives++;
    }
    
    const actualFPR = falsePositives / tests;
    // Allow 3x margin (statistical variance)
    assert.ok(actualFPR < fpr * 3, 
      `FPR too high: ${actualFPR.toFixed(4)} (expected < ${(fpr * 3).toFixed(4)})`);
  });

  it('estimated FPR reasonable', () => {
    const bf = new BloomFilter(100, 0.05);
    for (let i = 0; i < 100; i++) bf.add(`item-${i}`);
    
    const estimated = bf.estimateFPR();
    assert.ok(estimated > 0 && estimated < 0.15, `Estimated FPR: ${estimated}`);
  });

  it('BloomFilter.from() convenience', () => {
    const items = ['a', 'b', 'c', 'd', 'e'];
    const bf = BloomFilter.from(items);
    
    for (const item of items) {
      assert.ok(bf.test(item));
    }
    assert.equal(bf.count, items.length);
  });

  it('merge combines two filters', () => {
    const bf1 = new BloomFilter(100);
    const bf2 = new BloomFilter(100);
    
    bf1.add('a');
    bf1.add('b');
    bf2.add('c');
    bf2.add('d');
    
    bf1.merge(bf2);
    
    assert.ok(bf1.test('a'));
    assert.ok(bf1.test('b'));
    assert.ok(bf1.test('c'));
    assert.ok(bf1.test('d'));
  });

  it('merge fails with incompatible filters', () => {
    const bf1 = new BloomFilter(100);
    const bf2 = new BloomFilter(200);
    assert.throws(() => bf1.merge(bf2), /Cannot merge/);
  });

  it('memory efficiency', () => {
    const bf = new BloomFilter(10000, 0.01);
    // For 10000 items at 1% FPR, should use ~12KB (9.58 bits/item)
    assert.ok(bf.byteSize < 20000, `Too much memory: ${bf.byteSize} bytes`);
    assert.ok(bf.byteSize > 5000, `Suspiciously small: ${bf.byteSize} bytes`);
  });

  it('parameters computed correctly', () => {
    const bf = new BloomFilter(1000, 0.01);
    // m = -1000 * ln(0.01) / (ln(2))^2 ≈ 9585
    assert.ok(bf.bitCount > 9000 && bf.bitCount < 10000, `m=${bf.bitCount}`);
    // k = (m/n) * ln(2) ≈ 6.6 → 7
    assert.ok(bf.hashCount >= 6 && bf.hashCount <= 8, `k=${bf.hashCount}`);
  });

  it('empty filter returns false for everything', () => {
    const bf = new BloomFilter(100);
    assert.ok(!bf.test('anything'));
    assert.ok(!bf.test(''));
    assert.ok(!bf.test('test'));
  });
});

describe('BloomFilter Stress', () => {
  it('10000 items, measure actual FPR', () => {
    const n = 10000;
    const targetFPR = 0.01;
    const bf = new BloomFilter(n, targetFPR);
    
    // Add items
    for (let i = 0; i < n; i++) bf.add(`exists-${i}`);
    
    // No false negatives
    for (let i = 0; i < n; i++) {
      assert.ok(bf.test(`exists-${i}`));
    }
    
    // Measure FPR
    let fp = 0;
    const testCount = 100000;
    for (let i = 0; i < testCount; i++) {
      if (bf.test(`nonexistent-${i}`)) fp++;
    }
    
    const actualFPR = fp / testCount;
    console.log(`  10K items, target=${targetFPR}, actual FPR=${actualFPR.toFixed(4)}, memory=${bf.byteSize} bytes`);
    assert.ok(actualFPR < targetFPR * 2, `FPR ${actualFPR} too high (target ${targetFPR})`);
  });
});
