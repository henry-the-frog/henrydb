// counting-bloom.test.js — Tests for Counting Bloom Filter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CountingBloomFilter } from './counting-bloom.js';

describe('CountingBloomFilter — Basic', () => {
  it('insert and contains', () => {
    const cbf = new CountingBloomFilter(1000);
    cbf.insert('hello');
    cbf.insert('world');
    assert.ok(cbf.contains('hello'));
    assert.ok(cbf.contains('world'));
    assert.ok(!cbf.contains('missing'));
  });

  it('delete removes items', () => {
    const cbf = new CountingBloomFilter(1000);
    cbf.insert('temp');
    assert.ok(cbf.contains('temp'));
    cbf.delete('temp');
    assert.ok(!cbf.contains('temp'), 'Deleted item should not be found');
  });

  it('delete preserves other items', () => {
    const cbf = new CountingBloomFilter(1000);
    cbf.insert('keep');
    cbf.insert('remove');
    cbf.delete('remove');
    assert.ok(cbf.contains('keep'), 'Non-deleted item should still be found');
    assert.ok(!cbf.contains('remove'), 'Deleted item should not be found');
  });

  it('handles duplicate inserts', () => {
    const cbf = new CountingBloomFilter(1000);
    cbf.insert('dup');
    cbf.insert('dup');
    cbf.insert('dup');
    assert.ok(cbf.contains('dup'));
    cbf.delete('dup');
    assert.ok(cbf.contains('dup'), 'After 3 inserts and 1 delete, should still be found');
    cbf.delete('dup');
    assert.ok(cbf.contains('dup'), 'After 3 inserts and 2 deletes, should still be found');
    cbf.delete('dup');
    assert.ok(!cbf.contains('dup'), 'After 3 inserts and 3 deletes, should not be found');
  });

  it('no false negatives', () => {
    const cbf = new CountingBloomFilter(10000);
    const items = [];
    for (let i = 0; i < 5000; i++) {
      const key = `item-${i}`;
      cbf.insert(key);
      items.push(key);
    }
    
    let falseNegatives = 0;
    for (const key of items) {
      if (!cbf.contains(key)) falseNegatives++;
    }
    assert.equal(falseNegatives, 0, 'Bloom filter should have zero false negatives');
  });

  it('false positive rate within target', () => {
    const cbf = new CountingBloomFilter(1000, 0.05); // 5% target
    for (let i = 0; i < 1000; i++) cbf.insert(`item-${i}`);
    
    let fp = 0;
    const trials = 10000;
    for (let i = 0; i < trials; i++) {
      if (cbf.contains(`nothere-${i}`)) fp++;
    }
    
    const fpr = fp / trials;
    console.log(`    CBF FPR: ${(fpr * 100).toFixed(2)}% (target: 5%)`);
    assert.ok(fpr < 0.15, `FPR ${fpr} exceeds 15%`); // Some margin
  });
});

describe('CountingBloomFilter — Advanced', () => {
  it('merge combines two filters', () => {
    const a = new CountingBloomFilter(1000, 0.01);
    const b = new CountingBloomFilter(1000, 0.01);
    
    a.insert('from_a');
    b.insert('from_b');
    
    a.merge(b);
    
    assert.ok(a.contains('from_a'));
    assert.ok(a.contains('from_b'));
  });

  it('estimatedFPR tracks load', () => {
    const cbf = new CountingBloomFilter(1000);
    
    const fpr0 = cbf.estimatedFPR();
    assert.equal(fpr0, 0); // Empty filter: FPR = 0
    
    for (let i = 0; i < 500; i++) cbf.insert(`item-${i}`);
    const fpr500 = cbf.estimatedFPR();
    
    for (let i = 500; i < 1000; i++) cbf.insert(`item-${i}`);
    const fpr1000 = cbf.estimatedFPR();
    
    assert.ok(fpr1000 > fpr500, 'FPR should increase with more items');
    console.log(`    Estimated FPR: @500=${(fpr500 * 100).toFixed(2)}%, @1000=${(fpr1000 * 100).toFixed(2)}%`);
  });

  it('toBitArray produces compact representation', () => {
    const cbf = new CountingBloomFilter(1000);
    cbf.insert('test');
    
    const bits = cbf.toBitArray();
    assert.ok(bits instanceof Uint8Array);
    assert.ok(bits.byteLength < cbf.numSlots); // Compact: 1 bit vs 1 byte per slot
  });

  it('getStats reports useful information', () => {
    const cbf = new CountingBloomFilter(1000);
    for (let i = 0; i < 500; i++) cbf.insert(`item-${i}`);
    
    const stats = cbf.getStats();
    assert.equal(stats.items, 500);
    assert.ok(stats.fillRatio > 0 && stats.fillRatio < 1);
    assert.ok(stats.maxCounter >= 1);
    assert.ok(stats.bytesUsed > 0);
  });
});

describe('CountingBloomFilter — Performance', () => {
  it('benchmark: 100K inserts + lookups + deletes', () => {
    const N = 100_000;
    const cbf = new CountingBloomFilter(N * 2); // 2x capacity for clean deletion
    
    const t0 = performance.now();
    for (let i = 0; i < N; i++) cbf.insert(`key-${i}`);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < N; i++) cbf.contains(`key-${i}`);
    const lookupMs = performance.now() - t1;
    
    const t2 = performance.now();
    for (let i = 0; i < N / 2; i++) cbf.delete(`key-${i}`);
    const deleteMs = performance.now() - t2;
    
    console.log(`    ${N} inserts: ${insertMs.toFixed(1)}ms (${(N / insertMs * 1000) | 0}/sec)`);
    console.log(`    ${N} lookups: ${lookupMs.toFixed(1)}ms (${(N / lookupMs * 1000) | 0}/sec)`);
    console.log(`    ${N / 2} deletes: ${deleteMs.toFixed(1)}ms`);
    
    // Verify: second half should remain (zero false negatives)
    let retained = 0;
    for (let i = N / 2; i < N; i++) {
      if (cbf.contains(`key-${i}`)) retained++;
    }
    assert.equal(retained, N / 2, 'All non-deleted keys should be found');
    
    // First half: some may still show as present (false positives from shared counters)
    // This is expected — counting bloom filters trade FP rate for deletion support
    let ghosted = 0;
    for (let i = 0; i < N / 2; i++) {
      if (cbf.contains(`key-${i}`)) ghosted++;
    }
    const ghostRate = ghosted / (N / 2);
    console.log(`    Ghost rate after delete: ${(ghostRate * 100).toFixed(2)}% (${ghosted}/${N / 2})`);
    assert.ok(ghostRate < 0.05, `Ghost rate ${ghostRate} too high (>5%)`);
  });
});
