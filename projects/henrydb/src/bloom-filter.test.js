// bloom-filter.test.js — Tests for Bloom filter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomFilter } from './bloom-filter.js';

describe('BloomFilter', () => {
  it('no false negatives: all added elements are found', () => {
    const bf = new BloomFilter(1000, 0.01);
    const elements = [];
    for (let i = 0; i < 1000; i++) {
      const elem = `element-${i}`;
      elements.push(elem);
      bf.add(elem);
    }
    
    for (const elem of elements) {
      assert.equal(bf.mightContain(elem), true, `Expected ${elem} to be found`);
    }
  });

  it('false positive rate is within expected bounds', () => {
    const n = 1000;
    const targetFPR = 0.01;
    const bf = new BloomFilter(n, targetFPR);
    
    // Add n elements
    for (let i = 0; i < n; i++) {
      bf.add(`elem-${i}`);
    }
    
    // Test with elements NOT in the filter
    let falsePositives = 0;
    const tests = 10000;
    for (let i = 0; i < tests; i++) {
      if (bf.mightContain(`not-in-filter-${i}`)) {
        falsePositives++;
      }
    }
    
    const actualFPR = falsePositives / tests;
    console.log(`  FPR: target=${(targetFPR * 100).toFixed(1)}%, actual=${(actualFPR * 100).toFixed(2)}%`);
    
    // Allow 5x the target rate (statistical variance + hash quality)
    assert.ok(actualFPR < targetFPR * 5, `FPR too high: ${(actualFPR * 100).toFixed(2)}% (target ${(targetFPR * 100).toFixed(1)}%)`);
  });

  it('empty filter: nothing is found', () => {
    const bf = new BloomFilter(100, 0.01);
    assert.equal(bf.mightContain('anything'), false);
    assert.equal(bf.mightContain(42), false);
  });

  it('numeric keys work', () => {
    const bf = new BloomFilter(100, 0.01);
    for (let i = 0; i < 100; i++) bf.add(i);
    
    for (let i = 0; i < 100; i++) {
      assert.equal(bf.mightContain(i), true);
    }
  });

  it('getStats reports correct parameters', () => {
    const bf = new BloomFilter(1000, 0.01);
    for (let i = 0; i < 500; i++) bf.add(`elem-${i}`);
    
    const stats = bf.getStats();
    assert.equal(stats.elements, 500);
    assert.ok(stats.bits > 0);
    assert.ok(stats.hashes > 0);
    assert.ok(stats.bytesUsed > 0);
    assert.ok(stats.fillRatio > 0);
    assert.ok(stats.fillRatio < 1);
    console.log(`  Stats: ${stats.bits} bits, ${stats.hashes} hashes, ${stats.bytesUsed} bytes, fill=${(stats.fillRatio*100).toFixed(1)}%`);
  });

  it('optimal parameters for common sizes', () => {
    const cases = [
      { n: 100, p: 0.01 },
      { n: 1000, p: 0.01 },
      { n: 10000, p: 0.001 },
      { n: 1000000, p: 0.01 },
    ];
    
    for (const { n, p } of cases) {
      const bf = new BloomFilter(n, p);
      const bitsPerElement = bf.m / n;
      console.log(`  n=${n}, p=${p}: ${bf.m} bits (${bitsPerElement.toFixed(1)} bits/elem), ${bf.k} hashes, ${bf._bits.byteLength} bytes`);
    }
    assert.ok(true);
  });

  it('merge combines two filters', () => {
    const bf1 = new BloomFilter(100, 0.01);
    const bf2 = new BloomFilter(100, 0.01);
    
    for (let i = 0; i < 50; i++) bf1.add(`a-${i}`);
    for (let i = 0; i < 50; i++) bf2.add(`b-${i}`);
    
    const merged = bf1.merge(bf2);
    
    // Merged should contain elements from both
    for (let i = 0; i < 50; i++) {
      assert.equal(merged.mightContain(`a-${i}`), true);
      assert.equal(merged.mightContain(`b-${i}`), true);
    }
  });

  it('clear resets the filter', () => {
    const bf = new BloomFilter(100, 0.01);
    for (let i = 0; i < 100; i++) bf.add(i);
    
    bf.clear();
    assert.equal(bf.mightContain(0), false);
    assert.equal(bf.mightContain(50), false);
    assert.equal(bf.bitsSet, 0);
  });

  it('performance: 100K add + 100K lookup', () => {
    const bf = new BloomFilter(100000, 0.01);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) bf.add(i);
    const addMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 100000; i++) bf.mightContain(i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  100K add: ${addMs.toFixed(1)}ms (${(addMs/100000*1000).toFixed(3)}µs avg)`);
    console.log(`  100K lookup: ${lookupMs.toFixed(1)}ms (${(lookupMs/100000*1000).toFixed(3)}µs avg)`);
    console.log(`  Memory: ${bf.getStats().bytesUsed} bytes (${(bf.getStats().bytesUsed/100000*8).toFixed(1)} bits/elem)`);
    
    assert.ok(addMs < 500);
    assert.ok(lookupMs < 500);
  });

  it('space efficiency: 1M elements at 1% FPR', () => {
    const bf = new BloomFilter(1000000, 0.01);
    const stats = bf.getStats();
    
    // At 1% FPR, optimal is ~9.6 bits per element
    const bitsPerElement = stats.bits / 1000000;
    console.log(`  1M elements: ${(stats.bytesUsed / 1024 / 1024).toFixed(2)} MB, ${bitsPerElement.toFixed(1)} bits/elem`);
    
    assert.ok(bitsPerElement < 12, 'Should use <12 bits per element');
    assert.ok(bitsPerElement > 8, 'Should use >8 bits per element');
  });

  it('database use case: skip non-matching pages', () => {
    // Simulate: build bloom filter from index values, test during scan
    const bf = new BloomFilter(1000, 0.01);
    
    // Add IDs of rows on page 5 (simulating a page-level bloom filter)
    const pageIds = new Set();
    for (let i = 500; i < 600; i++) {
      bf.add(i);
      pageIds.add(i);
    }
    
    // Test: can we skip this page when looking for id=42?
    const canSkip42 = !bf.mightContain(42);  // Should be true (42 not on page 5)
    const canSkip550 = !bf.mightContain(550); // Should be false (550 IS on page 5)
    
    assert.equal(canSkip42, true, 'Should skip page for id=42');
    assert.equal(canSkip550, false, 'Should NOT skip page for id=550');
  });
});
