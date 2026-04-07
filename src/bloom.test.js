// bloom.test.js — Bloom filter tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomFilter } from './bloom.js';

describe('Bloom Filter', () => {
  it('returns true for added elements', () => {
    const bf = new BloomFilter(100, 0.01);
    bf.add('hello');
    bf.add('world');
    bf.add(42);
    
    assert.equal(bf.mightContain('hello'), true);
    assert.equal(bf.mightContain('world'), true);
    assert.equal(bf.mightContain(42), true);
  });

  it('returns false for definitely absent elements', () => {
    const bf = new BloomFilter(100, 0.01);
    bf.add('hello');
    
    // These should almost certainly return false
    assert.equal(bf.mightContain('xyzzy'), false);
    assert.equal(bf.mightContain('nothere'), false);
    assert.equal(bf.mightContain(99999), false);
  });

  it('handles many insertions', () => {
    const bf = new BloomFilter(10000, 0.01);
    for (let i = 0; i < 10000; i++) {
      bf.add(`item_${i}`);
    }
    
    // All inserted items should test positive
    for (let i = 0; i < 100; i++) {
      assert.equal(bf.mightContain(`item_${i}`), true);
    }
  });

  it('false positive rate is within expected bounds', () => {
    const bf = new BloomFilter(1000, 0.05); // 5% FPR target
    for (let i = 0; i < 1000; i++) {
      bf.add(`item_${i}`);
    }
    
    // Test 1000 items NOT in the filter
    let falsePositives = 0;
    for (let i = 1000; i < 2000; i++) {
      if (bf.mightContain(`item_${i}`)) falsePositives++;
    }
    
    // FPR should be roughly < 10% (generous margin for statistical noise)
    const fpr = falsePositives / 1000;
    assert.ok(fpr < 0.10, `False positive rate ${fpr} exceeds 10%`);
  });

  it('stats returns useful information', () => {
    const bf = new BloomFilter(100, 0.01);
    bf.add('a');
    bf.add('b');
    bf.add('c');
    
    const stats = bf.stats();
    assert.equal(stats.itemCount, 3);
    assert.ok(stats.size > 0);
    assert.ok(stats.numHashes > 0);
    assert.ok(stats.fillRatio > 0);
  });

  it('empty filter returns false for everything', () => {
    const bf = new BloomFilter(100, 0.01);
    assert.equal(bf.mightContain('anything'), false);
    assert.equal(bf.mightContain(42), false);
    assert.equal(bf.mightContain(''), false);
  });

  it('handles different types', () => {
    const bf = new BloomFilter(100, 0.01);
    bf.add(1);
    bf.add('1');
    bf.add(null);
    
    assert.equal(bf.mightContain(1), true);
    assert.equal(bf.mightContain('1'), true);
    assert.equal(bf.mightContain(null), true);
  });

  it('optimal size calculations', () => {
    // For 1000 items at 1% FPR, size should be ~9585 bits
    const bf = new BloomFilter(1000, 0.01);
    assert.ok(bf._size > 9000, `Expected ~9585 bits, got ${bf._size}`);
    assert.ok(bf._size < 11000);
    // Number of hash functions should be ~7
    assert.ok(bf._numHashes >= 5 && bf._numHashes <= 10, `Expected ~7 hashes, got ${bf._numHashes}`);
  });
});
