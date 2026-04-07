// bloom-filter.test.js — Tests for Bloom filter implementations

import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { BloomFilter, CountingBloomFilter } from './bloom-filter.js';

describe('BloomFilter: Basic Operations', () => {
  it('no false negatives — added items always found', () => {
    const bf = new BloomFilter(1000, 0.01);
    
    for (let i = 0; i < 1000; i++) {
      bf.add(`key-${i}`);
    }
    
    for (let i = 0; i < 1000; i++) {
      assert.ok(bf.mightContain(`key-${i}`), `key-${i} should be found`);
    }
  });

  it('non-added items mostly not found (low false positive rate)', () => {
    const bf = new BloomFilter(1000, 0.01);
    
    for (let i = 0; i < 1000; i++) {
      bf.add(`key-${i}`);
    }
    
    let falsePositives = 0;
    const trials = 10000;
    for (let i = 1000; i < 1000 + trials; i++) {
      if (bf.mightContain(`key-${i}`)) falsePositives++;
    }
    
    const fpr = falsePositives / trials;
    console.log(`    FPR: ${(fpr * 100).toFixed(2)}% (target: 1.00%)`);
    assert.ok(fpr < 0.05, `FPR ${fpr} should be < 5% (target 1%)`);
  });

  it('empty filter contains nothing', () => {
    const bf = new BloomFilter(100, 0.01);
    
    for (let i = 0; i < 100; i++) {
      assert.equal(bf.mightContain(`key-${i}`), false);
    }
  });

  it('handles various key types', () => {
    const bf = new BloomFilter(100, 0.01);
    
    bf.add('hello');
    bf.add(42);
    bf.add(true);
    bf.add('');
    
    assert.ok(bf.mightContain('hello'));
    assert.ok(bf.mightContain(42));
    assert.ok(bf.mightContain(true));
    assert.ok(bf.mightContain(''));
  });
});

describe('BloomFilter: Parameter Calculation', () => {
  it('calculates optimal parameters', () => {
    const bf = new BloomFilter(10000, 0.01);
    const stats = bf.stats;
    
    assert.ok(stats.bits > 0, 'Should have bits');
    assert.ok(stats.hashes >= 1, 'Should have at least 1 hash');
    assert.ok(stats.hashes <= 32, 'Should have at most 32 hashes');
    console.log(`    10K items, 1% FPR: ${stats.bits} bits, ${stats.hashes} hashes, ${stats.bytesUsed} bytes`);
  });

  it('lower FPR requires more bits', () => {
    const bf1 = new BloomFilter(1000, 0.1);
    const bf2 = new BloomFilter(1000, 0.01);
    const bf3 = new BloomFilter(1000, 0.001);
    
    assert.ok(bf2.stats.bits > bf1.stats.bits, '0.01 needs more bits than 0.1');
    assert.ok(bf3.stats.bits > bf2.stats.bits, '0.001 needs more bits than 0.01');
  });

  it('more items requires more bits', () => {
    const bf1 = new BloomFilter(100, 0.01);
    const bf2 = new BloomFilter(10000, 0.01);
    
    assert.ok(bf2.stats.bits > bf1.stats.bits);
  });
});

describe('BloomFilter: Serialization', () => {
  it('serialize and deserialize produces identical filter', () => {
    const bf = new BloomFilter(500, 0.01);
    for (let i = 0; i < 500; i++) bf.add(`key-${i}`);
    
    const buf = bf.serialize();
    const bf2 = BloomFilter.deserialize(buf);
    
    // All originally added items should be found
    for (let i = 0; i < 500; i++) {
      assert.ok(bf2.mightContain(`key-${i}`), `key-${i} should survive serialization`);
    }
    
    // Stats should match
    assert.equal(bf2.stats.bits, bf.stats.bits);
    assert.equal(bf2.stats.hashes, bf.stats.hashes);
    assert.equal(bf2.stats.items, bf.stats.items);
  });
});

describe('BloomFilter: LSM Tree Simulation', () => {
  it('skips SSTables that definitely dont contain key', () => {
    // Simulate 5 SSTables, each with 1000 keys
    const sstables = [];
    for (let level = 0; level < 5; level++) {
      const bf = new BloomFilter(1000, 0.01);
      for (let i = level * 1000; i < (level + 1) * 1000; i++) {
        bf.add(`key-${i}`);
      }
      sstables.push(bf);
    }
    
    // Look up key in range of SSTable 2 (keys 2000-2999)
    const key = 'key-2500';
    let sstablesChecked = 0;
    for (const bf of sstables) {
      if (bf.mightContain(key)) {
        sstablesChecked++;
      }
    }
    
    // Should only need to check 1 SSTable (the one containing the key)
    // Plus maybe 1-2 false positives
    assert.ok(sstablesChecked <= 3, `Should check at most 3 SSTables, checked ${sstablesChecked}`);
    console.log(`    5 SSTables, key in #2: checked ${sstablesChecked} (ideal: 1)`);
  });

  it('100K keys with 1% FPR uses reasonable memory', () => {
    const bf = new BloomFilter(100000, 0.01);
    for (let i = 0; i < 100000; i++) bf.add(`key-${i}`);
    
    const stats = bf.stats;
    const bytesPerKey = stats.bytesUsed / 100000;
    console.log(`    100K keys: ${stats.bytesUsed} bytes (${bytesPerKey.toFixed(1)} bytes/key)`);
    
    // Should be ~1.2 bytes/key for 1% FPR (theoretical: -ln(0.01) / ln(2)^2 * n = 9.585n bits ≈ 1.2 bytes)
    assert.ok(bytesPerKey < 3, `${bytesPerKey} bytes/key should be < 3`);
  });
});

describe('CountingBloomFilter', () => {
  it('supports add and membership test', () => {
    const cbf = new CountingBloomFilter(1000, 0.01);
    
    for (let i = 0; i < 100; i++) cbf.add(`key-${i}`);
    
    for (let i = 0; i < 100; i++) {
      assert.ok(cbf.mightContain(`key-${i}`));
    }
  });

  it('supports deletion', () => {
    const cbf = new CountingBloomFilter(100, 0.01);
    
    cbf.add('alice');
    cbf.add('bob');
    cbf.add('charlie');
    
    assert.ok(cbf.mightContain('alice'));
    assert.ok(cbf.mightContain('bob'));
    
    cbf.remove('bob');
    
    assert.ok(cbf.mightContain('alice'), 'alice should still be present');
    // bob might still show as present due to hash collisions, but remove() should decrement counters
    assert.ok(cbf.mightContain('charlie'), 'charlie should still be present');
  });

  it('no false negatives after add/remove cycles', () => {
    const cbf = new CountingBloomFilter(1000, 0.01);
    
    // Add 100 items
    for (let i = 0; i < 100; i++) cbf.add(`key-${i}`);
    
    // Remove 50 items
    for (let i = 0; i < 50; i++) cbf.remove(`key-${i}`);
    
    // Remaining 50 should still be found
    for (let i = 50; i < 100; i++) {
      assert.ok(cbf.mightContain(`key-${i}`), `key-${i} should survive removal of others`);
    }
  });
});
