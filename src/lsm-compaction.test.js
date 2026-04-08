// lsm-compaction.test.js — Tests for leveled LSM compaction

import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { LSMTree } from './lsm.js';

describe('LSM Leveled Compaction', () => {
  it('compaction triggers after 4 SSTables in level 0', () => {
    const lsm = new LSMTree({ memtableThreshold: 5 });
    
    // Insert enough to create 5+ SSTables
    for (let i = 0; i < 30; i++) {
      lsm.put(`key-${String(i).padStart(3, '0')}`, `val-${i}`);
    }
    
    const stats = lsm.stats();
    assert.ok(stats.compactions > 0, 'Should have triggered at least one compaction');
    assert.ok(stats.sstables < 10, 'Compaction should reduce SSTable count');
  });

  it('all data survives compaction', () => {
    const lsm = new LSMTree({ memtableThreshold: 5 });
    
    const n = 50;
    for (let i = 0; i < n; i++) {
      lsm.put(`key-${String(i).padStart(3, '0')}`, `val-${i}`);
    }
    
    // Force final flush
    lsm.flush();
    
    // Verify all data is still readable
    for (let i = 0; i < n; i++) {
      const val = lsm.get(`key-${String(i).padStart(3, '0')}`);
      assert.equal(val, `val-${i}`, `key-${i} should survive compaction`);
    }
  });

  it('updates survive compaction (latest version wins)', () => {
    const lsm = new LSMTree({ memtableThreshold: 5 });
    
    // Insert initial values
    for (let i = 0; i < 20; i++) {
      lsm.put(`key-${i}`, `original-${i}`);
    }
    
    // Update all values
    for (let i = 0; i < 20; i++) {
      lsm.put(`key-${i}`, `updated-${i}`);
    }
    
    lsm.flush();
    
    // Latest values should be visible
    for (let i = 0; i < 20; i++) {
      const val = lsm.get(`key-${i}`);
      assert.equal(val, `updated-${i}`, `key-${i} should have updated value`);
    }
  });

  it('deletes propagate through compaction', () => {
    const lsm = new LSMTree({ memtableThreshold: 5 });
    
    // Insert values
    for (let i = 0; i < 20; i++) {
      lsm.put(`key-${i}`, `val-${i}`);
    }
    
    // Delete half
    for (let i = 0; i < 10; i++) {
      lsm.delete(`key-${i}`);
    }
    
    lsm.flush();
    
    // Deleted keys should return undefined
    for (let i = 0; i < 10; i++) {
      assert.equal(lsm.get(`key-${i}`), undefined, `key-${i} should be deleted`);
    }
    
    // Non-deleted keys should still be there
    for (let i = 10; i < 20; i++) {
      assert.equal(lsm.get(`key-${i}`), `val-${i}`);
    }
  });

  it('range scan returns correct results after compaction', () => {
    const lsm = new LSMTree({ memtableThreshold: 5 });
    
    for (let i = 0; i < 30; i++) {
      lsm.put(`k-${String(i).padStart(3, '0')}`, `v-${i}`);
    }
    
    lsm.flush();
    
    // Range scan
    const results = lsm.range('k-010', 'k-020');
    assert.ok(results.length >= 10 && results.length <= 11, `Range should have ~10-11 results, got ${results.length}`);
  });

  it('1000 keys survive heavy compaction', () => {
    const lsm = new LSMTree({ memtableThreshold: 50 });
    
    for (let i = 0; i < 1000; i++) {
      lsm.put(`key-${String(i).padStart(4, '0')}`, `val-${i}`);
    }
    
    lsm.flush();
    const stats = lsm.stats();
    
    // All 1000 keys should be readable
    let found = 0;
    for (let i = 0; i < 1000; i++) {
      if (lsm.get(`key-${String(i).padStart(4, '0')}`) !== undefined) found++;
    }
    assert.equal(found, 1000, 'All 1000 keys should survive compaction');
    
    console.log(`    1000 keys: ${stats.sstables} SSTables, ${stats.compactions} compactions`);
  });

  it('SSTable levels increase with compaction', () => {
    const lsm = new LSMTree({ memtableThreshold: 3 });
    
    for (let i = 0; i < 50; i++) {
      lsm.put(`key-${i}`, `val-${i}`);
    }
    
    lsm.flush();
    
    // Check that some SSTables have level > 0
    const stats = lsm.stats();
    const levels = lsm._sstables.map(s => s.level);
    const maxLevel = Math.max(...levels);
    
    assert.ok(maxLevel >= 1, `Max level should be >= 1, got ${maxLevel}`);
    console.log(`    Levels: ${JSON.stringify(levels)}, max: ${maxLevel}`);
  });
});
