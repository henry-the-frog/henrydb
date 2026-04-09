// sstable.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SSTable } from './sstable.js';

describe('SSTable', () => {
  const entries = Array.from({ length: 1000 }, (_, i) => ({ key: i, value: `val-${i}` }));

  it('point lookup', () => {
    const sst = new SSTable(entries, { blockSize: 64 });
    assert.equal(sst.get(500), 'val-500');
    assert.equal(sst.get(0), 'val-0');
    assert.equal(sst.get(999), 'val-999');
    assert.equal(sst.get(1000), undefined);
  });

  it('bloom filter rejects misses fast', () => {
    const sst = new SSTable(entries, { blockSize: 64 });
    // Non-existent keys should be rejected by bloom filter
    assert.equal(sst.get(5000), undefined);
  });

  it('range scan', () => {
    const sst = new SSTable(entries, { blockSize: 32 });
    const range = sst.range(100, 109);
    assert.equal(range.length, 10);
    assert.equal(range[0].key, 100);
    assert.equal(range[9].key, 109);
  });

  it('iterator', () => {
    const small = new SSTable([{ key: 1, value: 'a' }, { key: 2, value: 'b' }]);
    const all = [...small];
    assert.equal(all.length, 2);
  });

  it('merge two SSTables (compaction)', () => {
    const a = new SSTable([
      { key: 1, value: 'a1' }, { key: 3, value: 'a3' }, { key: 5, value: 'a5' },
    ]);
    const b = new SSTable([
      { key: 2, value: 'b2' }, { key: 3, value: 'b3' }, { key: 4, value: 'b4' },
    ]);
    
    const merged = SSTable.merge(a, b);
    assert.equal(merged.size, 5); // key 3 from a wins (first arg)
    assert.equal(merged.get(3), 'a3');
    assert.equal(merged.get(2), 'b2');
  });

  it('stats', () => {
    const sst = new SSTable(entries, { blockSize: 64 });
    const s = sst.getStats();
    assert.equal(s.entries, 1000);
    assert.equal(s.blocks, 16); // ceil(1000/64)
    assert.equal(s.hasBloomFilter, true);
  });

  it('performance: 100K entries', () => {
    const big = Array.from({ length: 100000 }, (_, i) => ({ key: i, value: i }));
    const sst = new SSTable(big, { blockSize: 128 });
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) sst.get(Math.floor(Math.random() * 100000));
    const elapsed = performance.now() - t0;
    
    console.log(`  10K random lookups in 100K SSTable: ${elapsed.toFixed(1)}ms (${(elapsed/10000*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 200);
  });

  it('empty SSTable', () => {
    const sst = new SSTable([]);
    assert.equal(sst.size, 0);
    assert.equal(sst.get(1), undefined);
    assert.deepEqual(sst.range(0, 10), []);
  });
});
