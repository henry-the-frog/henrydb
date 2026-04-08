// lsm-compaction.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LSMTree } from './lsm-compaction.js';

describe('LSMTree Compaction', () => {
  it('basic put/get', () => {
    const lsm = new LSMTree({ strategy: 'leveled', memtableLimit: 8 });
    lsm.put('a', 1);
    lsm.put('b', 2);
    lsm.put('c', 3);
    assert.equal(lsm.get('a'), 1);
    assert.equal(lsm.get('b'), 2);
    assert.equal(lsm.get('d'), undefined);
  });

  it('delete with tombstone', () => {
    const lsm = new LSMTree({ memtableLimit: 16 });
    lsm.put('x', 100);
    assert.equal(lsm.get('x'), 100);
    lsm.delete('x');
    assert.equal(lsm.get('x'), undefined);
  });

  it('update overwrites', () => {
    const lsm = new LSMTree({ memtableLimit: 16 });
    lsm.put('k', 'old');
    lsm.put('k', 'new');
    assert.equal(lsm.get('k'), 'new');
  });

  it('leveled compaction triggers', () => {
    const lsm = new LSMTree({ strategy: 'leveled', memtableLimit: 4, sizeTierThreshold: 2 });
    // Insert enough to trigger multiple flushes and compactions
    for (let i = 0; i < 40; i++) lsm.put(i, i * 10);

    assert.ok(lsm.stats.flushes >= 5);
    assert.ok(lsm.stats.compactions >= 1);
    // All data still accessible
    for (let i = 0; i < 40; i++) assert.equal(lsm.get(i), i * 10);
  });

  it('size-tiered compaction triggers', () => {
    const lsm = new LSMTree({ strategy: 'size-tiered', memtableLimit: 4, sizeTierThreshold: 3 });
    for (let i = 0; i < 30; i++) lsm.put(i, i);

    assert.ok(lsm.stats.compactions >= 1);
    for (let i = 0; i < 30; i++) assert.equal(lsm.get(i), i);
  });

  it('scan range', () => {
    const lsm = new LSMTree({ memtableLimit: 8 });
    for (let i = 0; i < 20; i++) lsm.put(i, i * 100);

    const range = [...lsm.scan(5, 10)];
    assert.equal(range.length, 6); // 5,6,7,8,9,10
    assert.equal(range[0].key, 5);
    assert.equal(range[0].value, 500);
  });

  it('data survives compaction', () => {
    const lsm = new LSMTree({ strategy: 'leveled', memtableLimit: 4, sizeTierThreshold: 2 });
    for (let i = 0; i < 100; i++) lsm.put(`key_${String(i).padStart(3, '0')}`, i);
    
    lsm.compact();
    
    for (let i = 0; i < 100; i++) {
      assert.equal(lsm.get(`key_${String(i).padStart(3, '0')}`), i);
    }
  });

  it('deletes are visible after flush', () => {
    const lsm = new LSMTree({ memtableLimit: 16 });
    for (let i = 0; i < 10; i++) lsm.put(i, i);
    lsm.put(5, 'five');
    lsm.delete(5);
    
    // Delete should be visible even without compaction
    assert.equal(lsm.get(5), undefined);
    assert.equal(lsm.get(3), 3);
    
    // After flush, still visible
    lsm._flush();
    assert.equal(lsm.get(5), undefined);
    assert.equal(lsm.get(3), 3);
  });

  it('stats tracking', () => {
    const lsm = new LSMTree({ memtableLimit: 4 });
    for (let i = 0; i < 20; i++) lsm.put(i, i);
    for (let i = 0; i < 5; i++) lsm.get(i);

    const stats = lsm.getStats();
    assert.equal(stats.writes, 20);
    assert.equal(stats.reads, 5);
    assert.ok(stats.flushes >= 1);
  });

  it('benchmark: 10K ops with leveled compaction', () => {
    const lsm = new LSMTree({ strategy: 'leveled', memtableLimit: 64 });
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) lsm.put(i, i);
    const writeMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) lsm.get(i);
    const readMs = Date.now() - t1;

    console.log(`    10K writes: ${writeMs}ms, 10K reads: ${readMs}ms, ${lsm.getStats().sstables}`);
  });
});
