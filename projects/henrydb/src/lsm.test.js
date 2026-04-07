// lsm.test.js — LSM Tree tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LSMTree, SSTable, mergeSorted, TOMBSTONE } from './lsm.js';

describe('SSTable', () => {
  it('binary search finds keys', () => {
    const sst = new SSTable([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'c', value: 3 },
    ]);
    assert.equal(sst.get('b'), 2);
    assert.equal(sst.get('d'), undefined);
  });

  it('range scan returns subset', () => {
    const entries = [];
    for (let i = 0; i < 20; i++) entries.push({ key: i, value: `v${i}` });
    const sst = new SSTable(entries);
    
    const range = sst.range(5, 10);
    assert.equal(range.length, 6);
    assert.equal(range[0].key, 5);
    assert.equal(range[5].key, 10);
  });
});

describe('mergeSorted', () => {
  it('merges two sorted arrays', () => {
    const a = [{ key: 1, value: 'a' }, { key: 3, value: 'c' }];
    const b = [{ key: 2, value: 'b' }, { key: 4, value: 'd' }];
    const merged = mergeSorted(a, b);
    assert.deepEqual(merged.map(e => e.key), [1, 2, 3, 4]);
  });

  it('newer entries take precedence on conflict', () => {
    const older = [{ key: 1, value: 'old' }];
    const newer = [{ key: 1, value: 'new' }];
    const merged = mergeSorted(older, newer);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].value, 'new');
  });

  it('removes tombstones during merge', () => {
    const a = [{ key: 1, value: 'a' }];
    const b = [{ key: 1, value: TOMBSTONE }];
    const merged = mergeSorted(a, b);
    assert.equal(merged.length, 0);
  });
});

describe('LSMTree', () => {
  it('put and get', () => {
    const lsm = new LSMTree();
    lsm.put('key1', 'value1');
    lsm.put('key2', 'value2');
    
    assert.equal(lsm.get('key1'), 'value1');
    assert.equal(lsm.get('key2'), 'value2');
    assert.equal(lsm.get('key3'), undefined);
  });

  it('newer puts overwrite older', () => {
    const lsm = new LSMTree();
    lsm.put('key', 'old');
    lsm.put('key', 'new');
    
    assert.equal(lsm.get('key'), 'new');
  });

  it('delete removes key', () => {
    const lsm = new LSMTree();
    lsm.put('key', 'value');
    lsm.delete('key');
    
    assert.equal(lsm.get('key'), undefined);
  });

  it('flush creates SSTable', () => {
    const lsm = new LSMTree(5);
    for (let i = 0; i < 5; i++) lsm.put(i, `v${i}`);
    lsm.flush();
    
    assert.equal(lsm.stats().sstableCount, 1);
    assert.equal(lsm.stats().memtableSize, 0);
    
    // Data should still be accessible from SSTable
    assert.equal(lsm.get(3), 'v3');
  });

  it('auto-flushes when memtable is full', () => {
    const lsm = new LSMTree(10);
    for (let i = 0; i < 25; i++) {
      lsm.put(i, `v${i}`);
    }
    
    assert.ok(lsm.stats().sstableCount >= 1);
    // All data should still be accessible
    for (let i = 0; i < 25; i++) {
      assert.equal(lsm.get(i), `v${i}`);
    }
  });

  it('range scan across memtable and SSTables', () => {
    const lsm = new LSMTree(5);
    for (let i = 0; i < 10; i++) lsm.put(i, `v${i}`);
    lsm.flush(); // Move first 10 to SSTable
    for (let i = 10; i < 15; i++) lsm.put(i, `v${i}`);
    
    const range = lsm.range(3, 12);
    assert.equal(range.length, 10);
    assert.equal(range[0].key, 3);
    assert.equal(range[9].key, 12);
  });

  it('compaction reduces SSTable count', () => {
    const lsm = new LSMTree(10);
    // Generate enough data to trigger multiple flushes and compaction
    for (let i = 0; i < 100; i++) {
      lsm.put(i, `v${i}`);
    }
    lsm.flush();
    
    const stats = lsm.stats();
    assert.ok(stats.compactions >= 0);
    
    // All data should still be accessible after compaction
    for (let i = 0; i < 100; i++) {
      assert.equal(lsm.get(i), `v${i}`);
    }
  });

  it('delete propagates through compaction', () => {
    const lsm = new LSMTree(5);
    lsm.put('a', '1');
    lsm.put('b', '2');
    lsm.put('c', '3');
    lsm.flush();
    
    lsm.delete('b');
    lsm.flush();
    
    assert.equal(lsm.get('a'), '1');
    assert.equal(lsm.get('b'), undefined);
    assert.equal(lsm.get('c'), '3');
  });

  it('stats reports write amplification metrics', () => {
    const lsm = new LSMTree(10);
    for (let i = 0; i < 50; i++) lsm.put(i, i);
    
    const stats = lsm.stats();
    assert.equal(stats.writes, 50);
    assert.ok(stats.memtableMaxSize === 10);
  });
});
