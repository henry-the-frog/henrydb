// lsm.test.js — Tests for LSM tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LSMTree } from './lsm.js';

describe('LSMTree', () => {
  it('basic put and get', () => {
    const lsm = new LSMTree();
    lsm.put('name', 'Alice');
    assert.equal(lsm.get('name'), 'Alice');
  });

  it('overwrite key', () => {
    const lsm = new LSMTree();
    lsm.put('x', 1);
    lsm.put('x', 2);
    assert.equal(lsm.get('x'), 2);
  });

  it('delete key', () => {
    const lsm = new LSMTree();
    lsm.put('x', 42);
    assert.equal(lsm.get('x'), 42);
    lsm.delete('x');
    assert.equal(lsm.get('x'), null); // tombstone
  });

  it('get non-existent key', () => {
    const lsm = new LSMTree();
    assert.equal(lsm.get('missing'), undefined);
  });

  it('flush to SSTable on memtable full', () => {
    const lsm = new LSMTree({ memtableSize: 10 });
    for (let i = 0; i < 15; i++) {
      lsm.put(`key-${i}`, i);
    }
    
    const stats = lsm.getStats();
    assert.ok(stats.sstableCount > 0, 'Should have flushed to SSTable');
    
    // All values still readable
    for (let i = 0; i < 15; i++) {
      assert.equal(lsm.get(`key-${i}`), i);
    }
  });

  it('data survives multiple flushes', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    for (let i = 0; i < 50; i++) {
      lsm.put(`k${String(i).padStart(3, '0')}`, i);
    }
    
    // All values readable
    for (let i = 0; i < 50; i++) {
      assert.equal(lsm.get(`k${String(i).padStart(3, '0')}`), i);
    }
  });

  it('newer writes shadow older ones across SSTables', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    
    // Write v1
    for (let i = 0; i < 10; i++) lsm.put(`key-${i}`, 'v1');
    
    // Write v2 (triggers flush, so v1 is in SSTable, v2 in memtable/newer SSTable)
    for (let i = 0; i < 10; i++) lsm.put(`key-${i}`, 'v2');
    
    for (let i = 0; i < 10; i++) {
      assert.equal(lsm.get(`key-${i}`), 'v2');
    }
  });

  it('scan returns sorted key-value pairs', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    lsm.put('c', 3);
    lsm.put('a', 1);
    lsm.put('b', 2);
    lsm.put('e', 5);
    lsm.put('d', 4);
    
    const result = lsm.scan();
    assert.deepEqual(result, [
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'c', value: 3 },
      { key: 'd', value: 4 },
      { key: 'e', value: 5 },
    ]);
  });

  it('scan with range', () => {
    const lsm = new LSMTree();
    for (let i = 0; i < 10; i++) lsm.put(`key-${i}`, i);
    
    const result = lsm.scan('key-3', 'key-7');
    assert.equal(result.length, 4); // key-3 through key-6
    assert.equal(result[0].key, 'key-3');
    assert.equal(result[3].key, 'key-6');
  });

  it('scan excludes deleted keys', () => {
    const lsm = new LSMTree();
    lsm.put('a', 1);
    lsm.put('b', 2);
    lsm.put('c', 3);
    lsm.delete('b');
    
    const result = lsm.scan();
    assert.equal(result.length, 2);
    assert.deepEqual(result.map(r => r.key), ['a', 'c']);
  });

  it('compaction merges SSTables', () => {
    const lsm = new LSMTree({ memtableSize: 5 });
    
    // Generate enough data to trigger compaction
    for (let i = 0; i < 100; i++) {
      lsm.put(`key-${String(i).padStart(3, '0')}`, i);
    }
    
    lsm.compact();
    const stats = lsm.getStats();
    
    // After compaction, should have fewer SSTables
    assert.ok(stats.compactions > 0 || stats.sstableCount <= 5,
      `Expected compaction or few tables, got ${stats.sstableCount} tables, ${stats.compactions} compactions`);
    
    // All data still readable
    for (let i = 0; i < 100; i++) {
      assert.equal(lsm.get(`key-${String(i).padStart(3, '0')}`), i);
    }
  });

  it('Bloom filter accelerates negative lookups', () => {
    const lsm = new LSMTree({ memtableSize: 100 });
    
    // Insert 500 keys (triggers flushes)
    for (let i = 0; i < 500; i++) lsm.put(`exists-${i}`, i);
    
    // Force flush to get SSTables with Bloom filters
    lsm.compact();
    
    // Read 1000 non-existent keys
    for (let i = 0; i < 1000; i++) lsm.get(`missing-${i}`);
    
    const stats = lsm.getStats();
    assert.ok(stats.bloomSaves > 0, `Bloom filter should have saved reads, got ${stats.bloomSaves}`);
  });

  it('stats tracking', () => {
    const lsm = new LSMTree({ memtableSize: 10 });
    
    for (let i = 0; i < 20; i++) lsm.put(`k${i}`, i);
    for (let i = 0; i < 10; i++) lsm.get(`k${i}`);
    
    const stats = lsm.getStats();
    assert.equal(stats.writes, 20);
    assert.equal(stats.reads, 10);
    assert.ok(stats.memtableCapacity === 10);
  });
});

describe('LSMTree Stress', () => {
  it('10000 random operations maintain consistency', () => {
    const lsm = new LSMTree({ memtableSize: 100 });
    const reference = new Map(); // ground truth
    
    let seed = 42;
    const rng = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    
    for (let i = 0; i < 10000; i++) {
      const key = `key-${Math.floor(rng() * 500)}`;
      const op = rng();
      
      if (op < 0.6) {
        // Put
        const val = Math.floor(rng() * 10000);
        lsm.put(key, val);
        reference.set(key, val);
      } else if (op < 0.8) {
        // Get
        const expected = reference.get(key);
        const actual = lsm.get(key);
        if (expected !== undefined) {
          assert.equal(actual, expected, `Mismatch for ${key}`);
        }
      } else {
        // Delete
        lsm.delete(key);
        reference.delete(key);
      }
    }
    
    // Verify all remaining keys
    for (const [key, val] of reference) {
      assert.equal(lsm.get(key), val, `Final check failed for ${key}`);
    }
    
    const stats = lsm.getStats();
    console.log(`    Writes: ${stats.writes}, Reads: ${stats.reads}, Bloom saves: ${stats.bloomSaves}, Compactions: ${stats.compactions}`);
  });
});
