// btree-table.test.js — Tests for clustered B+tree table storage
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BTreeTable } from './btree-table.js';

describe('BTreeTable', () => {
  it('insert and get by rid', () => {
    const table = new BTreeTable('users');
    const rid1 = table.insert([1, 'Alice', 30]);
    const rid2 = table.insert([2, 'Bob', 25]);
    const rid3 = table.insert([3, 'Charlie', 35]);

    assert.deepEqual(table.get(rid1.pageId, rid1.slotIdx), [1, 'Alice', 30]);
    assert.deepEqual(table.get(rid2.pageId, rid2.slotIdx), [2, 'Bob', 25]);
    assert.deepEqual(table.get(rid3.pageId, rid3.slotIdx), [3, 'Charlie', 35]);
    assert.equal(table.rowCount, 3);
  });

  it('scan returns rows in PK order', () => {
    const table = new BTreeTable('users');
    // Insert out of order
    table.insert([3, 'Charlie', 35]);
    table.insert([1, 'Alice', 30]);
    table.insert([2, 'Bob', 25]);

    const rows = [...table.scan()].map(r => r.values);
    assert.deepEqual(rows, [
      [1, 'Alice', 30],
      [2, 'Bob', 25],
      [3, 'Charlie', 35],
    ]);
  });

  it('delete by rid', () => {
    const table = new BTreeTable('t');
    const rid1 = table.insert([1, 'a']);
    const rid2 = table.insert([2, 'b']);
    const rid3 = table.insert([3, 'c']);

    assert.equal(table.delete(rid2.pageId, rid2.slotIdx), true);
    assert.equal(table.rowCount, 2);
    assert.equal(table.get(rid2.pageId, rid2.slotIdx), null);

    const rows = [...table.scan()].map(r => r.values);
    assert.deepEqual(rows, [[1, 'a'], [3, 'c']]);
  });

  it('delete by PK', () => {
    const table = new BTreeTable('t');
    table.insert([10, 'x']);
    table.insert([20, 'y']);
    table.insert([30, 'z']);

    assert.equal(table.deleteByPK(20), true);
    assert.equal(table.deleteByPK(99), false);
    assert.equal(table.rowCount, 2);

    const rows = [...table.scan()].map(r => r.values[0]);
    assert.deepEqual(rows, [10, 30]);
  });

  it('upsert on duplicate PK', () => {
    const table = new BTreeTable('t');
    const rid1 = table.insert([1, 'Alice', 30]);
    const rid2 = table.insert([1, 'Alice', 31]); // Same PK, updated age

    assert.equal(table.rowCount, 1);
    const values = table.get(rid1.pageId, rid1.slotIdx);
    assert.deepEqual(values, [1, 'Alice', 31]);
  });

  it('findByPK point lookup', () => {
    const table = new BTreeTable('t');
    table.insert([100, 'Alice']);
    table.insert([200, 'Bob']);
    table.insert([300, 'Charlie']);

    assert.deepEqual(table.findByPK(200), [200, 'Bob']);
    assert.equal(table.findByPK(999), null);
  });

  it('lookupByPK returns full result with rid', () => {
    const table = new BTreeTable('t');
    table.insert([1, 'a']);
    table.insert([2, 'b']);

    const result = table.lookupByPK(2);
    assert.ok(result);
    assert.deepEqual(result.values, [2, 'b']);
    assert.equal(typeof result.pageId, 'number');
    assert.equal(typeof result.slotIdx, 'number');
  });

  it('rangeScan returns ordered subset', () => {
    const table = new BTreeTable('t');
    for (let i = 1; i <= 10; i++) {
      table.insert([i, `row-${i}`]);
    }

    const results = [...table.rangeScan(3, 7)].map(r => r.values[0]);
    assert.deepEqual(results, [3, 4, 5, 6, 7]);
  });

  it('minKey and maxKey', () => {
    const table = new BTreeTable('t');
    table.insert([50, 'mid']);
    table.insert([10, 'low']);
    table.insert([90, 'high']);

    assert.equal(table.minKey().key, 10);
    assert.equal(table.maxKey().key, 90);
  });

  it('composite primary key', () => {
    const table = new BTreeTable('t', { pkIndices: [0, 1] });
    table.insert(['US', 'CA', 'California']);
    table.insert(['US', 'TX', 'Texas']);
    table.insert(['CA', 'ON', 'Ontario']);

    assert.equal(table.rowCount, 3);
    const rows = [...table.scan()].map(r => r.values[2]);
    // Should be sorted by composite key: CA\0ON < US\0CA < US\0TX
    assert.deepEqual(rows, ['Ontario', 'California', 'Texas']);
  });

  it('large insert maintains order', () => {
    const table = new BTreeTable('t');
    const N = 1000;
    // Insert in reverse order
    for (let i = N; i >= 1; i--) {
      table.insert([i, `val-${i}`]);
    }

    assert.equal(table.rowCount, N);

    // Scan should be in ascending order
    const keys = [...table.scan()].map(r => r.values[0]);
    for (let i = 0; i < keys.length - 1; i++) {
      assert.ok(keys[i] < keys[i + 1], `keys[${i}]=${keys[i]} should be < keys[${i + 1}]=${keys[i + 1]}`);
    }
  });

  it('stress: insert, delete, scan consistency', () => {
    const table = new BTreeTable('stress');
    const N = 500;

    // Insert N rows
    for (let i = 1; i <= N; i++) {
      table.insert([i, `data-${i}`]);
    }
    assert.equal(table.rowCount, N);

    // Delete even rows
    for (let i = 2; i <= N; i += 2) {
      table.deleteByPK(i);
    }
    assert.equal(table.rowCount, N / 2);

    // Scan: should only have odd rows, in order
    const keys = [...table.scan()].map(r => r.values[0]);
    assert.equal(keys.length, N / 2);
    for (let i = 0; i < keys.length; i++) {
      assert.equal(keys[i], i * 2 + 1);
    }
  });

  it('HeapFile compatibility: scan yields pageId/slotIdx/values', () => {
    const table = new BTreeTable('compat');
    table.insert([1, 'a']);
    table.insert([2, 'b']);

    for (const entry of table.scan()) {
      assert.ok('pageId' in entry);
      assert.ok('slotIdx' in entry);
      assert.ok('values' in entry);
      assert.ok(Array.isArray(entry.values));
    }
  });

  it('getStats returns btree info', () => {
    const table = new BTreeTable('t');
    for (let i = 1; i <= 100; i++) table.insert([i, 'x']);

    const stats = table.getStats();
    assert.equal(stats.engine, 'btree');
    assert.equal(stats.rows, 100);
    assert.equal(stats.order, 64);
  });

  it('empty table scan', () => {
    const table = new BTreeTable('empty');
    const rows = [...table.scan()];
    assert.equal(rows.length, 0);
    assert.equal(table.rowCount, 0);
    assert.equal(table.pageCount, 1);
  });

  it('string primary keys', () => {
    const table = new BTreeTable('dict');
    table.insert(['banana', 2]);
    table.insert(['apple', 1]);
    table.insert(['cherry', 3]);

    const keys = [...table.scan()].map(r => r.values[0]);
    assert.deepEqual(keys, ['apple', 'banana', 'cherry']);
    assert.deepEqual(table.findByPK('banana'), ['banana', 2]);
  });

  it('delete non-existent returns false', () => {
    const table = new BTreeTable('t');
    table.insert([1, 'x']);
    assert.equal(table.delete(99, 99), false);
    assert.equal(table.deleteByPK(999), false);
  });

  it('get non-existent returns null', () => {
    const table = new BTreeTable('t');
    assert.equal(table.get(0, 0), null);
    assert.equal(table.findByPK(999), null);
    assert.equal(table.lookupByPK(999), null);
  });
});

describe('BTreeTable vs HeapFile comparison', () => {
  it('scan order differs: BTreeTable sorted, HeapFile insertion-order', async () => {
    // This is the fundamental semantic difference
    const { HeapFile } = await import('./page.js');
    const heap = new HeapFile('heap-test');
    const btree = new BTreeTable('btree-test');

    const data = [[3, 'c'], [1, 'a'], [2, 'b']];
    for (const row of data) {
      heap.insert(row);
      btree.insert(row);
    }

    const heapOrder = [...heap.scan()].map(r => r.values[0]);
    const btreeOrder = [...btree.scan()].map(r => r.values[0]);

    // HeapFile: insertion order
    assert.deepEqual(heapOrder, [3, 1, 2]);
    // BTreeTable: sorted by PK
    assert.deepEqual(btreeOrder, [1, 2, 3]);
  });

  it('BTreeTable point lookup is O(log n) vs HeapFile O(n)', () => {
    const btree = new BTreeTable('bench');
    const N = 10000;
    for (let i = 1; i <= N; i++) {
      btree.insert([i, `data-${i}`]);
    }

    // Point lookup should be fast
    const t0 = performance.now();
    for (let i = 1; i <= 1000; i++) {
      const key = Math.floor(Math.random() * N) + 1;
      btree.findByPK(key);
    }
    const elapsed = performance.now() - t0;
    
    // 1000 lookups in 10K rows should be well under 100ms
    assert.ok(elapsed < 100, `1000 point lookups took ${elapsed.toFixed(1)}ms (expected <100ms)`);
  });

  it('BTreeTable range scan is efficient', () => {
    const btree = new BTreeTable('bench');
    const N = 10000;
    for (let i = 1; i <= N; i++) {
      btree.insert([i, `data-${i}`]);
    }

    const t0 = performance.now();
    const results = [...btree.rangeScan(4000, 6000)];
    const elapsed = performance.now() - t0;

    assert.equal(results.length, 2001); // 4000..6000 inclusive
    assert.ok(elapsed < 50, `Range scan 2001 rows took ${elapsed.toFixed(1)}ms`);
  });
});
