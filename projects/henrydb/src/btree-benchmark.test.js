// btree-benchmark.test.js — Benchmark: BTreeTable vs HeapFile
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BTreeTable } from './btree-table.js';
import { HeapFile } from './page.js';

const N = 10000;

function populateBTree(n = N) {
  const table = new BTreeTable('bench', { pkIndices: [0] });
  for (let i = 1; i <= n; i++) table.insert([i, `data-${i}`, i * 3]);
  return table;
}

function populateHeap(n = N) {
  const heap = new HeapFile('bench');
  const rids = [];
  for (let i = 1; i <= n; i++) rids.push({ rid: heap.insert([i, `data-${i}`, i * 3]), pk: i });
  return { heap, rids };
}

describe('BTreeTable vs HeapFile benchmarks', () => {
  it('sequential insert: 10K rows', () => {
    // BTreeTable
    const bt0 = performance.now();
    const btree = populateBTree();
    const btreeMs = performance.now() - bt0;

    // HeapFile
    const hf0 = performance.now();
    populateHeap();
    const heapMs = performance.now() - hf0;

    console.log(`  INSERT 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | ratio ${(btreeMs/heapMs).toFixed(2)}x`);
    assert.equal(btree.rowCount, N);
  });

  it('random insert (reverse order): 10K rows', () => {
    const btree = new BTreeTable('bench');
    const heap = new HeapFile('bench');

    const bt0 = performance.now();
    for (let i = N; i >= 1; i--) btree.insert([i, `data-${i}`, i]);
    const btreeMs = performance.now() - bt0;

    const hf0 = performance.now();
    for (let i = N; i >= 1; i--) heap.insert([i, `data-${i}`, i]);
    const heapMs = performance.now() - hf0;

    console.log(`  REVERSE INSERT 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | ratio ${(btreeMs/heapMs).toFixed(2)}x`);
    
    // BTree should be sorted despite reverse insert
    const keys = [...btree.scan()].map(r => r.values[0]);
    assert.equal(keys[0], 1);
    assert.equal(keys[keys.length - 1], N);
  });

  it('point lookup by PK: 1000 random lookups in 10K', () => {
    const btree = populateBTree();
    const { heap, rids } = populateHeap();

    // BTree: O(log n) via findByPK
    const bt0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      const pk = Math.floor(Math.random() * N) + 1;
      btree.findByPK(pk);
    }
    const btreeMs = performance.now() - bt0;

    // HeapFile: O(n) via full scan
    const hf0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      const pk = Math.floor(Math.random() * N) + 1;
      // HeapFile has no PK index — must scan
      for (const { values } of heap.scan()) {
        if (values[0] === pk) break;
      }
    }
    const heapMs = performance.now() - hf0;

    const speedup = heapMs / btreeMs;
    console.log(`  POINT LOOKUP 1K in 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | speedup ${speedup.toFixed(1)}x`);
    assert.ok(speedup > 5, `Expected >5x speedup, got ${speedup.toFixed(1)}x`);
  });

  it('range scan: 2000 rows from 10K', () => {
    const btree = populateBTree();
    const { heap } = populateHeap();

    // BTree: O(log n + k) via rangeScan
    const bt0 = performance.now();
    const btreeResults = [...btree.rangeScan(4000, 6000)];
    const btreeMs = performance.now() - bt0;

    // HeapFile: O(n) full scan with filter
    const hf0 = performance.now();
    const heapResults = [];
    for (const { values } of heap.scan()) {
      if (values[0] >= 4000 && values[0] <= 6000) heapResults.push(values);
    }
    const heapMs = performance.now() - hf0;

    console.log(`  RANGE SCAN 2K from 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | speedup ${(heapMs/btreeMs).toFixed(1)}x`);
    assert.equal(btreeResults.length, 2001);
    assert.equal(heapResults.length, 2001);
  });

  it('full table scan: 10K rows', () => {
    const btree = populateBTree();
    const { heap } = populateHeap();

    // BTree scan (sorted)
    const bt0 = performance.now();
    let btreeCount = 0;
    for (const _ of btree.scan()) btreeCount++;
    const btreeMs = performance.now() - bt0;

    // Heap scan (unsorted)
    const hf0 = performance.now();
    let heapCount = 0;
    for (const _ of heap.scan()) heapCount++;
    const heapMs = performance.now() - hf0;

    console.log(`  FULL SCAN 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | ratio ${(btreeMs/heapMs).toFixed(2)}x`);
    assert.equal(btreeCount, N);
    assert.equal(heapCount, N);
  });

  it('delete 50% of rows: 5K deletes from 10K', () => {
    const btree = populateBTree();
    const { heap, rids } = populateHeap();

    // BTree: delete by PK
    const bt0 = performance.now();
    for (let i = 2; i <= N; i += 2) btree.deleteByPK(i);
    const btreeMs = performance.now() - bt0;

    // Heap: delete by rid
    const hf0 = performance.now();
    for (const { rid, pk } of rids) {
      if (pk % 2 === 0) heap.delete(rid.pageId, rid.slotIdx);
    }
    const heapMs = performance.now() - hf0;

    console.log(`  DELETE 5K from 10K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms | ratio ${(btreeMs/heapMs).toFixed(2)}x`);
    assert.equal(btree.rowCount, N / 2);
  });

  it('ORDER BY PK: BTree is already sorted (no sort needed)', () => {
    const btree = populateBTree();

    // BTree scan is already in order — no sort needed
    const t0 = performance.now();
    const sorted = [...btree.scan()].map(r => r.values[0]);
    const btreeMs = performance.now() - t0;

    // HeapFile scan + sort
    const { heap } = populateHeap();
    const t1 = performance.now();
    const unsorted = [...heap.scan()].map(r => r.values[0]);
    unsorted.sort((a, b) => a - b);
    const heapMs = performance.now() - t1;

    console.log(`  ORDER BY PK 10K: BTree ${btreeMs.toFixed(1)}ms (pre-sorted) | Heap+Sort ${heapMs.toFixed(1)}ms | speedup ${(heapMs/btreeMs).toFixed(1)}x`);
    assert.deepEqual(sorted[0], 1);
    assert.deepEqual(sorted[sorted.length - 1], N);
  });

  it('summary: 10K row benchmark table', () => {
    console.log('\n  ╔══════════════════════════════════════════╗');
    console.log('  ║  BTreeTable vs HeapFile — 10K Rows       ║');
    console.log('  ╠══════════════════════════════════════════╣');
    console.log('  ║  BTree wins: point lookup, range scan,   ║');
    console.log('  ║              ORDER BY PK                 ║');
    console.log('  ║  HeapFile wins: sequential insert,       ║');
    console.log('  ║                 full scan (no overhead)  ║');
    console.log('  ╚══════════════════════════════════════════╝');
    assert.ok(true);
  });
});
