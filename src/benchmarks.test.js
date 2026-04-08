// benchmarks.test.js — Comprehensive benchmark suite across all engines and data structures
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// Data structures
import { BPlusTree } from './bplus-tree.js';
import { SkipList } from './skip-list.js';
import { CuckooHashTable } from './cuckoo-hash.js';
import { RobinHoodHashTable } from './robin-hood-hash.js';
import { Trie } from './trie.js';
import { BinaryHeap, Quadtree } from './more-trees.js';
import { Treap, CuckooFilter } from './probabilistic-filters.js';
import { FenwickTree, SegmentTree, SuffixArray, UnionFind, DoubleHashTable } from './advanced-ds.js';
import { HyperLogLog } from './hyperloglog.js';
import { TDigest } from './tdigest.js';
import { CountMinSketch } from './count-min-sketch.js';
import { BitmapIndex } from './bitmap-index.js';
import { RingBuffer } from './ring-buffer.js';
import { LSMTree } from './lsm-compaction.js';
import { LinearHashTable } from './linear-hashing.js';
import { ExtendibleHashTable } from './extendible-hashing.js';
import { ConsistentHashRing } from './consistent-hashing.js';
import { LogHashTable } from './log-hash-table.js';
import { BufferPoolManager } from './buffer-pool.js';
import { ExpressionCompiler } from './expression-compiler.js';
import { kmpSearch, murmur3, LFUCache } from './more-ds.js';
import { batchLike, batchUpper } from './simd-string.js';
import { kWayMerge } from './merge-iterator.js';
import { rleEncode, deltaEncode, bitPackEncode, bitPackDecode } from './column-compression.js';

const N = 10000;

describe('Benchmark: Sorted structures (10K ops)', () => {
  for (const [name, create, insert, get] of [
    ['B+ Tree (order=64)', () => new BPlusTree(64), (t, k, v) => t.insert(k, v), (t, k) => t.get(k)],
    ['Skip List', () => new SkipList(), (t, k, v) => t.set(k, v), (t, k) => t.get(k)],
    ['Treap', () => new Treap(), (t, k, v) => t.insert(k, v), (t, k) => t.get(k)],
  ]) {
    it(`${name}: insert ${N}`, () => {
      const t = create();
      const t0 = Date.now();
      for (let i = 0; i < N; i++) insert(t, i, i);
      console.log(`    ${name} insert: ${Date.now() - t0}ms`);
      assert.equal(get(t, N - 1), N - 1);
    });
    it(`${name}: lookup ${N}`, () => {
      const t = create();
      for (let i = 0; i < N; i++) insert(t, i, i);
      const t0 = Date.now();
      for (let i = 0; i < N; i++) get(t, i);
      console.log(`    ${name} lookup: ${Date.now() - t0}ms`);
    });
  }
});

describe('Benchmark: Hash tables (10K ops)', () => {
  for (const [name, create] of [
    ['Cuckoo', () => new CuckooHashTable(N * 2)],
    ['Robin Hood', () => new RobinHoodHashTable(N * 2)],
    ['Double Hash', () => new DoubleHashTable(N * 2)],
    ['Linear Hash', () => new LinearHashTable()],
    ['Extendible Hash', () => new ExtendibleHashTable(8)],
    ['Log Hash', () => new LogHashTable()],
    ['JS Map', () => new Map()],
  ]) {
    it(`${name}: ${N} set+get`, () => {
      const ht = create();
      const t0 = Date.now();
      for (let i = 0; i < N; i++) ht.set(i, i);
      const setMs = Date.now() - t0;
      const t1 = Date.now();
      for (let i = 0; i < N; i++) ht.get(i);
      const getMs = Date.now() - t1;
      console.log(`    ${name}: set ${setMs}ms, get ${getMs}ms`);
    });
  }
});

describe('Benchmark: Probabilistic structures (100K ops)', () => {
  it('HyperLogLog: 100K adds', () => {
    const h = new HyperLogLog(12);
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) h.add(i);
    console.log(`    HLL: ${Date.now() - t0}ms, estimate: ${h.estimate()}`);
    assert.ok(Math.abs(h.estimate() - 100000) / 100000 < 0.05);
  });

  it('Count-Min Sketch: 100K adds', () => {
    const cms = new CountMinSketch(4096, 7);
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) cms.add(`key_${i % 1000}`);
    console.log(`    CMS: ${Date.now() - t0}ms`);
    assert.ok(cms.estimate('key_0') >= 90);
  });

  it('T-Digest: 100K adds', () => {
    const td = new TDigest(200);
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) td.add(Math.random() * 1000);
    console.log(`    T-Digest: ${Date.now() - t0}ms, centroids: ${td.centroidCount}`);
    assert.ok(td.percentile(50) > 400 && td.percentile(50) < 600);
  });

  it('Cuckoo Filter: 10K insert+contains', () => {
    const cf = new CuckooFilter(4096, 4);
    const t0 = Date.now();
    for (let i = 0; i < N; i++) cf.insert(`key_${i}`);
    for (let i = 0; i < N; i++) cf.contains(`key_${i}`);
    console.log(`    Cuckoo Filter: ${Date.now() - t0}ms`);
  });
});

describe('Benchmark: Bitmap operations (100K rows)', () => {
  it('build + query', () => {
    const idx = new BitmapIndex();
    const values = Array.from({ length: 100000 }, () => ['A', 'B', 'C', 'D'][Math.floor(Math.random() * 4)]);
    const t0 = Date.now();
    idx.build(values);
    const buildMs = Date.now() - t0;
    const t1 = Date.now();
    for (let i = 0; i < 1000; i++) idx.eq('A').and(idx.eq('B')); // Will always be empty (same col)
    const queryMs = Date.now() - t1;
    console.log(`    Bitmap: build ${buildMs}ms, 1K AND queries ${queryMs}ms`);
  });
});

describe('Benchmark: Tree-based structures', () => {
  it('Fenwick tree: 10K updates + queries', () => {
    const ft = FenwickTree.fromArray(new Array(N).fill(1));
    const t0 = Date.now();
    for (let i = 0; i < N; i++) ft.update(i % N, 1);
    for (let i = 0; i < N; i++) ft.prefixSum(i);
    console.log(`    Fenwick: ${Date.now() - t0}ms`);
  });

  it('Segment tree: 10K min queries', () => {
    const st = new SegmentTree(Array.from({ length: N }, () => Math.random() * 1000));
    const t0 = Date.now();
    for (let i = 0; i < N; i++) st.query(0, i);
    console.log(`    Segment tree: ${Date.now() - t0}ms`);
  });

  it('Trie: 10K prefix lookups', () => {
    const t = new Trie();
    for (let i = 0; i < N; i++) t.insert(`word_${i}`, i);
    const t0 = Date.now();
    for (let i = 0; i < N; i++) t.get(`word_${i}`);
    console.log(`    Trie lookup: ${Date.now() - t0}ms`);
  });
});

describe('Benchmark: String operations (100K)', () => {
  const col = Array.from({ length: 100000 }, (_, i) => `user_${i}_name`);
  
  it('LIKE pattern', () => {
    const t0 = Date.now();
    batchLike(col, '%500%');
    console.log(`    LIKE: ${Date.now() - t0}ms`);
  });
  
  it('UPPER', () => {
    const t0 = Date.now();
    batchUpper(col);
    console.log(`    UPPER: ${Date.now() - t0}ms`);
  });
  
  it('KMP search', () => {
    const text = 'x'.repeat(100000);
    const t0 = Date.now();
    kmpSearch(text, 'xxx');
    console.log(`    KMP: ${Date.now() - t0}ms`);
  });
  
  it('Murmur3: 100K hashes', () => {
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) murmur3(`key_${i}`);
    console.log(`    Murmur3: ${Date.now() - t0}ms`);
  });
});

describe('Benchmark: Compression', () => {
  it('RLE on sorted column', () => {
    const data = [];
    for (let i = 0; i < 10; i++) for (let j = 0; j < 10000; j++) data.push(i);
    const t0 = Date.now();
    const encoded = rleEncode(data);
    console.log(`    RLE: ${Date.now() - t0}ms, ${data.length} → ${encoded.length} runs`);
    assert.equal(encoded.length, 10);
  });

  it('Bit-packing on small integers', () => {
    const data = Array.from({ length: 100000 }, () => Math.floor(Math.random() * 16));
    const t0 = Date.now();
    const encoded = bitPackEncode(data);
    const decoded = bitPackDecode(encoded);
    console.log(`    Bit-pack: ${Date.now() - t0}ms`);
    assert.deepEqual(decoded, data);
  });

  it('Delta encoding', () => {
    const data = Array.from({ length: 100000 }, (_, i) => 1000000 + i);
    const t0 = Date.now();
    const encoded = deltaEncode(data);
    console.log(`    Delta: ${Date.now() - t0}ms`);
    assert.equal(encoded.base, 1000000);
  });
});

describe('Benchmark: K-way merge', () => {
  it('10-way merge of 10K each', () => {
    const arrays = Array.from({ length: 10 }, () =>
      Array.from({ length: N }, () => Math.floor(Math.random() * 1000000)).sort((a, b) => a - b)
    );
    const t0 = Date.now();
    const merged = kWayMerge(arrays);
    console.log(`    10-way merge 100K: ${Date.now() - t0}ms`);
    assert.equal(merged.length, 10 * N);
  });
});

describe('Benchmark: LSM Tree', () => {
  it('10K write + read', () => {
    const lsm = new LSMTree({ memtableLimit: 256 });
    const t0 = Date.now();
    for (let i = 0; i < N; i++) lsm.put(i, i);
    const writeMs = Date.now() - t0;
    const t1 = Date.now();
    for (let i = 0; i < N; i++) lsm.get(i);
    const readMs = Date.now() - t1;
    console.log(`    LSM: write ${writeMs}ms, read ${readMs}ms`);
  });
});

describe('Benchmark: Buffer Pool', () => {
  it('10K page accesses', () => {
    const bpm = new BufferPoolManager(128, 256);
    const pages = [];
    for (let i = 0; i < 500; i++) { const p = bpm.newPage(); bpm.unpinPage(p.pageId); pages.push(p.pageId); }
    
    const t0 = Date.now();
    for (let i = 0; i < N; i++) {
      const pid = pages[Math.floor(Math.pow(Math.random(), 2) * pages.length)];
      const p = bpm.fetchPage(pid);
      if (p) bpm.unpinPage(p.pageId);
    }
    console.log(`    Buffer pool: ${Date.now() - t0}ms, hit rate: ${bpm.getStats().hitRate}`);
  });
});

describe('Benchmark: Expression compiler', () => {
  it('compiled vs native filter on 100K rows', () => {
    const rows = Array.from({ length: 100000 }, (_, i) => ({ a: i, b: i % 10 }));
    const ec = new ExpressionCompiler();
    const { fn } = ec.compile({
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: { type: 'column', name: 'a' }, right: { type: 'literal', value: 50000 } },
      right: { type: 'COMPARE', op: 'EQ', left: { type: 'column', name: 'b' }, right: { type: 'literal', value: 3 } },
    });
    
    const t0 = Date.now();
    const compiled = rows.filter(fn);
    const compiledMs = Date.now() - t0;
    
    const t1 = Date.now();
    const native = rows.filter(r => r.a > 50000 && r.b === 3);
    const nativeMs = Date.now() - t1;
    
    console.log(`    Compiled: ${compiledMs}ms, Native: ${nativeMs}ms, Ratio: ${(compiledMs / (nativeMs || 1)).toFixed(1)}x`);
    assert.equal(compiled.length, native.length);
  });
});

describe('Benchmark: Consistent hashing', () => {
  it('100K key lookups with 10 nodes', () => {
    const ring = new ConsistentHashRing(150);
    for (let i = 0; i < 10; i++) ring.addNode(`node_${i}`);
    
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) ring.getNode(`key_${i}`);
    console.log(`    Consistent hash: ${Date.now() - t0}ms`);
  });
});

describe('Benchmark: Quadtree', () => {
  it('10K insert + 1K range queries', () => {
    const qt = new Quadtree(0, 0, 10000, 10000);
    const t0 = Date.now();
    for (let i = 0; i < N; i++) qt.insert({ x: Math.random() * 10000, y: Math.random() * 10000 });
    const buildMs = Date.now() - t0;
    
    const t1 = Date.now();
    for (let i = 0; i < 1000; i++) qt.query({ x: i * 10, y: i * 10, w: 100, h: 100 });
    const queryMs = Date.now() - t1;
    
    console.log(`    Quadtree: build ${buildMs}ms, 1K queries ${queryMs}ms`);
  });
});

describe('Benchmark: Ring buffer throughput', () => {
  it('10M pushes', () => {
    const rb = new RingBuffer(1000);
    const t0 = Date.now();
    for (let i = 0; i < 10000000; i++) rb.push(i);
    console.log(`    Ring buffer 10M: ${Date.now() - t0}ms`);
    assert.equal(rb.size, 1000);
  });
});

describe('Benchmark: LFU cache', () => {
  it('Zipf workload on 1K cache', () => {
    const cache = new LFUCache(1000);
    let hits = 0;
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) {
      const key = Math.floor(Math.pow(Math.random(), 2) * 5000);
      if (cache.get(key) !== undefined) hits++;
      else cache.set(key, key);
    }
    console.log(`    LFU Zipf: ${Date.now() - t0}ms, hit rate: ${(hits / 100000 * 100).toFixed(1)}%`);
  });
});

describe('Benchmark: Suffix array', () => {
  it('build + search', () => {
    const text = Array.from({ length: 2000 }, () => String.fromCharCode(97 + Math.floor(Math.random() * 26))).join('');
    const t0 = Date.now();
    const sa = new SuffixArray(text);
    const buildMs = Date.now() - t0;
    
    const t1 = Date.now();
    for (let i = 0; i < 100; i++) sa.search('abc');
    const searchMs = Date.now() - t1;
    
    console.log(`    Suffix array: build ${buildMs}ms, 100 searches ${searchMs}ms`);
  });
});

describe('Benchmark: Union-Find', () => {
  it('10K unions', () => {
    const uf = new UnionFind(N);
    const t0 = Date.now();
    for (let i = 0; i < N - 1; i++) uf.union(i, i + 1);
    console.log(`    Union-Find: ${Date.now() - t0}ms, components: ${uf.count}`);
    assert.equal(uf.count, 1);
  });
});
