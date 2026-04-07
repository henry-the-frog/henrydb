// catalog.test.js — Comprehensive catalog of all data structures in HenryDB
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// Import all data structures
import { BPlusTree } from './btree.js';
import { HashIndex } from './hash-index.js';
import { InvertedIndex } from './fulltext.js';
import { SkipList } from './skip-list.js';
import { BitmapIndex } from './bitmap-index.js';
import { RTree, Rect } from './rtree.js';
import { Trie } from './trie.js';
import { BloomFilter } from './bloom.js';
import { CountMinSketch, HyperLogLog } from './streaming.js';
import { ConsistentHashRing } from './consistent-hash.js';
import { BufferPool } from './buffer-pool.js';
import { LSMTree } from './lsm.js';
import { RaftCluster } from './raft.js';
import { LockManager, LockMode } from './lock-manager.js';
import { UnionFind, SortedSet } from './union-find.js';
import { DAG, WorkerPool } from './scheduler.js';
import { TDigest, SegmentTree } from './analytics.js';
import { FenwickTree, CuckooHashTable } from './fenwick.js';
import { RingBuffer } from './trie.js';
import { MinHeap, IntervalTree } from './interval-tree.js';
import { PlanCache } from './plan-cache.js';
import { WriteAheadLog, WALRecord, WAL_TYPES } from './wal.js';

describe('Data Structure Catalog', () => {
  describe('Index Structures', () => {
    it('B+Tree: sorted range queries', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 50; i++) tree.insert(i, `row_${i}`);
      assert.ok(tree.search(25));
    });

    it('Hash Index: O(1) equality', () => {
      const idx = new HashIndex();
      for (let i = 0; i < 100; i++) idx.insert(i, i);
      assert.equal(idx.find(42).length, 1);
    });

    it('Inverted Index: full-text search', () => {
      const idx = new InvertedIndex('idx', 'docs', 'content');
      idx.addDocument(0, 'database systems');
      idx.addDocument(1, 'web systems');
      assert.equal(idx.searchAnd('database systems').length, 1);
    });

    it('Skip List: probabilistic sorted index', () => {
      const sl = new SkipList();
      for (let i = 0; i < 100; i++) sl.insert(i, i);
      assert.equal(sl.find(50), 50);
      assert.equal(sl.range(10, 20).length, 11);
    });

    it('Bitmap Index: low-cardinality columns', () => {
      const idx = new BitmapIndex('idx', 'status');
      for (let i = 0; i < 100; i++) idx.addRow(i, i % 3 === 0 ? 'active' : 'inactive');
      assert.ok(idx.count('active') > 0);
    });

    it('R-Tree: spatial queries', () => {
      const tree = new RTree();
      tree.insert(Rect.point(5, 5), 'A');
      tree.insert(Rect.point(50, 50), 'B');
      assert.equal(tree.search(new Rect(0, 0, 10, 10)).length, 1);
    });

    it('Trie: prefix queries', () => {
      const trie = new Trie();
      trie.insert('database'); trie.insert('data');
      assert.equal(trie.findByPrefix('dat').length, 2);
    });

    it('Cuckoo Hash: O(1) worst-case', () => {
      const ht = new CuckooHashTable();
      for (let i = 0; i < 50; i++) ht.insert(i, i * 2);
      assert.equal(ht.get(25), 50);
    });
  });

  describe('Probabilistic Structures', () => {
    it('Bloom Filter: membership testing', () => {
      const bf = new BloomFilter(100);
      bf.add('exists');
      assert.equal(bf.mightContain('exists'), true);
      assert.equal(bf.mightContain('nope'), false);
    });

    it('Count-Min Sketch: frequency estimation', () => {
      const cms = new CountMinSketch();
      for (let i = 0; i < 100; i++) cms.add('hot');
      assert.ok(cms.estimate('hot') >= 100);
    });

    it('HyperLogLog: cardinality estimation', () => {
      const hll = new HyperLogLog(14);
      for (let i = 0; i < 1000; i++) hll.add(`item_${i}`);
      assert.ok(hll.estimate() > 800 && hll.estimate() < 1200);
    });

    it('T-Digest: quantile estimation', () => {
      const td = new TDigest();
      for (let i = 0; i < 1000; i++) td.add(i);
      assert.ok(td.p50() > 300 && td.p50() < 700);
    });
  });

  describe('Storage Engines', () => {
    it('LSM Tree: write-optimized storage', () => {
      const lsm = new LSMTree(50);
      for (let i = 0; i < 200; i++) lsm.put(i, `val_${i}`);
      assert.equal(lsm.get(100), 'val_100');
    });

    it('Buffer Pool: page cache', () => {
      const pool = new BufferPool(4);
      pool.fetchPage(1, id => ({ id }));
      pool.unpinPage(1);
      pool.fetchPage(1, id => ({ id })); // Cache hit
      assert.equal(pool.stats().hits, 1);
    });

    it('WAL: write-ahead logging', () => {
      const wal = new WriteAheadLog();
      wal.appendInsert(1, 'users', 0, 0, [1, 'Alice']);
      wal.appendCommit(1);
      assert.ok(wal.isCommitted(1));
    });

    it('Plan Cache: query plan reuse', () => {
      const cache = new PlanCache(10);
      cache.put('SELECT 1', { type: 'SELECT' });
      assert.ok(cache.get('SELECT 1'));
    });
  });

  describe('Distributed Primitives', () => {
    it('Consistent Hashing: data distribution', () => {
      const ring = new ConsistentHashRing(100);
      ring.addNode('A'); ring.addNode('B'); ring.addNode('C');
      assert.ok(ring.getNode('key'));
    });

    it('Raft Consensus: leader election', () => {
      const cluster = new RaftCluster(3);
      assert.ok(cluster.electLeader(0));
    });

    it('Lock Manager: deadlock detection', () => {
      const lm = new LockManager();
      lm.lock(1, 'r1', LockMode.EXCLUSIVE);
      lm.lock(2, 'r2', LockMode.EXCLUSIVE);
      lm.unlock(1);
      assert.equal(lm.lock(2, 'r1', LockMode.EXCLUSIVE), true);
    });
  });

  describe('Utility Structures', () => {
    it('Union-Find: connected components', () => {
      const uf = new UnionFind(5);
      uf.union(0, 1); uf.union(2, 3);
      assert.equal(uf.componentCount, 3);
    });

    it('Sorted Set: rank queries', () => {
      const ss = new SortedSet();
      [5, 2, 8, 1].forEach(v => ss.insert(v));
      assert.equal(ss.kth(0), 1);
      assert.equal(ss.rank(5), 2);
    });

    it('DAG: topological sort', () => {
      const dag = new DAG();
      dag.addNode('A'); dag.addNode('B', null, ['A']);
      assert.equal(dag.topologicalSort()[0].id, 'A');
    });

    it('Min-Heap: priority queue', () => {
      const heap = new MinHeap();
      heap.push(5); heap.push(1); heap.push(3);
      assert.equal(heap.pop(), 1);
    });

    it('Interval Tree: range overlap', () => {
      const tree = new IntervalTree();
      tree.insert(1, 5, 'A'); tree.insert(3, 8, 'B');
      assert.equal(tree.queryPoint(4).length, 2);
    });

    it('Segment Tree: range queries', () => {
      const st = new SegmentTree([1, 2, 3, 4, 5], 'sum');
      assert.equal(st.query(0, 4), 15);
    });

    it('Fenwick Tree: prefix sums', () => {
      const ft = new FenwickTree(5);
      [1, 2, 3, 4, 5].forEach((v, i) => ft.update(i, v));
      assert.equal(ft.rangeQuery(0, 4), 15);
    });

    it('Ring Buffer: circular logging', () => {
      const rb = new RingBuffer(3);
      rb.push('a'); rb.push('b'); rb.push('c'); rb.push('d');
      assert.equal(rb.latest(), 'd');
      assert.equal(rb.size, 3);
    });

    it('Worker Pool: task execution', () => {
      const pool = new WorkerPool(2);
      pool.submit('t1', () => 42);
      pool.executeAll();
      assert.equal(pool.getResult('t1').result, 42);
    });
  });
});
