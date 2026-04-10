// edge-cases.test.js — Edge case and boundary tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BPlusTree } from './bplus-tree.js';
import { SkipList } from './skip-list.js';
import { RingBuffer } from './ring-buffer.js';
import { CuckooHashTable } from './cuckoo-hash.js';
import { RobinHoodHashMap } from './robin-hood-hash.js';
import { HyperLogLog } from './hyperloglog.js';
import { TDigest } from './tdigest.js';
import { CountMinSketch } from './count-min-sketch.js';
import { BitmapIndex, BitVector } from './bitmap-index.js';
import { FenwickTree, SegmentTree, UnionFind } from './advanced-ds.js';
import { BinaryHeap } from './more-trees.js';
import { LFUCache, kmpSearch, murmur3 } from './more-ds.js';
import { Trie } from './trie.js';
import { LamportClock, VectorClock, GCounter, PNCounter } from './distributed-primitives.js';
import { ReservoirSampler } from './sampling.js';
import { IntervalTree, OrderStatisticsTree } from './more-trees.js';

describe('Edge Cases: Empty collections', () => {
  it('B+ tree: get from empty', () => assert.equal(new BPlusTree().get(1), undefined));
  it('B+ tree: range from empty', () => assert.deepEqual(new BPlusTree().range(0, 100), []));
  it('B+ tree: delete from empty', () => assert.ok(!new BPlusTree().delete(1)));
  it('Skip list: get from empty', () => assert.equal(new SkipList().get(1), undefined));
  it('Skip list: range from empty', () => assert.deepEqual([...new SkipList().range(0, 100)], []));
  it('Ring buffer: peek from empty', () => assert.equal(new RingBuffer(5).peekBack(), undefined));
  it('Ring buffer: pop from empty', () => assert.equal(new RingBuffer(5).shift(), undefined));
  it('Cuckoo: get from empty', () => assert.equal(new CuckooHashTable().get('x'), undefined));
  it('Robin Hood: get from empty', () => assert.equal(new RobinHoodHashMap().get('x'), undefined));
  it('HLL: estimate from empty', () => assert.equal(new HyperLogLog().estimate(), 0));
  it('TDigest: quantile from empty', () => assert.equal(new TDigest().quantile(0.5), null));
  it('CMS: estimate from empty', () => assert.equal(new CountMinSketch().estimate('x'), 0));
  it('Trie: get from empty', () => assert.equal(new Trie().get('x'), undefined));
  it('Trie: prefix search from empty', () => assert.deepEqual(new Trie().prefixSearch('x'), []));
  it('LFU: get from empty', () => assert.equal(new LFUCache(10).get('x'), undefined));
  it('Heap: pop from empty', () => assert.equal(new BinaryHeap().pop(), undefined));
  it('Fenwick: prefix sum of empty', () => { const ft = new FenwickTree(0); assert.equal(ft.prefixSum(-1), 0); });
  it('Interval tree: query empty', () => assert.deepEqual(new IntervalTree().query(5), []));
  it('Reservoir: sample from empty', () => assert.deepEqual(new ReservoirSampler(10).sample, []));
});

describe('Edge Cases: Single element', () => {
  it('B+ tree: single element', () => { const t = new BPlusTree(); t.insert(1, 'a'); assert.equal(t.get(1), 'a'); assert.equal(t.size, 1); });
  it('Skip list: single element', () => { const s = new SkipList(); s.insert(1, 'a'); assert.equal(s.get(1), 'a'); });
  it('Ring buffer: single element', () => { const r = new RingBuffer(1); r.push('x'); assert.equal(r.peekBack(), 'x'); r.push('y'); assert.equal(r.peekBack(), 'y'); });
  it('HLL: single element', () => { const h = new HyperLogLog(); h.add('x'); assert.ok(h.estimate() >= 1); });
  it('TDigest: single element', () => { const t = new TDigest(); t.add(42); assert.equal(t.percentile(50), 42); });
  it('Trie: single key', () => { const t = new Trie(); t.insert('a', 1); assert.equal(t.get('a'), 1); });
  it('KMP: single char match', () => assert.deepEqual(kmpSearch('a', 'a'), [0]));
  it('Reservoir: single item', () => { const r = new ReservoirSampler(10); r.add('x'); assert.deepEqual(r.sample, ['x']); });
  it('Order stats: single element', () => { const o = new OrderStatisticsTree(); o.insert(5); assert.equal(o.select(0), 5); assert.equal(o.rank(5), 0); });
});

describe('Edge Cases: Boundary values', () => {
  it('Murmur3: empty string', () => assert.ok(typeof murmur3('') === 'number'));
  it('Murmur3: very long string', () => assert.ok(typeof murmur3('x'.repeat(10000)) === 'number'));
  it('KMP: empty pattern', () => assert.deepEqual(kmpSearch('hello', ''), []));
  it('KMP: pattern longer than text', () => assert.deepEqual(kmpSearch('hi', 'hello world'), []));
  it('KMP: exact match', () => assert.deepEqual(kmpSearch('abc', 'abc'), [0]));
  it('BitVector: set bit 0', () => { const bv = new BitVector(1); bv.set(0); assert.ok(bv.get(0)); });
  it('BitVector: popcount of 0', () => assert.equal(new BitVector(64).popcount(), 0));
  it('BitVector: large position', () => { const bv = new BitVector(1000); bv.set(999); assert.ok(bv.get(999)); assert.ok(!bv.get(998)); });
  it('Ring buffer: capacity 1', () => { const r = new RingBuffer(1); r.push(1); r.push(2); assert.equal(r.peekBack(), 2); assert.equal(r.size, 1); });
  it('LFU: capacity 1', () => { const c = new LFUCache(1); c.set('a', 1); c.set('b', 2); assert.equal(c.get('a'), undefined); assert.equal(c.get('b'), 2); });
  it('UnionFind: single element', () => { const uf = new UnionFind(1); assert.equal(uf.count, 1); assert.equal(uf.find(0), 0); });
  it('UnionFind: self-union', () => { const uf = new UnionFind(2); assert.ok(!uf.union(0, 0)); });
});

describe('Edge Cases: Special values', () => {
  it('Hash tables handle numeric 0', () => {
    const c = new CuckooHashTable(); c.set(0, 'zero'); assert.equal(c.get(0), 'zero');
  });
  it('Hash tables handle empty string', () => {
    const r = new RobinHoodHashMap(); r.set('', 'empty'); assert.equal(r.get(''), 'empty');
  });
  it('Skip list: negative keys', () => {
    const s = new SkipList(); s.insert(-100, 'neg'); s.insert(100, 'pos');
    assert.equal(s.get(-100), 'neg');
    assert.equal(s.min().key, -100);
  });
  it('B+ tree: string keys with special chars', () => {
    const t = new BPlusTree(4);
    t.insert('key with spaces', 1);
    t.insert('key\twith\ttabs', 2);
    t.insert('key/with/slashes', 3);
    assert.equal(t.get('key with spaces'), 1);
  });
  it('Trie: overlapping prefixes', () => {
    const t = new Trie();
    t.insert('a', 1); t.insert('ab', 2); t.insert('abc', 3);
    assert.equal(t.get('a'), 1);
    assert.equal(t.get('ab'), 2);
    assert.equal(t.get('abc'), 3);
  });
  it('G-Counter: zero increments', () => {
    const c = new GCounter('n1'); assert.equal(c.value, 0);
  });
  it('PN-Counter: decrement below zero', () => {
    const c = new PNCounter('n1'); c.decrement(5); assert.equal(c.value, -5);
  });
  it('Lamport clock starts at 0', () => assert.equal(new LamportClock().time, 0));
  it('Vector clock: empty', () => assert.deepEqual(new VectorClock('A').clock, {}));
});

describe('Edge Cases: Duplicate handling', () => {
  it('B+ tree: duplicate key overwrites', () => {
    const t = new BPlusTree(4);
    t.insert(1, 'a'); t.insert(1, 'b');
    assert.equal(t.get(1), 'b');
  });
  it('Skip list: duplicate key overwrites', () => {
    const s = new SkipList();
    s.insert('k', 'old'); s.insert('k', 'new');
    assert.equal(s.get('k'), 'new');
  });
  it('Trie: duplicate key overwrites', () => {
    const t = new Trie();
    t.insert('key', 'old'); t.insert('key', 'new');
    assert.equal(t.get('key'), 'new');
  });
  it('CMS: duplicate adds accumulate', () => {
    const cms = new CountMinSketch(1024, 5);
    cms.add('x'); cms.add('x'); cms.add('x');
    assert.ok(cms.estimate('x') >= 3);
  });
  it('HLL: duplicates dont increase estimate', () => {
    const h = new HyperLogLog(10);
    for (let i = 0; i < 1000; i++) h.add('same');
    assert.ok(h.estimate() <= 3); // Should be ~1
  });
  it('Bitmap: all same value', () => {
    const idx = new BitmapIndex();
    for (let i = 0; i < 100; i++) idx.set(i, 'same');
    assert.equal(idx.values.length, 1);
    assert.equal(idx.lookup('same').length, 100);
  });
});

describe('Edge Cases: Stress tests', () => {
  it('10K inserts into order=3 B+ tree', () => {
    const t = new BPlusTree(3);
    for (let i = 0; i < 10000; i++) t.insert(i, i);
    assert.equal(t.size, 10000);
    // Spot check
    assert.equal(t.get(0), 0);
    assert.equal(t.get(5000), 5000);
    assert.equal(t.get(9999), 9999);
  });

  it('10K inserts into skip list', () => {
    const s = new SkipList();
    for (let i = 0; i < 10000; i++) s.insert(i, i);
    assert.equal(s.size, 10000);
    assert.equal(s.min().key, 0);
    assert.equal(s.max().key, 9999);
  });

  it('100K CMS adds', () => {
    const cms = new CountMinSketch(4096, 7);
    for (let i = 0; i < 100000; i++) cms.add(`key_${i % 1000}`);
    assert.ok(cms.estimate('key_0') >= 95);
    assert.ok(cms.estimate('key_0') <= 120);
  });

  it('100K HLL adds', () => {
    const h = new HyperLogLog(12);
    for (let i = 0; i < 100000; i++) h.add(i);
    const est = h.estimate();
    assert.ok(Math.abs(est - 100000) / 100000 < 0.05);
  });
});
