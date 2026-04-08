// advanced-ds.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FenwickTree, SegmentTree, UnionFind, SuffixArray, DoubleHashTable } from './advanced-ds.js';

describe('FenwickTree', () => {
  it('prefix sum', () => {
    const ft = FenwickTree.fromArray([1, 2, 3, 4, 5]);
    assert.equal(ft.prefixSum(2), 6); // 1+2+3
    assert.equal(ft.prefixSum(4), 15); // 1+2+3+4+5
  });
  it('range sum', () => {
    const ft = FenwickTree.fromArray([1, 2, 3, 4, 5]);
    assert.equal(ft.rangeSum(1, 3), 9); // 2+3+4
  });
  it('point update', () => {
    const ft = FenwickTree.fromArray([1, 2, 3]);
    ft.update(1, 10); // Add 10 to index 1
    assert.equal(ft.rangeSum(0, 2), 16); // 1+12+3
  });
});

describe('SegmentTree', () => {
  it('range min query', () => {
    const st = new SegmentTree([5, 2, 8, 1, 4, 3]);
    assert.equal(st.query(0, 5), 1); // Min of all
    assert.equal(st.query(0, 2), 2); // Min of first 3
    assert.equal(st.query(3, 5), 1); // Min of last 3
  });
  it('point update', () => {
    const st = new SegmentTree([5, 2, 8, 1, 4, 3]);
    st.update(3, 10); // Change index 3 from 1 to 10
    assert.equal(st.query(0, 5), 2); // New min
  });
  it('range max query', () => {
    const st = new SegmentTree([5, 2, 8, 1, 4, 3], Math.max, -Infinity);
    assert.equal(st.query(0, 5), 8);
  });
  it('range sum query', () => {
    const st = new SegmentTree([1, 2, 3, 4, 5], (a, b) => a + b, 0);
    assert.equal(st.query(0, 4), 15);
    assert.equal(st.query(1, 3), 9);
  });
});

describe('UnionFind', () => {
  it('initially disconnected', () => {
    const uf = new UnionFind(5);
    assert.ok(!uf.connected(0, 1));
    assert.equal(uf.count, 5);
  });
  it('union connects', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1); uf.union(2, 3);
    assert.ok(uf.connected(0, 1));
    assert.ok(!uf.connected(0, 2));
    assert.equal(uf.count, 3);
  });
  it('transitive', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1); uf.union(1, 2);
    assert.ok(uf.connected(0, 2));
  });
});

describe('SuffixArray', () => {
  it('search finds all occurrences', () => {
    const sa = new SuffixArray('abcabcabc');
    const positions = sa.search('abc');
    assert.deepEqual(positions.sort(), [0, 3, 6]);
  });
  it('no match returns empty', () => {
    const sa = new SuffixArray('hello');
    assert.deepEqual(sa.search('xyz'), []);
  });
  it('single char search', () => {
    const sa = new SuffixArray('banana');
    assert.equal(sa.search('a').length, 3);
  });
});

describe('DoubleHashTable', () => {
  it('set/get', () => {
    const ht = new DoubleHashTable();
    ht.set('a', 1); ht.set('b', 2);
    assert.equal(ht.get('a'), 1);
    assert.equal(ht.get('b'), 2);
  });
  it('1000 inserts', () => {
    const ht = new DoubleHashTable(256);
    for (let i = 0; i < 1000; i++) ht.set(i, i * 10);
    for (let i = 0; i < 1000; i++) assert.equal(ht.get(i), i * 10);
  });
  it('auto resize', () => {
    const ht = new DoubleHashTable(4);
    for (let i = 0; i < 20; i++) ht.set(i, i);
    assert.equal(ht.size, 20);
    for (let i = 0; i < 20; i++) assert.equal(ht.get(i), i);
  });
});
