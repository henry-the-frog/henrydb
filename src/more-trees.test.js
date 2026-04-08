// more-trees.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IntervalTree, OrderStatisticsTree, Quadtree, BinaryHeap, SplayTree } from './more-trees.js';

describe('IntervalTree', () => {
  it('point query', () => {
    const it = new IntervalTree();
    it.insert(1, 5, 'A'); it.insert(3, 8, 'B'); it.insert(10, 15, 'C');
    assert.deepEqual(it.query(4).sort(), ['A', 'B']);
    assert.deepEqual(it.query(12), ['C']);
    assert.deepEqual(it.query(9), []);
  });
  it('overlap query', () => {
    const it = new IntervalTree();
    it.insert(1, 5, 'A'); it.insert(3, 8, 'B'); it.insert(10, 15, 'C');
    assert.deepEqual(it.overlap(4, 6).sort(), ['A', 'B']);
    assert.deepEqual(it.overlap(6, 12).sort(), ['B', 'C']);
  });
});

describe('OrderStatisticsTree', () => {
  it('select k-th smallest', () => {
    const ost = new OrderStatisticsTree();
    [5, 3, 8, 1, 4, 7, 9].forEach(k => ost.insert(k));
    assert.equal(ost.select(0), 1); // Smallest
    assert.equal(ost.select(3), 5); // 4th smallest
    assert.equal(ost.select(6), 9); // Largest
  });
  it('rank', () => {
    const ost = new OrderStatisticsTree();
    [5, 3, 8, 1, 4, 7, 9].forEach(k => ost.insert(k));
    assert.equal(ost.rank(5), 3); // 3 elements less than 5
    assert.equal(ost.rank(1), 0); // Smallest
  });
  it('size', () => {
    const ost = new OrderStatisticsTree();
    for (let i = 0; i < 100; i++) ost.insert(i);
    assert.equal(ost.size, 100);
  });
});

describe('Quadtree', () => {
  it('insert and query', () => {
    const qt = new Quadtree(0, 0, 100, 100);
    qt.insert({ x: 10, y: 10 }); qt.insert({ x: 50, y: 50 }); qt.insert({ x: 90, y: 90 });
    const found = qt.query({ x: 0, y: 0, w: 60, h: 60 });
    assert.equal(found.length, 2); // (10,10) and (50,50)
  });
  it('many points', () => {
    const qt = new Quadtree(0, 0, 1000, 1000);
    for (let i = 0; i < 100; i++) qt.insert({ x: Math.random() * 1000, y: Math.random() * 1000 });
    const all = qt.query({ x: 0, y: 0, w: 1000, h: 1000 });
    assert.equal(all.length, 100);
  });
  it('empty region', () => {
    const qt = new Quadtree(0, 0, 100, 100);
    qt.insert({ x: 10, y: 10 });
    assert.equal(qt.query({ x: 50, y: 50, w: 50, h: 50 }).length, 0);
  });
});

describe('BinaryHeap', () => {
  it('min heap', () => {
    const h = new BinaryHeap((a, b) => a - b);
    h.push(5); h.push(2); h.push(8); h.push(1);
    assert.equal(h.pop(), 1);
    assert.equal(h.pop(), 2);
    assert.equal(h.pop(), 5);
  });
  it('max heap', () => {
    const h = new BinaryHeap((a, b) => b - a);
    h.push(5); h.push(2); h.push(8);
    assert.equal(h.pop(), 8);
    assert.equal(h.pop(), 5);
  });
  it('top-K', () => {
    const h = new BinaryHeap((a, b) => a - b);
    for (let i = 0; i < 100; i++) h.push(Math.floor(Math.random() * 1000));
    const top5 = [];
    for (let i = 0; i < 5; i++) top5.push(h.pop());
    assert.ok(top5[0] <= top5[1]); // Sorted ascending
  });
});

describe('SplayTree', () => {
  it('insert/get', () => {
    const st = new SplayTree();
    st.insert(5, 'five'); st.insert(3, 'three'); st.insert(8, 'eight');
    assert.equal(st.get(5), 'five');
    assert.equal(st.get(3), 'three');
    assert.equal(st.get(99), undefined);
  });
  it('recently accessed is at root', () => {
    const st = new SplayTree();
    st.insert(1, 'a'); st.insert(2, 'b'); st.insert(3, 'c');
    st.get(1); // Splay 1 to root
    assert.equal(st.root.key, 1);
  });
  it('1000 inserts', () => {
    const st = new SplayTree();
    for (let i = 0; i < 1000; i++) st.insert(i, i);
    assert.equal(st.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(st.get(i), i);
  });
});
