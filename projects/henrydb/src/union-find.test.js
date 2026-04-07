// union-find.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { UnionFind, SortedSet } from './union-find.js';

describe('UnionFind', () => {
  it('elements start as separate components', () => {
    const uf = new UnionFind(5);
    assert.equal(uf.componentCount, 5);
    assert.equal(uf.connected(0, 1), false);
  });

  it('union merges components', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1);
    uf.union(2, 3);
    
    assert.equal(uf.connected(0, 1), true);
    assert.equal(uf.connected(2, 3), true);
    assert.equal(uf.connected(0, 2), false);
    assert.equal(uf.componentCount, 3);
  });

  it('transitive connectivity', () => {
    const uf = new UnionFind(4);
    uf.union(0, 1);
    uf.union(1, 2);
    
    assert.equal(uf.connected(0, 2), true);
  });

  it('componentSize tracks sizes', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1);
    uf.union(0, 2);
    
    assert.equal(uf.componentSize(0), 3);
    assert.equal(uf.componentSize(3), 1);
  });

  it('getComponents returns all groups', () => {
    const uf = new UnionFind(6);
    uf.union(0, 1);
    uf.union(2, 3);
    uf.union(4, 5);
    
    const components = uf.getComponents();
    assert.equal(components.length, 3);
    assert.ok(components.every(c => c.length === 2));
  });

  it('handles large number of unions', () => {
    const uf = new UnionFind(10000);
    for (let i = 0; i < 9999; i++) uf.union(i, i + 1);
    
    assert.equal(uf.componentCount, 1);
    assert.equal(uf.connected(0, 9999), true);
  });
});

describe('SortedSet', () => {
  it('inserts in sorted order', () => {
    const ss = new SortedSet();
    ss.insert(5);
    ss.insert(2);
    ss.insert(8);
    ss.insert(1);
    
    assert.deepEqual([...ss], [1, 2, 5, 8]);
  });

  it('maintains uniqueness', () => {
    const ss = new SortedSet();
    assert.equal(ss.insert(5), true);
    assert.equal(ss.insert(5), false);
    assert.equal(ss.size, 1);
  });

  it('rank returns position', () => {
    const ss = new SortedSet();
    [10, 20, 30, 40, 50].forEach(v => ss.insert(v));
    
    assert.equal(ss.rank(30), 2);
    assert.equal(ss.rank(10), 0);
    assert.equal(ss.rank(50), 4);
  });

  it('kth returns element by position', () => {
    const ss = new SortedSet();
    [10, 20, 30, 40, 50].forEach(v => ss.insert(v));
    
    assert.equal(ss.kth(0), 10);
    assert.equal(ss.kth(2), 30);
    assert.equal(ss.kth(4), 50);
  });

  it('min and max', () => {
    const ss = new SortedSet();
    [30, 10, 50, 20, 40].forEach(v => ss.insert(v));
    
    assert.equal(ss.min(), 10);
    assert.equal(ss.max(), 50);
  });

  it('range query', () => {
    const ss = new SortedSet();
    [10, 20, 30, 40, 50].forEach(v => ss.insert(v));
    
    assert.deepEqual(ss.range(20, 40), [20, 30, 40]);
  });

  it('delete removes element', () => {
    const ss = new SortedSet();
    [1, 2, 3, 4, 5].forEach(v => ss.insert(v));
    
    ss.delete(3);
    assert.deepEqual([...ss], [1, 2, 4, 5]);
    assert.equal(ss.has(3), false);
  });
});
