// union-find.test.js — Tests for Union-Find
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { UnionFind } from './union-find.js';

describe('UnionFind', () => {
  it('basic find and union', () => {
    const uf = new UnionFind(5);
    assert.equal(uf.sets, 5);
    
    uf.union(0, 1);
    assert.equal(uf.connected(0, 1), true);
    assert.equal(uf.connected(0, 2), false);
    assert.equal(uf.sets, 4);
  });

  it('transitive connectivity', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1);
    uf.union(1, 2);
    
    assert.equal(uf.connected(0, 2), true); // Transitive
    assert.equal(uf.connected(0, 3), false);
  });

  it('setSize', () => {
    const uf = new UnionFind(6);
    uf.union(0, 1);
    uf.union(1, 2);
    
    assert.equal(uf.setSize(0), 3);
    assert.equal(uf.setSize(3), 1);
  });

  it('makeSet adds new element', () => {
    const uf = new UnionFind(2);
    const newId = uf.makeSet();
    assert.equal(newId, 2);
    assert.equal(uf.elements, 3);
    assert.equal(uf.sets, 3);
  });

  it('getAllSets', () => {
    const uf = new UnionFind(5);
    uf.union(0, 1);
    uf.union(2, 3);
    
    const sets = uf.getAllSets();
    assert.equal(sets.length, 3); // {0,1}, {2,3}, {4}
  });

  it('union returns false for already connected', () => {
    const uf = new UnionFind(3);
    assert.equal(uf.union(0, 1), true);
    assert.equal(uf.union(0, 1), false);
  });

  it('stress: 10K elements, random unions', () => {
    const n = 10000;
    const uf = new UnionFind(n);
    
    const t0 = performance.now();
    for (let i = 0; i < n - 1; i++) {
      uf.union(i, i + 1);
    }
    const elapsed = performance.now() - t0;
    
    assert.equal(uf.sets, 1);
    assert.equal(uf.setSize(0), n);
    console.log(`  10K unions: ${elapsed.toFixed(1)}ms`);
  });

  it('path compression makes repeated finds fast', () => {
    const n = 10000;
    const uf = new UnionFind(n);
    // Create a long chain: 0→1→2→...→9999
    for (let i = 0; i < n - 1; i++) uf.union(i, i + 1);
    
    // First find: may traverse long path
    uf.find(0);
    
    // Second find: should be O(1) due to path compression
    const t0 = performance.now();
    for (let i = 0; i < n; i++) uf.find(i);
    const elapsed = performance.now() - t0;
    
    console.log(`  10K finds after compression: ${elapsed.toFixed(2)}ms (${(elapsed/n*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 50);
  });

  it('use case: join equivalence classes', () => {
    // In query optimization, columns connected by = are in the same class
    // SELECT * FROM a, b, c WHERE a.x = b.y AND b.y = c.z
    // → a.x, b.y, c.z are all equivalent
    
    const cols = ['a.x', 'b.y', 'c.z', 'a.id', 'b.id'];
    const colIdx = new Map(cols.map((c, i) => [c, i]));
    const uf = new UnionFind(cols.length);
    
    uf.union(colIdx.get('a.x'), colIdx.get('b.y'));
    uf.union(colIdx.get('b.y'), colIdx.get('c.z'));
    
    assert.equal(uf.connected(colIdx.get('a.x'), colIdx.get('c.z')), true);
    assert.equal(uf.connected(colIdx.get('a.x'), colIdx.get('a.id')), false);
  });
});
