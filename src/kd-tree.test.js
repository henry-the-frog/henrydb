// kd-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { KDTree } from './kd-tree.js';

describe('KDTree', () => {
  const points = [
    { x: 2, y: 3, id: 'A' },
    { x: 5, y: 4, id: 'B' },
    { x: 9, y: 6, id: 'C' },
    { x: 4, y: 7, id: 'D' },
    { x: 8, y: 1, id: 'E' },
    { x: 7, y: 2, id: 'F' },
  ];

  it('build and nearest neighbor', () => {
    const tree = KDTree.build(points);
    const nearest = tree.nearest({ x: 5, y: 5 });
    assert.equal(nearest.id, 'B'); // (5,4) closest to (5,5)
  });

  it('nearest to corner', () => {
    const tree = KDTree.build(points);
    const nearest = tree.nearest({ x: 0, y: 0 });
    assert.equal(nearest.id, 'A'); // (2,3) closest to (0,0)
  });

  it('range search', () => {
    const tree = KDTree.build(points);
    const results = tree.rangeSearch({ x: 3, y: 1 }, { x: 8, y: 5 });
    assert.ok(results.some(p => p.id === 'B'));
    assert.ok(results.some(p => p.id === 'F'));
  });

  it('k-nearest neighbors', () => {
    const tree = KDTree.build(points);
    const knn = tree.kNearest({ x: 5, y: 5 }, 3);
    assert.equal(knn.length, 3);
    assert.equal(knn[0].id, 'B');
  });

  it('insert', () => {
    const tree = new KDTree(2);
    tree.insert({ x: 1, y: 1 });
    tree.insert({ x: 5, y: 5 });
    tree.insert({ x: 9, y: 9 });
    assert.equal(tree.size, 3);
    assert.equal(tree.nearest({ x: 4, y: 4 }).x, 5);
  });

  it('empty tree', () => {
    const tree = new KDTree(2);
    assert.equal(tree.nearest({ x: 0, y: 0 }), null);
  });

  it('benchmark: 10K points', () => {
    const pts = Array.from({ length: 10000 }, () => ({ x: Math.random() * 1000, y: Math.random() * 1000 }));
    const tree = KDTree.build(pts);
    
    const t0 = Date.now();
    for (let i = 0; i < 1000; i++) tree.nearest({ x: Math.random() * 1000, y: Math.random() * 1000 });
    console.log(`    KD-tree 10K: 1K nearest queries in ${Date.now() - t0}ms`);
    assert.equal(tree.size, 10000);
  });

  it('benchmark: range search on 10K points', () => {
    const pts = Array.from({ length: 10000 }, () => ({ x: Math.random() * 100, y: Math.random() * 100 }));
    const tree = KDTree.build(pts);
    
    const t0 = Date.now();
    const results = tree.rangeSearch({ x: 25, y: 25 }, { x: 75, y: 75 });
    console.log(`    KD-tree range: ${results.length} results in ${Date.now() - t0}ms`);
    assert.ok(results.length > 0);
  });
});
