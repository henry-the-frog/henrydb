// kd-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { KDTree } from './kd-tree.js';

describe('KDTree', () => {
  it('nearest neighbor', () => {
    const kd = new KDTree(2);
    kd.insert([0, 0], 'origin');
    kd.insert([3, 4], 'far');
    kd.insert([1, 1], 'near');
    
    const nn = kd.nearest([0.5, 0.5]);
    assert.equal(nn.value, 'origin');
    assert.ok(nn.distance < 1);
  });

  it('range search', () => {
    const kd = new KDTree(2);
    kd.insert([0, 0], 'A');
    kd.insert([1, 1], 'B');
    kd.insert([5, 5], 'C');
    
    const within2 = kd.rangeSearch([0, 0], 2);
    assert.equal(within2.length, 2); // A and B
  });

  it('KNN', () => {
    const kd = new KDTree(2);
    for (let i = 0; i < 100; i++) kd.insert([i, i], i);
    
    const neighbors = kd.knn([50, 50], 3);
    assert.equal(neighbors.length, 3);
    assert.equal(neighbors[0].value, 50); // Exact match
  });

  it('balanced build from points', () => {
    const points = Array.from({ length: 1000 }, (_, i) => ({
      point: [Math.random() * 100, Math.random() * 100],
      value: i,
    }));
    
    const kd = KDTree.build(points, 2);
    assert.equal(kd.size, 1000);
    
    const nn = kd.nearest([50, 50]);
    assert.ok(nn.distance < 10);
  });

  it('3D points', () => {
    const kd = new KDTree(3);
    kd.insert([0, 0, 0], 'origin');
    kd.insert([1, 1, 1], 'unit');
    kd.insert([10, 10, 10], 'far');
    
    const nn = kd.nearest([0.5, 0.5, 0.5]);
    assert.equal(nn.value, 'origin');
  });

  it('performance: 10K nearest neighbor queries', () => {
    const points = Array.from({ length: 10000 }, (_, i) => ({
      point: [Math.random() * 1000, Math.random() * 1000],
      value: i,
    }));
    const kd = KDTree.build(points, 2);
    
    const t0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      kd.nearest([Math.random() * 1000, Math.random() * 1000]);
    }
    const elapsed = performance.now() - t0;
    console.log(`  1K NN queries on 10K points: ${elapsed.toFixed(1)}ms`);
  });
});
