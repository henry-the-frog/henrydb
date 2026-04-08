// rtree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RTree } from './rtree.js';

describe('RTree', () => {
  it('insert and search points', () => {
    const tree = new RTree();
    tree.insert({ x: 10, y: 10 }, 'A');
    tree.insert({ x: 20, y: 20 }, 'B');
    tree.insert({ x: 30, y: 30 }, 'C');
    
    const results = tree.search({ minX: 5, minY: 5, maxX: 25, maxY: 25 });
    assert.ok(results.includes('A'));
    assert.ok(results.includes('B'));
    assert.ok(!results.includes('C'));
  });

  it('insert rectangles', () => {
    const tree = new RTree();
    tree.insert({ minX: 0, minY: 0, maxX: 10, maxY: 10 }, 'rect1');
    tree.insert({ minX: 5, minY: 5, maxX: 15, maxY: 15 }, 'rect2');
    tree.insert({ minX: 20, minY: 20, maxX: 30, maxY: 30 }, 'rect3');
    
    const results = tree.search({ minX: 8, minY: 8, maxX: 12, maxY: 12 });
    assert.ok(results.includes('rect1'));
    assert.ok(results.includes('rect2'));
    assert.ok(!results.includes('rect3'));
  });

  it('nearest neighbor', () => {
    const tree = new RTree();
    tree.insert({ x: 0, y: 0 }, 'origin');
    tree.insert({ x: 100, y: 100 }, 'far');
    tree.insert({ x: 5, y: 5 }, 'close');
    
    const nearest = tree.nearest({ x: 3, y: 3 }, 1);
    assert.equal(nearest[0].data, 'close');
  });

  it('k nearest neighbors', () => {
    const tree = new RTree();
    for (let i = 0; i < 20; i++) tree.insert({ x: i * 10, y: i * 10 }, `p${i}`);
    
    const knn = tree.nearest({ x: 50, y: 50 }, 3);
    assert.equal(knn.length, 3);
    assert.equal(knn[0].data, 'p5'); // Closest to (50,50)
  });

  it('empty tree returns empty', () => {
    const tree = new RTree();
    assert.deepEqual(tree.search({ minX: 0, minY: 0, maxX: 100, maxY: 100 }), []);
    assert.deepEqual(tree.nearest({ x: 0, y: 0 }), []);
  });

  it('many insertions with splitting', () => {
    const tree = new RTree(4); // Small node capacity
    for (let i = 0; i < 100; i++) {
      tree.insert({ x: Math.random() * 1000, y: Math.random() * 1000 }, `p${i}`);
    }
    assert.equal(tree.size, 100);
    
    // Search should find points in range
    const results = tree.search({ minX: 0, minY: 0, maxX: 1000, maxY: 1000 });
    assert.equal(results.length, 100);
  });

  it('benchmark: 10K points', () => {
    const tree = new RTree(16);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      tree.insert({ x: Math.random() * 10000, y: Math.random() * 10000 }, i);
    }
    const buildMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < 1000; i++) {
      tree.search({ minX: i * 10, minY: i * 10, maxX: i * 10 + 100, maxY: i * 10 + 100 });
    }
    const searchMs = Date.now() - t1;

    console.log(`    10K build: ${buildMs}ms, 1K searches: ${searchMs}ms`);
  });
});
