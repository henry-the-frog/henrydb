// r-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RTree } from './r-tree.js';

describe('RTree', () => {
  it('insert and search', () => {
    const rt = new RTree();
    rt.insert(0, 0, 10, 10, 'A');
    rt.insert(5, 5, 15, 15, 'B');
    rt.insert(20, 20, 30, 30, 'C');
    
    const overlap = rt.search(7, 7, 12, 12);
    const data = overlap.map(e => e.data).sort();
    assert.deepEqual(data, ['A', 'B']);
  });

  it('point query', () => {
    const rt = new RTree();
    rt.insert(0, 0, 10, 10, 'rect1');
    rt.insert(5, 5, 15, 15, 'rect2');
    
    const at3 = rt.searchPoint(3, 3);
    assert.equal(at3.length, 1);
    assert.equal(at3[0].data, 'rect1');
    
    const at7 = rt.searchPoint(7, 7);
    assert.equal(at7.length, 2);
  });

  it('no results outside all rectangles', () => {
    const rt = new RTree();
    rt.insert(0, 0, 5, 5, 'A');
    assert.equal(rt.search(10, 10, 20, 20).length, 0);
  });

  it('1000 rectangles', () => {
    const rt = new RTree(16);
    for (let i = 0; i < 1000; i++) {
      rt.insert(i, i, i + 5, i + 5, i);
    }
    
    assert.equal(rt.size, 1000);
    
    const results = rt.search(500, 500, 502, 502);
    assert.ok(results.length > 0);
    
    const t0 = performance.now();
    for (let q = 0; q < 1000; q++) rt.search(q, q, q + 2, q + 2);
    const elapsed = performance.now() - t0;
    console.log(`  1K queries on 1K rects: ${elapsed.toFixed(1)}ms`);
  });

  it('geospatial use case: find buildings near a point', () => {
    const rt = new RTree();
    // Buildings as bounding boxes (lat/lng simplified)
    rt.insert(40.7, -74.0, 40.71, -73.99, 'WTC');
    rt.insert(40.748, -73.986, 40.749, -73.985, 'Empire State');
    rt.insert(40.689, -74.045, 40.69, -74.044, 'Statue of Liberty');
    
    // Search near Empire State Building
    const nearby = rt.search(40.74, -74.0, 40.76, -73.98);
    assert.ok(nearby.some(e => e.data === 'Empire State'));
  });
});
