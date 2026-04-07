// rtree.test.js — R-tree spatial index tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RTree, Rect } from './rtree.js';

describe('Rect', () => {
  it('calculates area', () => {
    const r = new Rect(0, 0, 10, 5);
    assert.equal(r.area(), 50);
  });

  it('detects overlap', () => {
    const r1 = new Rect(0, 0, 10, 10);
    const r2 = new Rect(5, 5, 15, 15);
    assert.equal(r1.overlaps(r2), true);
  });

  it('detects non-overlap', () => {
    const r1 = new Rect(0, 0, 5, 5);
    const r2 = new Rect(10, 10, 15, 15);
    assert.equal(r1.overlaps(r2), false);
  });

  it('merges bounding boxes', () => {
    const merged = Rect.merge(new Rect(0, 0, 5, 5), new Rect(3, 3, 10, 10));
    assert.equal(merged.minX, 0);
    assert.equal(merged.maxX, 10);
  });
});

describe('RTree', () => {
  it('insert and search points', () => {
    const tree = new RTree();
    tree.insert(Rect.point(5, 5), 'A');
    tree.insert(Rect.point(10, 10), 'B');
    tree.insert(Rect.point(50, 50), 'C');
    
    const results = tree.search(new Rect(0, 0, 20, 20));
    assert.equal(results.length, 2);
    assert.ok(results.some(r => r.data === 'A'));
    assert.ok(results.some(r => r.data === 'B'));
  });

  it('search returns empty for non-overlapping query', () => {
    const tree = new RTree();
    tree.insert(Rect.point(5, 5), 'A');
    
    const results = tree.search(new Rect(100, 100, 200, 200));
    assert.equal(results.length, 0);
  });

  it('handles many insertions with splitting', () => {
    const tree = new RTree(4, 2); // Small node size to trigger splits
    
    for (let i = 0; i < 100; i++) {
      tree.insert(Rect.point(i, i), `item_${i}`);
    }
    
    assert.equal(tree.size, 100);
    
    // Search a region that should contain some items
    const results = tree.search(new Rect(10, 10, 20, 20));
    assert.ok(results.length >= 10);
  });

  it('searches overlapping rectangles', () => {
    const tree = new RTree();
    tree.insert(new Rect(0, 0, 10, 10), 'rect1');
    tree.insert(new Rect(5, 5, 15, 15), 'rect2');
    tree.insert(new Rect(20, 20, 30, 30), 'rect3');
    
    const results = tree.search(new Rect(8, 8, 12, 12));
    assert.equal(results.length, 2); // rect1 and rect2 overlap
  });

  it('radius search', () => {
    const tree = new RTree();
    tree.insert(Rect.point(0, 0), 'origin');
    tree.insert(Rect.point(3, 4), 'near'); // distance = 5
    tree.insert(Rect.point(10, 10), 'far'); // distance ~14
    
    const results = tree.searchRadius(0, 0, 6);
    assert.equal(results.length, 2);
    assert.ok(results.some(r => r.data === 'origin'));
    assert.ok(results.some(r => r.data === 'near'));
  });

  it('geographic-style data', () => {
    const tree = new RTree();
    // Simulate US cities (approximate lat/lon)
    tree.insert(Rect.point(-73.9, 40.7), 'New York');
    tree.insert(Rect.point(-118.2, 34.0), 'Los Angeles');
    tree.insert(Rect.point(-87.6, 41.9), 'Chicago');
    tree.insert(Rect.point(-95.4, 29.7), 'Houston');
    tree.insert(Rect.point(-112.1, 33.4), 'Phoenix');
    
    // Search East Coast region
    const eastCoast = tree.search(new Rect(-80, 25, -70, 45));
    assert.ok(eastCoast.some(r => r.data === 'New York'));
    assert.ok(!eastCoast.some(r => r.data === 'Los Angeles'));
  });
});
