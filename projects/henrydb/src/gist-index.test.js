// gist-index.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { GiSTIndex, RangeOpClass, Point2DOpClass } from './gist-index.js';

describe('GiST Index — Range', () => {
  test('insert and search range containment', () => {
    const idx = new GiSTIndex(RangeOpClass);
    idx.insert([1, 10], 'a');
    idx.insert([5, 15], 'b');
    idx.insert([20, 30], 'c');
    idx.insert([25, 35], 'd');

    const results = idx.search({ op: 'contains', value: 7 });
    assert.ok(results.some(r => r.data === 'a'));
    assert.ok(results.some(r => r.data === 'b'));
    assert.ok(!results.some(r => r.data === 'c'));
  });

  test('search range overlap', () => {
    const idx = new GiSTIndex(RangeOpClass);
    idx.insert([1, 10], 'a');
    idx.insert([5, 15], 'b');
    idx.insert([20, 30], 'c');

    const results = idx.search({ op: 'overlaps', range: [8, 12] });
    assert.ok(results.some(r => r.data === 'a'));
    assert.ok(results.some(r => r.data === 'b'));
    assert.ok(!results.some(r => r.data === 'c'));
  });

  test('many inserts maintain index', () => {
    const idx = new GiSTIndex(RangeOpClass, { maxEntries: 5 });
    for (let i = 0; i < 100; i++) {
      idx.insert([i * 10, i * 10 + 5], `item_${i}`);
    }
    assert.equal(idx.size, 100);

    const results = idx.search({ op: 'contains', value: 505 });
    assert.ok(results.length > 0);
    assert.ok(results.some(r => r.data === 'item_50'));
  });

  test('remove entry', () => {
    const idx = new GiSTIndex(RangeOpClass);
    idx.insert([1, 10], 'a');
    idx.insert([5, 15], 'b');
    
    idx.remove([1, 10], 'a');
    assert.equal(idx.size, 1);
    
    const results = idx.search({ op: 'contains', value: 3 });
    assert.ok(!results.some(r => r.data === 'a'));
  });

  test('nearest neighbor for ranges', () => {
    const idx = new GiSTIndex(RangeOpClass);
    idx.insert([1, 5], 'near');
    idx.insert([10, 15], 'mid');
    idx.insert([100, 200], 'far');

    const nn = idx.nearestNeighbor(3, 2);
    assert.equal(nn.length, 2);
    assert.equal(nn[0].data, 'near');
  });

  test('empty search returns empty', () => {
    const idx = new GiSTIndex(RangeOpClass);
    idx.insert([1, 10], 'a');
    const results = idx.search({ op: 'contains', value: 999 });
    assert.equal(results.length, 0);
  });
});

describe('GiST Index — Point2D', () => {
  test('insert and search points in box', () => {
    const idx = new GiSTIndex(Point2DOpClass);
    idx.insert([1, 1, 1, 1], { name: 'A' });
    idx.insert([5, 5, 5, 5], { name: 'B' });
    idx.insert([10, 10, 10, 10], { name: 'C' });

    const results = idx.search({ op: 'intersects_box', x1: 0, y1: 0, x2: 6, y2: 6 });
    assert.ok(results.some(r => r.data.name === 'A'));
    assert.ok(results.some(r => r.data.name === 'B'));
    assert.ok(!results.some(r => r.data.name === 'C'));
  });

  test('contains point query', () => {
    const idx = new GiSTIndex(Point2DOpClass);
    idx.insert([0, 0, 10, 10], 'box1');
    idx.insert([20, 20, 30, 30], 'box2');

    const results = idx.search({ op: 'contains_point', x: 5, y: 5 });
    assert.ok(results.some(r => r.data === 'box1'));
    assert.ok(!results.some(r => r.data === 'box2'));
  });

  test('many 2D points', () => {
    const idx = new GiSTIndex(Point2DOpClass, { maxEntries: 10 });
    for (let i = 0; i < 50; i++) {
      idx.insert([i, i, i, i], `point_${i}`);
    }
    assert.equal(idx.size, 50);

    const results = idx.search({ op: 'intersects_box', x1: 20, y1: 20, x2: 25, y2: 25 });
    assert.ok(results.length >= 5);
  });

  test('nearest neighbor for 2D points', () => {
    const idx = new GiSTIndex(Point2DOpClass);
    idx.insert([1, 1, 1, 1], 'close');
    idx.insert([10, 10, 10, 10], 'mid');
    idx.insert([100, 100, 100, 100], 'far');

    const nn = idx.nearestNeighbor({ x: 0, y: 0 }, 2);
    assert.equal(nn.length, 2);
    assert.equal(nn[0].data, 'close');
  });
});

describe('GiST extensibility', () => {
  test('custom operator class', () => {
    // Simple integer operator class
    const IntOpClass = {
      consistent: (key, query) => key === query || (Array.isArray(key) && key[0] <= query && key[1] >= query),
      union: (keys) => [Math.min(...keys.flat()), Math.max(...keys.flat())],
      penalty: (a, b) => Math.abs((Array.isArray(a) ? a[0] : a) - (Array.isArray(b) ? b[0] : b)),
      picksplit: (entries) => {
        const mid = Math.ceil(entries.length / 2);
        return [entries.slice(0, mid), entries.slice(mid)];
      },
      same: (a, b) => JSON.stringify(a) === JSON.stringify(b),
    };

    const idx = new GiSTIndex(IntOpClass);
    idx.insert(5, 'five');
    idx.insert(10, 'ten');
    idx.insert(15, 'fifteen');

    const results = idx.search(10);
    assert.ok(results.some(r => r.data === 'ten'));
  });
});
