// leapfrog.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LeapfrogIterator, LeapfrogJoin, CrackerColumn } from './leapfrog.js';

describe('LeapfrogJoin', () => {
  it('2-way intersection', () => {
    const a = new LeapfrogIterator([1, 3, 5, 7, 9]);
    const b = new LeapfrogIterator([2, 3, 5, 8, 9]);
    const join = new LeapfrogJoin([a, b]);
    assert.deepEqual([...join.join()], [3, 5, 9]);
  });

  it('3-way intersection', () => {
    const a = new LeapfrogIterator([1, 2, 3, 5, 8]);
    const b = new LeapfrogIterator([2, 3, 5, 7, 8]);
    const c = new LeapfrogIterator([3, 4, 5, 8, 9]);
    const join = new LeapfrogJoin([a, b, c]);
    assert.deepEqual([...join.join()], [3, 5, 8]);
  });

  it('no intersection', () => {
    const a = new LeapfrogIterator([1, 2, 3]);
    const b = new LeapfrogIterator([4, 5, 6]);
    assert.deepEqual([...new LeapfrogJoin([a, b]).join()], []);
  });

  it('single iterator', () => {
    const a = new LeapfrogIterator([1, 2, 3]);
    assert.deepEqual([...new LeapfrogJoin([a]).join()], [1, 2, 3]);
  });

  it('empty iterator', () => {
    const a = new LeapfrogIterator([1, 2, 3]);
    const b = new LeapfrogIterator([]);
    assert.deepEqual([...new LeapfrogJoin([a, b]).join()], []);
  });

  it('benchmark: 3-way join on 1K sorted arrays', () => {
    const make = () => Array.from({ length: 1000 }, (_, i) => i * 3 + Math.floor(Math.random() * 3)).sort((a, b) => a - b);
    const a = new LeapfrogIterator(make());
    const b = new LeapfrogIterator(make());
    const c = new LeapfrogIterator(make());
    const t0 = Date.now();
    const result = [...new LeapfrogJoin([a, b, c]).join()];
    console.log(`    Leapfrog 3×1K: ${Date.now() - t0}ms, ${result.length} matches`);
    assert.ok(result.length >= 0);
  });
});

describe('CrackerColumn (Adaptive Indexing)', () => {
  it('crack partitions data', () => {
    const col = new CrackerColumn([8, 3, 7, 1, 5, 9, 2, 6, 4]);
    const below5 = col.crack(5);
    assert.ok(below5.every(v => v < 5));
    assert.equal(below5.length, 4); // 1, 2, 3, 4
  });

  it('range query', () => {
    const col = new CrackerColumn(Array.from({ length: 100 }, (_, i) => i));
    const result = col.rangeQuery(25, 75);
    assert.ok(result.every(v => v >= 25 && v <= 75));
    assert.equal(result.length, 51);
  });

  it('multiple cracks refine structure', () => {
    const col = new CrackerColumn([5, 3, 8, 1, 7, 2, 9, 4, 6]);
    col.crack(5);
    col.crack(3);
    assert.equal(col.crackCount, 2);
  });

  it('benchmark: crack 100K column', () => {
    const data = Array.from({ length: 100000 }, () => Math.floor(Math.random() * 100000));
    const col = new CrackerColumn(data);
    const t0 = Date.now();
    col.crack(25000);
    col.crack(50000);
    col.crack(75000);
    console.log(`    Cracking 100K: ${Date.now() - t0}ms, ${col.crackCount} cracks`);
    assert.equal(col.crackCount, 3);
  });
});
