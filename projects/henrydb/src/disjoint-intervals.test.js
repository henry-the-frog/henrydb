// disjoint-intervals.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DisjointIntervals } from './disjoint-intervals.js';

describe('DisjointIntervals', () => {
  it('add and merge overlapping', () => {
    const di = new DisjointIntervals();
    di.add(1, 3);
    di.add(5, 7);
    di.add(2, 6); // Merges all three
    assert.deepEqual(di.toArray(), [[1, 7]]);
  });

  it('non-overlapping remain separate', () => {
    const di = new DisjointIntervals();
    di.add(1, 3);
    di.add(5, 7);
    assert.deepEqual(di.toArray(), [[1, 3], [5, 7]]);
  });

  it('contains', () => {
    const di = new DisjointIntervals();
    di.add(1, 5);
    di.add(10, 15);
    assert.equal(di.contains(3), true);
    assert.equal(di.contains(7), false);
    assert.equal(di.contains(12), true);
  });

  it('remove splits intervals', () => {
    const di = new DisjointIntervals();
    di.add(1, 10);
    di.remove(4, 6);
    assert.deepEqual(di.toArray(), [[1, 3], [7, 10]]);
  });

  it('totalCoverage', () => {
    const di = new DisjointIntervals();
    di.add(1, 5);
    di.add(10, 14);
    assert.equal(di.totalCoverage(), 10); // 5 + 5
  });

  it('gaps', () => {
    const di = new DisjointIntervals();
    di.add(1, 3);
    di.add(7, 9);
    const gaps = di.gaps();
    assert.equal(gaps.length, 1);
    assert.equal(gaps[0].lo, 4);
    assert.equal(gaps[0].hi, 6);
  });

  it('use case: range locks', () => {
    const locks = new DisjointIntervals();
    locks.add(100, 200); // Lock rows 100-200
    locks.add(300, 400); // Lock rows 300-400
    
    // Check if row is locked
    assert.equal(locks.contains(150), true);
    assert.equal(locks.contains(250), false);
    
    // Extend lock
    locks.add(180, 350);
    assert.deepEqual(locks.toArray(), [[100, 400]]); // Merged!
  });
});
