// interval-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IntervalTree } from './interval-tree.js';

describe('IntervalTree', () => {
  it('point query finds overlapping intervals', () => {
    const it = new IntervalTree();
    it.insert(1, 5, 'A');
    it.insert(3, 8, 'B');
    it.insert(6, 10, 'C');
    
    const at4 = it.queryPoint(4);
    assert.equal(at4.length, 2); // A and B
    const at7 = it.queryPoint(7);
    assert.equal(at7.length, 2); // B and C
    assert.equal(it.queryPoint(11).length, 0);
  });

  it('range query finds all overlapping', () => {
    const it = new IntervalTree();
    it.insert(1, 3, 'A');
    it.insert(5, 7, 'B');
    it.insert(9, 11, 'C');
    
    const overlap = it.queryRange(2, 6);
    assert.equal(overlap.length, 2); // A and B overlap [2,6]
  });

  it('scheduling: find busy time slots', () => {
    const cal = new IntervalTree();
    cal.insert(900, 1000, 'Meeting A');
    cal.insert(1030, 1130, 'Meeting B');
    cal.insert(1400, 1500, 'Meeting C');
    
    const busy = cal.queryPoint(950);
    assert.equal(busy.length, 1);
    assert.equal(busy[0].value, 'Meeting A');
    
    assert.equal(cal.queryPoint(1200).length, 0); // Free
  });

  it('1K intervals', () => {
    const it = new IntervalTree();
    for (let i = 0; i < 1000; i++) it.insert(i * 10, i * 10 + 5, i);
    
    const results = it.queryPoint(500);
    assert.ok(results.length >= 1);
  });
});
