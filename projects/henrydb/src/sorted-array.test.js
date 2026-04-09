// sorted-array.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SortedArray } from './sorted-array.js';

describe('SortedArray', () => {
  it('insert, get, has', () => {
    const sa = new SortedArray();
    sa.insert(3, 'c'); sa.insert(1, 'a'); sa.insert(2, 'b');
    assert.equal(sa.get(1), 'a');
    assert.equal(sa.get(2), 'b');
    assert.equal(sa.has(3), true);
    assert.equal(sa.has(4), false);
  });

  it('upsert', () => {
    const sa = new SortedArray();
    sa.insert(1, 'old');
    sa.insert(1, 'new');
    assert.equal(sa.get(1), 'new');
    assert.equal(sa.size, 1);
  });

  it('delete', () => {
    const sa = new SortedArray();
    sa.insert(1, 'a'); sa.insert(2, 'b');
    assert.equal(sa.delete(1), true);
    assert.equal(sa.has(1), false);
    assert.equal(sa.delete(99), false);
  });

  it('range', () => {
    const sa = new SortedArray();
    for (let i = 0; i < 10; i++) sa.insert(i, i);
    const r = sa.range(3, 7);
    assert.deepEqual(r.map(e => e.key), [3, 4, 5, 6, 7]);
  });

  it('min and max', () => {
    const sa = new SortedArray();
    sa.insert(5, 'e'); sa.insert(1, 'a'); sa.insert(9, 'i');
    assert.equal(sa.min().key, 1);
    assert.equal(sa.max().key, 9);
  });

  it('benchmark: 10K lookup (baseline)', () => {
    const sa = new SortedArray();
    for (let i = 0; i < 10000; i++) sa.insert(i, i);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) sa.get(i);
    const lookupMs = performance.now() - t0;
    
    console.log(`  10K lookup: ${lookupMs.toFixed(1)}ms (${(lookupMs/10000*1000).toFixed(3)}µs avg)`);
    assert.ok(lookupMs < 100);
  });
});
