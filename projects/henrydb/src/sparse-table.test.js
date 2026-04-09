// sparse-table.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SparseTable } from './sparse-table.js';

describe('SparseTable', () => {
  it('range minimum', () => {
    const st = SparseTable.min([5, 2, 8, 1, 9, 3, 7]);
    assert.equal(st.query(0, 6), 1);
    assert.equal(st.query(0, 2), 2);
    assert.equal(st.query(4, 6), 3);
  });

  it('range maximum', () => {
    const st = SparseTable.max([5, 2, 8, 1, 9, 3, 7]);
    assert.equal(st.query(0, 6), 9);
    assert.equal(st.query(0, 2), 8);
  });

  it('single element', () => {
    const st = SparseTable.min([42]);
    assert.equal(st.query(0, 0), 42);
  });

  it('performance: O(1) queries on 100K elements', () => {
    const arr = Array.from({ length: 100000 }, () => Math.random());
    const st = SparseTable.min(arr);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) {
      const l = Math.floor(Math.random() * 99000);
      st.query(l, l + Math.floor(Math.random() * 1000));
    }
    const elapsed = performance.now() - t0;
    console.log(`  100K queries on 100K elements: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
  });
});
