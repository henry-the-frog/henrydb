// grace-hash-join.test.js — Tests for Grace hash join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GraceHashJoin } from './grace-hash-join.js';
import { BatchOps } from './batch-ops.js';
import { TypedColumn } from './typed-columns.js';

describe('GraceHashJoin', () => {

  it('basic equi-join', () => {
    const left = [1, 2, 3, 4, 5];
    const right = [3, 1, 5, 7];

    const ghj = new GraceHashJoin({ numPartitions: 4 });
    const { left: lIdx, right: rIdx } = ghj.join(left, right);

    assert.equal(lIdx.length, 3);
    assert.equal(rIdx.length, 3);
  });

  it('many-to-many', () => {
    const left = [1, 1, 2, 2];
    const right = [1, 1, 2];

    const ghj = new GraceHashJoin({ numPartitions: 4 });
    const { left: lIdx } = ghj.join(left, right);
    assert.equal(lIdx.length, 6); // 2*2 + 2*1
  });

  it('no matches', () => {
    const ghj = new GraceHashJoin();
    const { left: lIdx } = ghj.join([1, 2, 3], [4, 5, 6]);
    assert.equal(lIdx.length, 0);
  });

  it('correctness vs standard hash join', () => {
    const n = 5000;
    const left = Array.from({ length: n }, (_, i) => i);
    const right = Array.from({ length: n * 2 }, (_, i) => i % n);

    const ghj = new GraceHashJoin({ numPartitions: 8 });
    const graceResult = ghj.join(left, right);

    // Standard
    const leftCol = new TypedColumn('INT', n);
    const rightCol = new TypedColumn('INT', n * 2);
    for (const v of left) leftCol.push(v);
    for (const v of right) rightCol.push(v);
    const ht = BatchOps.buildHash(rightCol);
    const stdResult = BatchOps.probeHash(leftCol, ht);

    assert.equal(graceResult.left.length, stdResult.left.length);
  });

  it('benchmark: Grace vs standard on 100K', () => {
    const n = 100000;
    const left = Array.from({ length: n }, (_, i) => i);
    const right = Array.from({ length: n }, (_, i) => i * 2);

    // Grace
    const ghj = new GraceHashJoin({ numPartitions: 16 });
    const t0 = Date.now();
    const graceResult = ghj.join(left, right);
    const graceMs = Date.now() - t0;

    // Standard
    const t1 = Date.now();
    const ht = new Map();
    for (let i = 0; i < right.length; i++) {
      if (!ht.has(right[i])) ht.set(right[i], []);
      ht.get(right[i]).push(i);
    }
    let stdMatches = 0;
    for (let i = 0; i < left.length; i++) {
      const m = ht.get(left[i]);
      if (m) stdMatches += m.length;
    }
    const stdMs = Date.now() - t1;

    console.log(`    Grace: ${graceMs}ms vs Standard: ${stdMs}ms | Matches: ${graceResult.left.length}`);
    assert.equal(graceResult.left.length, stdMatches);
  });

  it('stats tracked', () => {
    const ghj = new GraceHashJoin({ numPartitions: 4 });
    ghj.join([1, 2, 3], [2, 3, 4]);

    const stats = ghj.getStats();
    assert.equal(stats.leftRows, 3);
    assert.equal(stats.rightRows, 3);
    assert.equal(stats.matches, 2);
  });
});
