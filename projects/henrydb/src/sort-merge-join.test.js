// sort-merge-join.test.js — Tests for sort-merge join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SortMergeJoin } from './sort-merge-join.js';
import { TypedColumn } from './typed-columns.js';
import { BatchOps } from './batch-ops.js';

function makeCol(values) {
  const col = new TypedColumn('INT', values.length);
  for (const v of values) col.push(v);
  return col;
}

describe('SortMergeJoin', () => {

  it('joinSorted: basic equi-join on sorted data', () => {
    const left = makeCol([1, 2, 3, 4, 5]);
    const right = makeCol([2, 3, 5, 7]);

    const smj = new SortMergeJoin();
    const { left: lIdx, right: rIdx } = smj.joinSorted(left, right);

    assert.equal(lIdx.length, 3); // 2, 3, 5 match
    assert.equal(rIdx.length, 3);
  });

  it('join: unsorted data (sorts then merges)', () => {
    const left = [5, 3, 1, 4, 2];
    const right = [3, 1, 5];

    const smj = new SortMergeJoin();
    const { left: lIdx, right: rIdx } = smj.join(left, right);

    assert.equal(lIdx.length, 3);
  });

  it('many-to-many join on sorted data', () => {
    const left = makeCol([1, 1, 2, 2, 3]);
    const right = makeCol([1, 2, 2]);

    const smj = new SortMergeJoin();
    const { left: lIdx } = smj.joinSorted(left, right);

    // 1×1: 2*1=2, 2×2: 2*2=4 → 6
    assert.equal(lIdx.length, 6);
  });

  it('no matches', () => {
    const left = makeCol([1, 2, 3]);
    const right = makeCol([4, 5, 6]);

    const smj = new SortMergeJoin();
    const { left: lIdx } = smj.joinSorted(left, right);
    assert.equal(lIdx.length, 0);
  });

  it('correctness vs hash join', () => {
    const n = 1000;
    const leftValues = Array.from({ length: n }, (_, i) => i);
    const rightValues = Array.from({ length: n }, (_, i) => i * 2);

    const left = makeCol(leftValues);
    const right = makeCol(rightValues);

    // Sort-merge
    const smj = new SortMergeJoin();
    const smjResult = smj.joinSorted(left, right);

    // Hash join
    const ht = BatchOps.buildHash(right);
    const hashResult = BatchOps.probeHash(left, ht);

    assert.equal(smjResult.left.length, hashResult.left.length);
  });

  it('benchmark: sort-merge vs hash join on 50K pre-sorted', () => {
    const n = 50000;
    const left = makeCol(Array.from({ length: n }, (_, i) => i));
    const right = makeCol(Array.from({ length: n * 2 }, (_, i) => i % n));

    // Sort right side for merge join
    const rightArr = [...right.toArray()].sort((a, b) => a - b);
    const rightSorted = makeCol(rightArr);

    // Sort-merge join
    const smj = new SortMergeJoin();
    const t0 = Date.now();
    const smjResult = smj.joinSorted(left, rightSorted);
    const smjMs = Date.now() - t0;

    // Hash join
    const t1 = Date.now();
    const ht = BatchOps.buildHash(right);
    const hashResult = BatchOps.probeHash(left, ht);
    const hashMs = Date.now() - t1;

    console.log(`    SMJ: ${smjMs}ms vs Hash: ${hashMs}ms | Matches: ${smjResult.left.length}`);
    assert.equal(smjResult.left.length, hashResult.left.length);
  });

  it('stats tracked', () => {
    const smj = new SortMergeJoin();
    smj.joinSorted(makeCol([1, 2, 3]), makeCol([2, 3, 4]));

    const stats = smj.getStats();
    assert.equal(stats.leftRows, 3);
    assert.equal(stats.rightRows, 3);
    assert.equal(stats.matches, 2);
    assert.ok(stats.mergeTimeMs >= 0);
  });
});
