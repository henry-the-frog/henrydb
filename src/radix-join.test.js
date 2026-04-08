// radix-join.test.js — Tests for radix-partitioned hash join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RadixJoin } from './radix-join.js';
import { TypedColumn } from './typed-columns.js';
import { BatchOps } from './batch-ops.js';

function makeIntCol(values) {
  const col = new TypedColumn('INT', values.length);
  for (const v of values) col.push(v);
  return col;
}

describe('RadixJoin', () => {

  it('simple equi-join', () => {
    const left = makeIntCol([1, 2, 3, 4, 5]);
    const right = makeIntCol([3, 1, 4, 1, 5]);

    const rj = new RadixJoin();
    const { left: lIdx, right: rIdx } = rj.join(left, right);

    // Expected matches: (1,1), (1,3), (3,0), (4,2), (5,4)
    assert.equal(lIdx.length, 5);
    assert.equal(rIdx.length, 5);
  });

  it('no matches', () => {
    const left = makeIntCol([1, 2, 3]);
    const right = makeIntCol([4, 5, 6]);

    const rj = new RadixJoin();
    const { left: lIdx, right: rIdx } = rj.join(left, right);
    assert.equal(lIdx.length, 0);
  });

  it('many-to-many join', () => {
    const left = makeIntCol([1, 1, 2, 2]);
    const right = makeIntCol([1, 1, 2]);

    const rj = new RadixJoin();
    const { left: lIdx, right: rIdx } = rj.join(left, right);
    
    // 1×1: 2*2=4 matches, 2×2: 2*1=2 matches → 6 total
    assert.equal(lIdx.length, 6);
  });

  it('correctness: matches standard hash join', () => {
    const n = 500;
    const leftValues = Array.from({ length: n }, (_, i) => i);
    const rightValues = Array.from({ length: n * 3 }, (_, i) => i % n);

    const left = makeIntCol(leftValues);
    const right = makeIntCol(rightValues);

    // Radix join
    const rj = new RadixJoin();
    const radix = rj.join(left, right);

    // Standard hash join (using BatchOps)
    const ht = BatchOps.buildHash(right);
    const standard = BatchOps.probeHash(left, ht);

    assert.equal(radix.left.length, standard.left.length,
      `Radix: ${radix.left.length} vs Standard: ${standard.left.length}`);
    assert.equal(radix.left.length, n * 3); // Each left matches 3 right rows
  });

  it('benchmark: radix vs standard hash join', () => {
    const n = 10000;
    const leftValues = Array.from({ length: n }, (_, i) => i);
    const rightValues = Array.from({ length: n * 3 }, (_, i) => i % n);

    const left = makeIntCol(leftValues);
    const right = makeIntCol(rightValues);

    // Radix join
    const rj = new RadixJoin();
    const t0 = Date.now();
    const radixResult = rj.join(left, right);
    const radixMs = Date.now() - t0;

    // Standard hash join
    const t1 = Date.now();
    const ht = BatchOps.buildHash(right);
    const stdResult = BatchOps.probeHash(left, ht);
    const stdMs = Date.now() - t1;

    console.log(`    Radix: ${radixMs}ms (${rj.stats.partitionsUsed} partitions) vs Standard: ${stdMs}ms (${(stdMs / Math.max(radixMs, 0.1)).toFixed(1)}x)`);
    assert.equal(radixResult.left.length, stdResult.left.length);
    assert.equal(radixResult.left.length, n * 3);
  });

  it('large join: 100K × 300K', () => {
    const n = 100000;
    const left = new TypedColumn('INT', n);
    const right = new TypedColumn('INT', n * 3);
    for (let i = 0; i < n; i++) left.push(i);
    for (let i = 0; i < n * 3; i++) right.push(i % n);

    const rj = new RadixJoin();
    const t0 = Date.now();
    const result = rj.join(left, right);
    const ms = Date.now() - t0;

    console.log(`    100K×300K: ${ms}ms, ${result.left.length} matches, ${rj.stats.partitionsUsed} partitions`);
    assert.equal(result.left.length, n * 3);
  });

  it('stats are tracked', () => {
    const left = makeIntCol([1, 2, 3]);
    const right = makeIntCol([2, 3, 4]);

    const rj = new RadixJoin();
    rj.join(left, right);

    const stats = rj.getStats();
    assert.ok(stats.partitionTimeMs >= 0);
    assert.ok(stats.joinTimeMs >= 0);
    assert.equal(stats.totalMatches, 2);
    assert.ok(stats.partitionsUsed > 0);
  });

  it('custom radix bits', () => {
    const left = makeIntCol([1, 2, 3, 4, 5]);
    const right = makeIntCol([3, 1, 5]);

    const rj = new RadixJoin({ radixBits: 4 }); // 16 partitions
    const { left: lIdx } = rj.join(left, right);
    assert.equal(lIdx.length, 3);
    assert.equal(rj.numPartitions, 16);
  });
});
