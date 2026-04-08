// index-nested-loop-join.test.js — Tests for INLJ + semi/anti join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IndexNestedLoopJoin } from './index-nested-loop-join.js';

describe('IndexNestedLoopJoin', () => {

  it('basic join with pre-built index', () => {
    const outerKeys = [1, 2, 3, 4, 5];
    const innerKeys = [2, 3, 5, 7, 9];
    const index = IndexNestedLoopJoin.buildIndex(innerKeys);

    const inlj = new IndexNestedLoopJoin();
    const { left, right } = inlj.join(outerKeys, index);

    assert.equal(left.length, 3); // 2, 3, 5 match
  });

  it('many-to-many', () => {
    const outerKeys = [1, 1, 2];
    const innerKeys = [1, 1, 2, 2];
    const index = IndexNestedLoopJoin.buildIndex(innerKeys);

    const inlj = new IndexNestedLoopJoin();
    const { left } = inlj.join(outerKeys, index);

    // 1×1: 2*2=4, 2×2: 1*2=2 → 6
    assert.equal(left.length, 6);
  });

  it('no matches', () => {
    const index = IndexNestedLoopJoin.buildIndex([10, 20, 30]);
    const inlj = new IndexNestedLoopJoin();
    const { left } = inlj.join([1, 2, 3], index);
    assert.equal(left.length, 0);
  });

  it('semiJoin: EXISTS semantics', () => {
    const outerKeys = [1, 2, 3, 4, 5];
    const innerKeys = [2, 4, 6, 8];
    const index = IndexNestedLoopJoin.buildIndex(innerKeys);

    const inlj = new IndexNestedLoopJoin();
    const result = inlj.semiJoin(outerKeys, index);

    assert.equal(result.length, 2); // indices 1 (key=2) and 3 (key=4)
    assert.deepEqual([...result], [1, 3]);
  });

  it('antiJoin: NOT EXISTS semantics', () => {
    const outerKeys = [1, 2, 3, 4, 5];
    const innerKeys = [2, 4];
    const index = IndexNestedLoopJoin.buildIndex(innerKeys);

    const inlj = new IndexNestedLoopJoin();
    const result = inlj.antiJoin(outerKeys, index);

    assert.equal(result.length, 3); // indices 0 (key=1), 2 (key=3), 4 (key=5)
    assert.deepEqual([...result], [0, 2, 4]);
  });

  it('semiJoin with duplicates: each outer only once', () => {
    const outerKeys = [1, 2, 3];
    const innerKeys = [1, 1, 1, 2, 2]; // Many matches for 1 and 2
    const index = IndexNestedLoopJoin.buildIndex(innerKeys);

    const inlj = new IndexNestedLoopJoin();
    const result = inlj.semiJoin(outerKeys, index);

    assert.equal(result.length, 2); // Only 1 and 2 (not 3 duplicates)
  });

  it('benchmark: INLJ vs hash join on selective query', () => {
    const n = 100000;
    const inner = Array.from({ length: n }, (_, i) => i);
    const outer = [42, 1337, 99999]; // Very selective

    const index = IndexNestedLoopJoin.buildIndex(inner);
    const inlj = new IndexNestedLoopJoin();
    
    const t0 = Date.now();
    const inljResult = inlj.join(outer, index);
    const inljMs = Date.now() - t0;

    // Hash join (build entire hash table)
    const t1 = Date.now();
    const ht = new Map();
    for (let i = 0; i < n; i++) {
      if (!ht.has(inner[i])) ht.set(inner[i], []);
      ht.get(inner[i]).push(i);
    }
    let hashMatches = 0;
    for (const k of outer) {
      const m = ht.get(k);
      if (m) hashMatches += m.length;
    }
    const hashMs = Date.now() - t1;

    console.log(`    INLJ: ${inljMs}ms (with pre-built index) vs Hash: ${hashMs}ms (build + probe)`);
    assert.equal(inljResult.left.length, 3);
  });

  it('stats tracked', () => {
    const index = IndexNestedLoopJoin.buildIndex([1, 2, 3]);
    const inlj = new IndexNestedLoopJoin();
    inlj.join([1, 5], index);

    const stats = inlj.getStats();
    assert.equal(stats.outerRows, 2);
    assert.equal(stats.indexLookups, 2);
    assert.equal(stats.matches, 1);
  });
});
