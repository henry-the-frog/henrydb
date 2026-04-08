// bloom-join.test.js — Tests for Bloom filter semi-join reducer
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomJoin } from './bloom-join.js';

describe('BloomJoin', () => {

  it('basic join: correct matches', () => {
    const leftKeys = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const rightKeys = [3, 5, 7, 9]; // Only 4 match

    const bj = new BloomJoin();
    const result = bj.join(leftKeys, rightKeys, null, null);

    assert.equal(result.left.length, 4);
    assert.equal(result.right.length, 4);
  });

  it('no matches returns empty', () => {
    const leftKeys = [1, 2, 3, 4, 5];
    const rightKeys = [10, 20, 30];

    const bj = new BloomJoin();
    const result = bj.join(leftKeys, rightKeys, null, null);

    assert.equal(result.left.length, 0);
  });

  it('many-to-many join', () => {
    const leftKeys = [1, 1, 2, 2, 3];
    const rightKeys = [1, 2, 2];

    const bj = new BloomJoin();
    const result = bj.join(leftKeys, rightKeys, null, null);

    // 1×1: 2 left × 1 right = 2, 2×2: 2 left × 2 right = 4 → 6 total
    assert.equal(result.left.length, 6);
  });

  it('Bloom filter eliminates non-matching rows early', () => {
    // Right side: only keys 1-100
    const rightKeys = Array.from({ length: 100 }, (_, i) => i);
    // Left side: keys 0-9999 (90% won't match)
    const leftKeys = Array.from({ length: 10000 }, (_, i) => i);

    const bj = new BloomJoin();
    const bf = bj.buildFilter(rightKeys);
    const candidates = bj.probeFilter(leftKeys, bf);

    // Should filter out most of the 9900 non-matching rows
    // With 1% FP rate, expect ~100 matches + ~99 false positives ≈ 199
    assert.ok(candidates.length < 250, `Expected < 250 candidates, got ${candidates.length}`);
    assert.ok(candidates.length >= 100, `Should have at least 100 true matches, got ${candidates.length}`);

    const stats = bj.getStats();
    assert.ok(stats.filteredRows > 9500, `Should filter > 9500, filtered ${stats.filteredRows}`);
  });

  it('benchmark: Bloom join vs standard hash join on 100K × 1K', () => {
    const n = 100000;
    const m = 1000;
    const leftKeys = Array.from({ length: n }, (_, i) => i);
    const rightKeys = Array.from({ length: m }, (_, i) => i * 100); // 0, 100, 200, ..., 99900

    // Bloom join
    const bj = new BloomJoin();
    const t0 = Date.now();
    const bloomResult = bj.join(leftKeys, rightKeys, null, null);
    const bloomMs = Date.now() - t0;

    // Standard hash join
    const t1 = Date.now();
    const ht = new Map();
    for (let i = 0; i < m; i++) {
      const key = rightKeys[i];
      if (!ht.has(key)) ht.set(key, []);
      ht.get(key).push(i);
    }
    const stdLeft = [], stdRight = [];
    for (let i = 0; i < n; i++) {
      const matches = ht.get(leftKeys[i]);
      if (matches) for (const j of matches) { stdLeft.push(i); stdRight.push(j); }
    }
    const stdMs = Date.now() - t1;

    console.log(`    Bloom: ${bloomMs}ms (filtered ${bj.stats.filteredRows}/${n}) vs Standard: ${stdMs}ms`);
    assert.equal(bloomResult.left.length, stdLeft.length);
  });

  it('multiWayReduce: filter with multiple Bloom filters', () => {
    const mainKeys = Array.from({ length: 10000 }, (_, i) => i);
    const side1Keys = Array.from({ length: 100 }, (_, i) => i); // 0-99
    const side2Keys = Array.from({ length: 100 }, (_, i) => i * 2); // 0, 2, 4, ..., 198

    const bj = new BloomJoin();
    const reduced = bj.multiWayReduce(mainKeys, [side1Keys, side2Keys]);

    // Should pass only keys in intersection: even numbers 0-98 = 50 keys
    // Plus Bloom filter false positives
    assert.ok(reduced.length < 150, `Expected < 150, got ${reduced.length}`);
    assert.ok(reduced.length >= 50, `Should have at least 50 true matches`);
  });

  it('stats tracked', () => {
    const bj = new BloomJoin();
    bj.join([1, 2, 3], [2, 3, 4], null, null);

    const stats = bj.getStats();
    assert.ok(stats.buildTimeMs >= 0);
    assert.ok(stats.probeTimeMs >= 0);
    assert.equal(stats.inputRows, 3);
    assert.ok(stats.passedRows <= 3);
  });
});
