// band-join.test.js — Tests for band join (range predicates)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BandJoin } from './band-join.js';

describe('BandJoin', () => {

  it('basic range join', () => {
    const leftValues = [5, 15, 25];
    const rightLo = [0, 10, 20];
    const rightHi = [10, 20, 30];

    const bj = new BandJoin();
    const { left, right } = bj.join(leftValues, rightLo, rightHi);

    // 5 in [0,10], 15 in [10,20], 25 in [20,30]
    assert.equal(left.length, 3);
  });

  it('value matches multiple ranges', () => {
    const leftValues = [15];
    const rightLo = [10, 12, 14, 20];
    const rightHi = [20, 18, 16, 30];

    const bj = new BandJoin();
    const { left } = bj.join(leftValues, rightLo, rightHi);

    // 15 in [10,20], [12,18], [14,16] — 3 matches
    assert.equal(left.length, 3);
  });

  it('no matches', () => {
    const bj = new BandJoin();
    const { left } = bj.join([100], [0, 10], [5, 20]);
    assert.equal(left.length, 0); // 100 not in [0,5] or [10,20]
  });

  it('optimized version: same results', () => {
    const leftValues = [5, 15, 25, 35];
    const rightLo = [0, 10, 20, 30];
    const rightHi = [12, 22, 32, 42];

    const bj = new BandJoin();
    const basic = bj.join(leftValues, rightLo, rightHi);
    
    const bj2 = new BandJoin();
    const optimized = bj2.joinOptimized(leftValues, rightLo, rightHi);

    assert.equal(basic.left.length, optimized.left.length);
  });

  it('temporal join: events within sessions', () => {
    // Sessions: [0-100], [200-300], [500-600]
    const sessionStart = [0, 200, 500];
    const sessionEnd = [100, 300, 600];
    // Events at times: 50, 150, 250, 550
    const eventTimes = [50, 150, 250, 550];

    const bj = new BandJoin();
    const { left, right } = bj.join(eventTimes, sessionStart, sessionEnd);

    // 50 in [0,100], 250 in [200,300], 550 in [500,600]
    // 150 not in any session
    assert.equal(left.length, 3);
  });

  it('benchmark: 10K events × 1K ranges', () => {
    const events = Array.from({ length: 10000 }, (_, i) => i);
    const rangeLo = Array.from({ length: 1000 }, (_, i) => i * 10);
    const rangeHi = Array.from({ length: 1000 }, (_, i) => i * 10 + 15);

    const bj = new BandJoin();
    const t0 = Date.now();
    const result = bj.joinOptimized(events, rangeLo, rangeHi);
    const ms = Date.now() - t0;

    console.log(`    10K×1K band join: ${ms}ms, ${result.left.length} matches`);
    assert.ok(result.left.length > 0);
  });

  it('stats tracked', () => {
    const bj = new BandJoin();
    bj.join([5], [0], [10]);

    const stats = bj.getStats();
    assert.equal(stats.leftRows, 1);
    assert.equal(stats.rightRows, 1);
    assert.equal(stats.matches, 1);
  });
});
