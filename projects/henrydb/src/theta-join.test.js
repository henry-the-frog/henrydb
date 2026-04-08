// theta-join.test.js — Tests for theta join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ThetaJoin } from './theta-join.js';

describe('ThetaJoin', () => {

  it('equality predicate', () => {
    const left = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const right = [{ a_id: 2 }, { a_id: 3 }, { a_id: 4 }];

    const tj = new ThetaJoin();
    const { left: lIdx } = tj.join(left, right, (l, r) => l.id === r.a_id);
    assert.equal(lIdx.length, 2);
  });

  it('inequality predicate (greater-than)', () => {
    const left = [{ val: 10 }, { val: 20 }, { val: 30 }];
    const right = [{ val: 15 }, { val: 25 }];

    const tj = new ThetaJoin();
    const { left: lIdx } = tj.join(left, right, (l, r) => l.val > r.val);

    // 20 > 15 ✓, 30 > 15 ✓, 30 > 25 ✓ → 3
    assert.equal(lIdx.length, 3);
  });

  it('complex predicate', () => {
    const left = [{ x: 5, y: 10 }, { x: 15, y: 20 }];
    const right = [{ lo: 0, hi: 12 }, { lo: 10, hi: 25 }];

    const tj = new ThetaJoin();
    const { left: lIdx } = tj.join(left, right,
      (l, r) => l.x >= r.lo && l.x <= r.hi
    );

    // (5,10) in [0,12] ✓, (15,20) in [10,25] ✓ → 2
    assert.equal(lIdx.length, 2);
  });

  it('limit works', () => {
    const left = Array.from({ length: 10 }, (_, i) => ({ id: i }));
    const right = Array.from({ length: 10 }, (_, i) => ({ id: i }));

    const tj = new ThetaJoin();
    const { left: lIdx } = tj.join(left, right, () => true, 5);
    assert.equal(lIdx.length, 5);
  });

  it('blockJoin: same results, different traversal', () => {
    const left = Array.from({ length: 100 }, (_, i) => ({ id: i }));
    const right = Array.from({ length: 100 }, (_, i) => ({ a_id: i * 2 }));

    const tj1 = new ThetaJoin();
    const r1 = tj1.join(left, right, (l, r) => l.id === r.a_id);

    const tj2 = new ThetaJoin();
    const r2 = tj2.blockJoin(left, right, (l, r) => l.id === r.a_id, 16);

    assert.equal(r1.left.length, r2.left.length);
  });

  it('stats tracked', () => {
    const tj = new ThetaJoin();
    tj.join([{ id: 1 }], [{ id: 1 }, { id: 2 }], (l, r) => l.id === r.id);

    const stats = tj.getStats();
    assert.equal(stats.leftRows, 1);
    assert.equal(stats.rightRows, 2);
    assert.equal(stats.comparisons, 2);
    assert.equal(stats.matches, 1);
  });
});
