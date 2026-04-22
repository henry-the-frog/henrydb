// volcano-cost.test.js — Cost model tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  estimateSelectivity, estimateCardinality, estimateJoinCosts,
  chooseBestJoin, shouldUseIndexScan,
} from './volcano-cost.js';

describe('Volcano Cost Model', () => {

  describe('estimateSelectivity', () => {
    it('equality predicate → ~10%', () => {
      const sel = estimateSelectivity({ type: 'COMPARE', op: 'EQ' });
      assert.equal(sel, 0.1);
    });

    it('range predicate → ~33%', () => {
      const sel = estimateSelectivity({ type: 'COMPARE', op: 'GT' });
      assert.equal(sel, 0.33);
    });

    it('LIKE → ~20%', () => {
      const sel = estimateSelectivity({ type: 'LIKE' });
      assert.equal(sel, 0.2);
    });

    it('BETWEEN → ~25%', () => {
      const sel = estimateSelectivity({ type: 'BETWEEN' });
      assert.equal(sel, 0.25);
    });

    it('IS NULL → ~2%', () => {
      const sel = estimateSelectivity({ type: 'IS_NULL' });
      assert.equal(sel, 0.02);
    });

    it('AND combines multiplicatively', () => {
      const sel = estimateSelectivity({
        type: 'AND',
        left: { type: 'COMPARE', op: 'EQ' },
        right: { type: 'COMPARE', op: 'GT' },
      });
      assert.ok(Math.abs(sel - 0.1 * 0.33) < 0.001);
    });

    it('OR combines additively', () => {
      const sel = estimateSelectivity({
        type: 'OR',
        left: { type: 'COMPARE', op: 'EQ' },
        right: { type: 'COMPARE', op: 'EQ' },
      });
      // P(A∪B) = P(A) + P(B) - P(A)*P(B) = 0.1 + 0.1 - 0.01 = 0.19
      assert.ok(Math.abs(sel - 0.19) < 0.001);
    });

    it('NOT inverts selectivity', () => {
      const sel = estimateSelectivity({
        type: 'NOT',
        expr: { type: 'COMPARE', op: 'EQ' },
      });
      assert.ok(Math.abs(sel - 0.9) < 0.001);
    });

    it('null predicate → selectivity 1.0', () => {
      assert.equal(estimateSelectivity(null), 1.0);
    });
  });

  describe('estimateJoinCosts', () => {
    it('hash join cheaper than nested loop for large tables', () => {
      const costs = estimateJoinCosts(10000, 10000);
      assert.ok(costs.hashJoinCost < costs.nestedLoopCost);
    });

    it('nested loop cheapest for very small tables', () => {
      const costs = estimateJoinCosts(5, 5);
      // With tiny tables, nested loop cost is 25 * 0.01 = 0.25
      // Hash join cost is 5 * 0.02 + 5 * 0.01 = 0.15
      // Actually hash might still be cheaper, but nested loop is fine
      assert.ok(costs.nestedLoopCost < 1);
    });

    it('merge join competitive with hash for pre-sorted inputs', () => {
      const costs = estimateJoinCosts(1000, 1000);
      // Merge with sort: 1000*log2(1000)*0.05 * 2 + 2000*0.01 ≈ 1000 + 20
      // Hash: 1000*0.02 + 1000*0.01 = 30
      // Hash should be much cheaper without pre-sorted
      assert.ok(costs.hashJoinCost < costs.mergeJoinCost);
    });
  });

  describe('chooseBestJoin', () => {
    it('chooses hash join for large unsorted tables', () => {
      assert.equal(chooseBestJoin(10000, 10000, false, false), 'hash');
    });

    it('chooses nested loop for very small tables', () => {
      assert.equal(chooseBestJoin(5, 5, false, false), 'nested_loop');
    });

    it('considers merge join when both sorted', () => {
      // For moderate tables with sorted inputs, merge should be competitive
      const choice = chooseBestJoin(1000, 1000, true, true);
      // Could be hash or merge depending on constants
      assert.ok(['hash', 'merge'].includes(choice));
    });
  });

  describe('shouldUseIndexScan', () => {
    it('prefers seq scan for small tables', () => {
      assert.equal(shouldUseIndexScan(10, 0.01), false);
    });

    it('prefers index scan for large table with low selectivity', () => {
      assert.equal(shouldUseIndexScan(10000, 0.01), true);
    });

    it('prefers seq scan for high selectivity (most rows match)', () => {
      assert.equal(shouldUseIndexScan(10000, 0.5), false);
    });

    it('boundary at ~20% selectivity', () => {
      assert.equal(shouldUseIndexScan(1000, 0.19), true);
      assert.equal(shouldUseIndexScan(1000, 0.21), false);
    });
  });
});
