// selectivity.test.js — Tests for shared selectivity estimator
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { estimateSelectivity, getColumnNdv, SELECTIVITY_DEFAULTS } from './selectivity.js';

describe('Shared selectivity estimator', () => {
  // No stats — should use heuristics
  describe('without stats', () => {
    it('equality returns default 0.1', () => {
      const sel = estimateSelectivity(
        { type: 'COMPARE', op: 'EQ', left: { name: 'x' }, right: { value: 1 } },
        't', null
      );
      assert.equal(sel, SELECTIVITY_DEFAULTS.equality);
    });

    it('range returns default 0.33', () => {
      const sel = estimateSelectivity(
        { type: 'COMPARE', op: 'GT', left: { name: 'x' }, right: { value: 10 } },
        't', null
      );
      assert.equal(sel, SELECTIVITY_DEFAULTS.range);
    });

    it('AND multiplies selectivities', () => {
      const sel = estimateSelectivity(
        {
          type: 'AND',
          left: { type: 'COMPARE', op: 'EQ', left: { name: 'x' }, right: { value: 1 } },
          right: { type: 'COMPARE', op: 'EQ', left: { name: 'y' }, right: { value: 2 } },
        },
        't', null
      );
      assert.ok(Math.abs(sel - SELECTIVITY_DEFAULTS.equality * SELECTIVITY_DEFAULTS.equality) < 0.001);
    });

    it('OR uses inclusion-exclusion', () => {
      const sel = estimateSelectivity(
        {
          type: 'OR',
          left: { type: 'COMPARE', op: 'EQ', left: { name: 'x' }, right: { value: 1 } },
          right: { type: 'COMPARE', op: 'EQ', left: { name: 'y' }, right: { value: 2 } },
        },
        't', null
      );
      const e = SELECTIVITY_DEFAULTS.equality;
      assert.ok(Math.abs(sel - (e + e - e * e)) < 0.001);
    });

    it('null where returns 1.0', () => {
      assert.equal(estimateSelectivity(null, 't', null), 1.0);
    });
  });

  // With stats
  describe('with stats', () => {
    const tableStats = new Map([
      ['users', {
        columns: {
          status: { distinct: 5 },
          age: {
            distinct: 50,
            histogram: [
              { lo: 0, hi: 20, count: 200 },
              { lo: 20, hi: 40, count: 400 },
              { lo: 40, hi: 60, count: 300 },
              { lo: 60, hi: 80, count: 100 },
            ]
          }
        }
      }]
    ]);

    it('equality uses 1/ndistinct', () => {
      const sel = estimateSelectivity(
        { type: 'COMPARE', op: 'EQ', left: { name: 'status' }, right: { value: 'admin' } },
        'users', tableStats
      );
      assert.equal(sel, 0.2); // 1/5
    });

    it('range uses histogram', () => {
      // age > 40: buckets with hi >= 40 contribute
      // bucket [40,60]: all 300 rows match
      // bucket [60,80]: all 100 rows match
      // bucket [20,40]: frac = (40-40)/(40-20) = 0 → 0 rows
      // Total: 400/1000 = 0.4
      const sel = estimateSelectivity(
        { type: 'COMPARE', op: 'GT', left: { name: 'age' }, right: { value: 40 } },
        'users', tableStats
      );
      assert.ok(sel >= 0.3 && sel <= 0.5, `Expected ~0.4, got ${sel}`);
    });

    it('inequality uses 1 - 1/ndistinct', () => {
      const sel = estimateSelectivity(
        { type: 'COMPARE', op: 'NE', left: { name: 'status' }, right: { value: 'admin' } },
        'users', tableStats
      );
      assert.equal(sel, 0.8); // 1 - 1/5
    });
  });

  describe('getColumnNdv', () => {
    const tableStats = new Map([
      ['t', { columns: { x: { distinct: 10 } } }]
    ]);

    it('returns distinct count', () => {
      assert.equal(getColumnNdv('t', 'x', tableStats), 10);
    });

    it('handles qualified column name', () => {
      assert.equal(getColumnNdv('t', 't.x', tableStats), 10);
    });

    it('returns null for unknown column', () => {
      assert.equal(getColumnNdv('t', 'y', tableStats), null);
    });

    it('returns null for unknown table', () => {
      assert.equal(getColumnNdv('unknown', 'x', tableStats), null);
    });
  });
});
