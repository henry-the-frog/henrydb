// cardinality.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CardinalityEstimator, MultiColumnEstimator } from './cardinality.js';

describe('CardinalityEstimator', () => {
  it('small set exact', () => {
    const ce = new CardinalityEstimator();
    for (let i = 0; i < 100; i++) ce.add(i);
    const est = ce.estimate();
    assert.ok(est >= 80 && est <= 120, `Expected ~100, got ${est}`);
  });

  it('large set approximate', () => {
    const ce = new CardinalityEstimator();
    for (let i = 0; i < 10000; i++) ce.add(i);
    const est = ce.estimate();
    const error = Math.abs(est - 10000) / 10000;
    console.log(`    HLL 10K: estimate=${est}, error=${(error * 100).toFixed(1)}%`);
    assert.ok(error < 0.2, `Error too high: ${error}`);
  });

  it('duplicates dont inflate count', () => {
    const ce = new CardinalityEstimator();
    for (let i = 0; i < 1000; i++) ce.add(i % 100); // Only 100 distinct
    const est = ce.estimate();
    assert.ok(est >= 60 && est <= 160);
  });

  it('merge', () => {
    const a = new CardinalityEstimator();
    const b = new CardinalityEstimator();
    for (let i = 0; i < 500; i++) a.add(i);
    for (let i = 250; i < 750; i++) b.add(i);
    const merged = a.merge(b);
    const est = merged.estimate();
    assert.ok(est >= 500 && est <= 1000); // ~750 distinct
  });

  it('standard error', () => {
    const ce = new CardinalityEstimator(12);
    assert.ok(ce.standardError < 0.03); // p=12 → ~1.6% error
  });
});

describe('MultiColumnEstimator', () => {
  it('single column cardinality', () => {
    const mce = new MultiColumnEstimator();
    for (let i = 0; i < 100; i++) {
      mce.addRow({ dept: `dept_${i % 10}`, name: `person_${i}` }, ['dept', 'name']);
    }
    const deptCard = mce.columnCardinality('dept');
    assert.ok(deptCard >= 5 && deptCard <= 20); // ~10 departments
  });

  it('combined cardinality', () => {
    const mce = new MultiColumnEstimator();
    for (let i = 0; i < 100; i++) {
      mce.addRow({ a: i % 10, b: i % 5 }, ['a', 'b']);
    }
    const combined = mce.combinedCardinality();
    assert.ok(combined >= 5 && combined <= 80); // 10 × 5 = 50 combinations
  });
});
