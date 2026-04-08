// hash-aggregate.test.js — Tests for hash aggregation with spill support
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HashAggregate } from './hash-aggregate.js';

describe('HashAggregate', () => {

  it('basic SUM', () => {
    const ha = new HashAggregate();
    const result = ha.aggregate(
      ['A', 'B', 'A', 'B', 'A'],
      [10, 20, 30, 40, 50],
      'SUM'
    );
    assert.equal(result.length, 2);
    assert.equal(result.find(r => r.group === 'A').value, 90);
    assert.equal(result.find(r => r.group === 'B').value, 60);
  });

  it('COUNT', () => {
    const groups = ['X', 'Y', 'X', 'Z', 'Y', 'X'];
    const values = [1, 1, 1, 1, 1, 1];
    const ha = new HashAggregate();
    const result = ha.aggregate(groups, values, 'COUNT');
    assert.equal(result.find(r => r.group === 'X').value, 3);
    assert.equal(result.find(r => r.group === 'Y').value, 2);
    assert.equal(result.find(r => r.group === 'Z').value, 1);
  });

  it('AVG', () => {
    const ha = new HashAggregate();
    const result = ha.aggregate(['A', 'A', 'B'], [10, 20, 30], 'AVG');
    assert.equal(result.find(r => r.group === 'A').value, 15);
    assert.equal(result.find(r => r.group === 'B').value, 30);
  });

  it('MIN and MAX', () => {
    const ha = new HashAggregate();
    const minResult = ha.aggregate(['A', 'A', 'A'], [30, 10, 50], 'MIN');
    assert.equal(minResult[0].value, 10);

    const maxResult = ha.aggregate(['A', 'A', 'A'], [30, 10, 50], 'MAX');
    assert.equal(maxResult[0].value, 50);
  });

  it('multiAggregate', () => {
    const groups = ['US', 'EU', 'US', 'EU', 'US'];
    const amounts = [100, 200, 300, 400, 500];
    const quantities = [1, 2, 3, 4, 5];

    const ha = new HashAggregate();
    const result = ha.multiAggregate(groups, [
      { fn: 'SUM', alias: 'total_amount', values: amounts },
      { fn: 'AVG', alias: 'avg_qty', values: quantities },
      { fn: 'COUNT', alias: 'cnt', values: amounts },
    ]);

    const us = result.find(r => r.group === 'US');
    assert.equal(us.total_amount, 900);
    assert.equal(us.avg_qty, 3);
    assert.equal(us.cnt, 3);
  });

  it('spilling: large number of groups', () => {
    const n = 50000;
    const groups = Array.from({ length: n }, (_, i) => `g${i}`);
    const values = Array.from({ length: n }, (_, i) => i);

    const ha = new HashAggregate({ memoryBudget: 10000 });
    const result = ha.aggregate(groups, values, 'SUM');

    assert.equal(result.length, n);
    assert.ok(ha.stats.spills > 0, 'Should have spilled');
  });

  it('benchmark: 500K rows, 100 groups', () => {
    const n = 500000;
    const groups = Array.from({ length: n }, (_, i) => `g${i % 100}`);
    const values = Array.from({ length: n }, (_, i) => i % 1000);

    const ha = new HashAggregate();
    const t0 = Date.now();
    const result = ha.aggregate(groups, values, 'SUM');
    const ms = Date.now() - t0;

    console.log(`    500K/100 groups: ${ms}ms`);
    assert.equal(result.length, 100);
  });

  it('stats tracked', () => {
    const ha = new HashAggregate();
    ha.aggregate(['A', 'B', 'A'], [1, 2, 3], 'SUM');

    const stats = ha.getStats();
    assert.equal(stats.totalRows, 3);
    assert.equal(stats.groups, 2);
    assert.ok(stats.timeMs >= 0);
  });
});
