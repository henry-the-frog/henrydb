// statistics-collector.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { StatisticsCollector } from './statistics-collector.js';

describe('StatisticsCollector', () => {
  it('analyze and query', () => {
    const sc = new StatisticsCollector();
    sc.analyze('users', 'age', [25, 30, 35, 40, 45, 30, 35]);
    const stats = sc.get('users', 'age');
    assert.equal(stats.rowCount, 7);
    assert.equal(stats.min, 25);
    assert.equal(stats.max, 45);
  });

  it('estimate selectivity', () => {
    const sc = new StatisticsCollector();
    sc.analyze('users', 'dept', ['eng', 'sales', 'eng', 'hr']);
    const sel = sc.estimateSelectivity('users', 'dept', 'eng');
    assert.ok(sel > 0 && sel < 1);
  });
});
