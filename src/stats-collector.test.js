// stats-collector.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { StatsCollector } from './stats-collector.js';

describe('StatsCollector', () => {
  const collector = new StatsCollector();
  const employees = Array.from({ length: 1000 }, (_, i) => ({
    id: i,
    name: `emp_${i}`,
    dept: ['eng', 'sales', 'hr', 'marketing'][i % 4],
    salary: 50000 + Math.floor(Math.random() * 100000),
    manager_id: i < 10 ? null : Math.floor(i / 10),
  }));

  it('analyze collects basic stats', () => {
    const stats = collector.analyze('employees', employees);
    assert.equal(stats.rowCount, 1000);
    assert.ok(stats.columns.id);
    assert.ok(stats.columns.dept);
    assert.ok(stats.columns.salary);
  });

  it('distinct values', () => {
    const cs = collector.getColumnStats('employees', 'dept');
    assert.equal(cs.distinctValues, 4);
    assert.equal(cs.rowCount, 1000);
  });

  it('min/max for numeric column', () => {
    const cs = collector.getColumnStats('employees', 'id');
    assert.equal(cs.min, 0);
    assert.equal(cs.max, 999);
  });

  it('null fraction', () => {
    const cs = collector.getColumnStats('employees', 'manager_id');
    assert.ok(cs.nullFraction > 0);
    assert.ok(cs.nullFraction < 0.05); // 10/1000 = 1%
  });

  it('most common values', () => {
    const cs = collector.getColumnStats('employees', 'dept');
    assert.ok(cs.mostCommonValues.length <= 10);
    assert.ok(cs.mostCommonValues[0].frequency > 0);
    // Each dept appears ~250 times
    assert.ok(cs.mostCommonValues[0].frequency > 0.2);
  });

  it('histogram for numeric column', () => {
    const cs = collector.getColumnStats('employees', 'salary');
    assert.ok(cs.histogram);
    assert.ok(cs.histogram.buckets.length > 0);
    assert.ok(cs.histogram.buckets[0].lo <= cs.histogram.buckets[0].hi);
  });

  it('selectivity: equality', () => {
    const cs = collector.getColumnStats('employees', 'dept');
    const sel = cs.selectivityEq('eng');
    assert.ok(sel > 0.2 && sel < 0.3); // ~25%
  });

  it('selectivity: range (GT)', () => {
    const cs = collector.getColumnStats('employees', 'id');
    const sel = cs.selectivityGt(500);
    assert.ok(Math.abs(sel - 0.5) < 0.05); // ~50%
  });

  it('selectivity: BETWEEN', () => {
    const cs = collector.getColumnStats('employees', 'id');
    const sel = cs.selectivityBetween(200, 400);
    assert.ok(Math.abs(sel - 0.2) < 0.05); // ~20%
  });

  it('estimate rows', () => {
    const est = collector.estimateRows('employees', { column: 'dept', op: 'EQ', value: 'eng' });
    assert.ok(est > 200 && est < 300); // ~250
  });

  it('sampling support', () => {
    const sampled = new StatsCollector();
    sampled.analyze('emp_sampled', employees, { sampleRate: 0.1 });
    const cs = sampled.getColumnStats('emp_sampled', 'dept');
    assert.ok(cs.distinctValues <= 4);
    assert.equal(cs.rowCount, 1000); // Total count preserved
  });
});
