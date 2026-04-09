// table-statistics.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { StatisticsCollector } from './table-statistics.js';

let collector;
const rows = [];

describe('StatisticsCollector', () => {
  beforeEach(() => {
    collector = new StatisticsCollector();
    rows.length = 0;
    for (let i = 1; i <= 1000; i++) {
      rows.push({
        id: i,
        name: `User ${i}`,
        age: 20 + (i % 50),
        dept: ['Engineering', 'Marketing', 'Sales', 'HR'][i % 4],
        salary: 40000 + (i * 50),
        nullable_col: i % 10 === 0 ? null : i,
      });
    }
  });

  test('analyze generates basic stats', () => {
    const stats = collector.analyze('users', rows, ['id', 'age', 'dept']);
    assert.equal(stats.totalRows, 1000);
    assert.equal(stats.tableName, 'users');
    assert.ok(stats.lastAnalyzed);
  });

  test('column min/max', () => {
    collector.analyze('users', rows, ['id', 'age', 'salary']);
    const idStats = collector.getStats('users').getColumn('id');
    assert.equal(idStats.minValue, 1);
    assert.equal(idStats.maxValue, 1000);
    
    const ageStats = collector.getStats('users').getColumn('age');
    assert.equal(ageStats.minValue, 20);
    assert.equal(ageStats.maxValue, 69);
  });

  test('distinct count', () => {
    collector.analyze('users', rows, ['dept', 'age']);
    const deptStats = collector.getStats('users').getColumn('dept');
    assert.equal(deptStats.distinctCount, 4);
    
    const ageStats = collector.getStats('users').getColumn('age');
    assert.equal(ageStats.distinctCount, 50);
  });

  test('null fraction', () => {
    collector.analyze('users', rows, ['nullable_col']);
    const stats = collector.getStats('users').getColumn('nullable_col');
    assert.ok(stats.nullFraction > 0.05);
    assert.ok(stats.nullFraction < 0.15);
    assert.equal(stats.nullCount, 100);
  });

  test('histogram generation', () => {
    collector.analyze('users', rows, ['salary']);
    const stats = collector.getStats('users').getColumn('salary');
    assert.ok(stats.histogram.length > 2);
    assert.equal(stats.histogram[0], stats.minValue);
    assert.equal(stats.histogram[stats.histogram.length - 1], stats.maxValue);
  });

  test('most common values', () => {
    collector.analyze('users', rows, ['dept']);
    const stats = collector.getStats('users').getColumn('dept');
    assert.ok(stats.mostCommonValues.length <= 10);
    assert.ok(stats.mostCommonValues.length >= 4);
    assert.ok(stats.mostCommonValues[0].frequency > 0);
    assert.ok(stats.mostCommonValues[0].count > 0);
  });

  test('selectivity estimation: equality', () => {
    collector.analyze('users', rows, ['dept']);
    const sel = collector.estimateSelectivity('users', 'dept', '=', 'Engineering');
    assert.ok(sel > 0.2); // ~25% of rows
    assert.ok(sel < 0.3);
  });

  test('selectivity estimation: range', () => {
    collector.analyze('users', rows, ['age']);
    const sel = collector.estimateSelectivity('users', 'age', '<', 30);
    // age ranges 20-69, so < 30 is about 10/50 = 20%
    assert.ok(sel > 0.1);
    assert.ok(sel < 0.4);
  });

  test('selectivity estimation: IS NULL', () => {
    collector.analyze('users', rows, ['nullable_col']);
    const sel = collector.estimateSelectivity('users', 'nullable_col', 'IS NULL');
    assert.ok(sel > 0.05);
    assert.ok(sel < 0.15);
  });

  test('selectivity estimation: BETWEEN', () => {
    collector.analyze('users', rows, ['age']);
    const sel = collector.estimateSelectivity('users', 'age', 'BETWEEN', [30, 40]);
    // 11 out of 50 distinct values
    assert.ok(sel > 0.1);
    assert.ok(sel < 0.5);
  });

  test('row count estimation with multiple predicates', () => {
    collector.analyze('users', rows, ['age', 'dept']);
    const est = collector.estimateRowCount('users', [
      { column: 'dept', op: '=', value: 'Engineering' },
      { column: 'age', op: '>', value: 40 },
    ]);
    assert.ok(est > 10);
    assert.ok(est < 500);
  });

  test('correlation tracks physical ordering', () => {
    collector.analyze('users', rows, ['id']);
    const stats = collector.getStats('users').getColumn('id');
    // id is perfectly ordered
    assert.ok(stats.correlation > 0.5);
  });

  test('avg width computed', () => {
    collector.analyze('users', rows, ['name']);
    const stats = collector.getStats('users').getColumn('name');
    assert.ok(stats.avgWidth > 0);
  });

  test('no stats returns default estimates', () => {
    const sel = collector.estimateSelectivity('unknown_table', 'col', '=', 'val');
    assert.equal(sel, 0.5);
    
    const est = collector.estimateRowCount('unknown_table');
    assert.equal(est, 1000);
  });

  test('re-analyze increments counter', () => {
    collector.analyze('users', rows, ['id']);
    collector.analyze('users', rows, ['id']);
    assert.equal(collector.getStats('users').analyzeCount, 2);
  });

  test('sampling mode works', () => {
    const sampled = new StatisticsCollector({ sampleRatio: 0.1 });
    sampled.analyze('users', rows, ['id']);
    const stats = sampled.getStats('users');
    assert.equal(stats.totalRows, 1000);
    // Stats should still be reasonable despite sampling
    const idStats = stats.getColumn('id');
    assert.ok(idStats.minValue <= 100);
    assert.ok(idStats.maxValue >= 900);
  });
});
