// hash-group-by.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HashGroupBy } from './hash-group-by.js';

const data = [
  { dept: 'eng', name: 'Alice', salary: 120000, year: 2024 },
  { dept: 'eng', name: 'Bob', salary: 110000, year: 2024 },
  { dept: 'eng', name: 'Charlie', salary: 105000, year: 2023 },
  { dept: 'sales', name: 'Dana', salary: 95000, year: 2024 },
  { dept: 'sales', name: 'Eve', salary: 90000, year: 2024 },
  { dept: 'hr', name: 'Frank', salary: 85000, year: 2023 },
];

describe('HashGroupBy', () => {
  it('SUM by single column', () => {
    const gb = new HashGroupBy(['dept'], [{ col: 'salary', func: 'SUM', alias: 'total' }]);
    gb.addAll(data);
    const results = gb.results();
    const eng = results.find(r => r.dept === 'eng');
    assert.equal(eng.total, 335000);
  });

  it('COUNT by single column', () => {
    const gb = new HashGroupBy(['dept'], [{ col: 'name', func: 'COUNT', alias: 'cnt' }]);
    gb.addAll(data);
    assert.equal(gb.results().find(r => r.dept === 'eng').cnt, 3);
  });

  it('AVG', () => {
    const gb = new HashGroupBy(['dept'], [{ col: 'salary', func: 'AVG', alias: 'avg_sal' }]);
    gb.addAll(data);
    const eng = gb.results().find(r => r.dept === 'eng');
    assert.ok(Math.abs(eng.avg_sal - 111666.67) < 1);
  });

  it('MIN and MAX', () => {
    const gb = new HashGroupBy(['dept'], [
      { col: 'salary', func: 'MIN', alias: 'min_sal' },
      { col: 'salary', func: 'MAX', alias: 'max_sal' },
    ]);
    gb.addAll(data);
    const eng = gb.results().find(r => r.dept === 'eng');
    assert.equal(eng.min_sal, 105000);
    assert.equal(eng.max_sal, 120000);
  });

  it('COUNT_DISTINCT', () => {
    const gb = new HashGroupBy(['dept'], [{ col: 'year', func: 'COUNT_DISTINCT', alias: 'years' }]);
    gb.addAll(data);
    assert.equal(gb.results().find(r => r.dept === 'eng').years, 2);
  });

  it('multiple group columns', () => {
    const gb = new HashGroupBy(['dept', 'year'], [{ col: 'salary', func: 'SUM', alias: 'total' }]);
    gb.addAll(data);
    assert.equal(gb.groupCount, 4); // eng/2024, eng/2023, sales/2024, hr/2023
  });

  it('multiple aggregates', () => {
    const gb = new HashGroupBy(['dept'], [
      { col: 'salary', func: 'SUM', alias: 'total' },
      { col: 'salary', func: 'AVG', alias: 'avg' },
      { col: 'name', func: 'COUNT', alias: 'cnt' },
    ]);
    gb.addAll(data);
    const results = gb.results();
    assert.equal(results.length, 3);
    assert.ok(results[0].total > 0);
    assert.ok(results[0].avg > 0);
    assert.ok(results[0].cnt > 0);
  });

  it('benchmark: 100K rows group by', () => {
    const rows = Array.from({ length: 100000 }, (_, i) => ({
      dept: `dept_${i % 10}`,
      value: Math.random() * 1000,
    }));
    const gb = new HashGroupBy(['dept'], [
      { col: 'value', func: 'SUM', alias: 'total' },
      { col: 'value', func: 'AVG', alias: 'avg' },
      { col: 'value', func: 'COUNT', alias: 'cnt' },
    ]);
    const t0 = Date.now();
    gb.addAll(rows);
    const results = gb.results();
    console.log(`    100K group by: ${Date.now() - t0}ms, ${results.length} groups`);
    assert.equal(results.length, 10);
    assert.equal(results[0].cnt, 10000);
  });
});
