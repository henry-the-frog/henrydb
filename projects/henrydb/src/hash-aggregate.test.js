// hash-aggregate.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HashAggregate } from './hash-aggregate.js';

describe('HashAggregate', () => {
  it('GROUP BY with SUM', () => {
    const agg = new HashAggregate(['dept'], [{ col: 'salary', fn: 'sum' }]);
    agg.addRow({ dept: 'eng', salary: 100 });
    agg.addRow({ dept: 'eng', salary: 200 });
    agg.addRow({ dept: 'sales', salary: 150 });
    
    const results = agg.getResults();
    const eng = results.find(r => r.dept === 'eng');
    assert.equal(eng.sum_salary, 300);
  });

  it('multiple aggregates', () => {
    const agg = new HashAggregate(['dept'], [
      { col: 'salary', fn: 'sum' },
      { col: 'salary', fn: 'count' },
      { col: 'salary', fn: 'avg' },
    ]);
    agg.addRow({ dept: 'eng', salary: 100 });
    agg.addRow({ dept: 'eng', salary: 200 });
    
    const r = agg.getResults()[0];
    assert.equal(r.sum_salary, 300);
    assert.equal(r.count_salary, 2);
    assert.equal(r.avg_salary, 150);
  });
});
