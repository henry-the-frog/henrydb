// column-store.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ColumnStore } from './column-store.js';

describe('ColumnStore', () => {
  it('append and query', () => {
    const cs = new ColumnStore([
      { name: 'id', type: 'int' },
      { name: 'name', type: 'string' },
      { name: 'age', type: 'int' },
    ]);
    cs.appendRow([1, 'Alice', 30]);
    cs.appendRow([2, 'Bob', 25]);
    
    assert.equal(cs.rowCount, 2);
    assert.deepEqual(cs.getColumn('name'), ['Alice', 'Bob']);
  });

  it('aggregations', () => {
    const cs = new ColumnStore([{ name: 'val', type: 'int' }]);
    cs.appendBatch([[10], [20], [30], [40], [50]]);
    
    assert.equal(cs.sum('val'), 150);
    assert.equal(cs.avg('val'), 30);
    assert.equal(cs.min('val'), 10);
    assert.equal(cs.max('val'), 50);
  });

  it('filter and project', () => {
    const cs = new ColumnStore([
      { name: 'name', type: 'string' },
      { name: 'salary', type: 'int' },
    ]);
    cs.appendBatch([['Alice', 100], ['Bob', 200], ['Charlie', 150]]);
    
    const highPaid = cs.filter('salary', s => s >= 150);
    const results = cs.project(['name', 'salary'], highPaid);
    assert.equal(results.length, 2);
  });

  it('groupBy', () => {
    const cs = new ColumnStore([
      { name: 'dept', type: 'string' },
      { name: 'salary', type: 'int' },
    ]);
    cs.appendBatch([['eng', 100], ['eng', 200], ['sales', 150]]);
    
    const grouped = cs.groupBy('dept', 'salary', 'sum');
    const eng = grouped.find(g => g.dept === 'eng');
    assert.equal(eng.sum, 300);
  });

  it('performance: 100K rows columnar scan', () => {
    const cs = new ColumnStore([
      { name: 'id', type: 'int' },
      { name: 'value', type: 'int' },
    ]);
    for (let i = 0; i < 100000; i++) cs.appendRow([i, i * 2]);
    
    const t0 = performance.now();
    const total = cs.sum('value');
    const elapsed = performance.now() - t0;
    
    assert.equal(total, 9999900000);
    console.log(`  100K row SUM: ${elapsed.toFixed(1)}ms`);
  });
});
