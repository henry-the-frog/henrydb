// window-functions.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { applyWindowFunctions } from './window-functions.js';

const salesData = [
  { dept: 'eng', name: 'Alice', salary: 120000 },
  { dept: 'eng', name: 'Bob', salary: 110000 },
  { dept: 'eng', name: 'Charlie', salary: 110000 },
  { dept: 'sales', name: 'Dana', salary: 90000 },
  { dept: 'sales', name: 'Eve', salary: 95000 },
];

describe('WindowFunctions', () => {
  it('ROW_NUMBER()', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'ROW_NUMBER',
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'rn',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.deepEqual(eng.map(r => r.rn), [1, 2, 3]);
  });

  it('RANK() with ties', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'RANK',
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'rank',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.deepEqual(eng.map(r => r.rank), [1, 2, 2]); // Alice=1, Bob=Charlie=2
  });

  it('DENSE_RANK() with ties', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'DENSE_RANK',
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'drank',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.deepEqual(eng.map(r => r.drank), [1, 2, 2]); // Dense: no gap
  });

  it('LAG()', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'LAG',
      args: ['salary', 1, 0],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'prev_salary',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.equal(eng[0].prev_salary, 0); // No previous
    assert.equal(eng[1].prev_salary, 120000); // Alice's salary
  });

  it('LEAD()', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'LEAD',
      args: ['salary', 1, null],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'next_salary',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.equal(eng[0].next_salary, 110000);
    assert.equal(eng[2].next_salary, null); // No next
  });

  it('SUM() OVER (running total)', () => {
    const data = [
      { dept: 'eng', salary: 100 },
      { dept: 'eng', salary: 200 },
      { dept: 'eng', salary: 300 },
    ];
    const result = applyWindowFunctions(data, [{
      func: 'SUM',
      args: ['salary'],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'ASC' }],
      alias: 'running_total',
    }]);
    assert.equal(result[0].running_total, 100);
    assert.equal(result[1].running_total, 300);
    assert.equal(result[2].running_total, 600);
  });

  it('AVG() OVER', () => {
    const data = [
      { dept: 'eng', salary: 100 },
      { dept: 'eng', salary: 200 },
      { dept: 'eng', salary: 300 },
    ];
    const result = applyWindowFunctions(data, [{
      func: 'AVG',
      args: ['salary'],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'ASC' }],
      alias: 'running_avg',
    }]);
    assert.equal(result[0].running_avg, 100);
    assert.equal(result[1].running_avg, 150);
    assert.equal(result[2].running_avg, 200);
  });

  it('COUNT() OVER', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'COUNT',
      args: [],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary' }],
      alias: 'running_count',
    }]);
    const sales = result.filter(r => r.dept === 'sales');
    assert.equal(sales[0].running_count, 1);
    assert.equal(sales[1].running_count, 2);
  });

  it('FIRST_VALUE()', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'FIRST_VALUE',
      args: ['name'],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'top_earner',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    assert.equal(eng[0].top_earner, 'Alice');
    assert.equal(eng[2].top_earner, 'Alice');
  });

  it('NTILE()', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'NTILE',
      args: [2],
      partitionBy: ['dept'],
      orderBy: [{ column: 'salary' }],
      alias: 'quartile',
    }]);
    const eng = result.filter(r => r.dept === 'eng');
    // 3 rows into 2 tiles: [1, 1, 2] or [1, 2, 2]
    assert.ok(eng.every(r => r.quartile === 1 || r.quartile === 2));
  });

  it('no partition (whole table)', () => {
    const result = applyWindowFunctions(salesData, [{
      func: 'ROW_NUMBER',
      orderBy: [{ column: 'salary', direction: 'DESC' }],
      alias: 'global_rn',
    }]);
    assert.equal(result[0].global_rn, 1);
    assert.equal(result[4].global_rn, 5);
  });

  it('multiple window functions', () => {
    const result = applyWindowFunctions(salesData, [
      { func: 'ROW_NUMBER', partitionBy: ['dept'], orderBy: [{ column: 'salary', direction: 'DESC' }], alias: 'rn' },
      { func: 'SUM', args: ['salary'], partitionBy: ['dept'], orderBy: [{ column: 'salary' }], alias: 'cum_sum' },
    ]);
    assert.ok(result[0].rn !== undefined);
    assert.ok(result[0].cum_sum !== undefined);
  });
});
