// vectorized-bridge.test.js — Test the bridge between row-oriented and vectorized execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { canVectorize, vectorizedGroupBy, benchmarkVectorizedAgg } from './vectorized-bridge.js';

describe('canVectorize', () => {
  it('returns true for simple GROUP BY with SUM', () => {
    const ast = {
      groupBy: ['department'],
      columns: [
        { type: 'column', name: 'department' },
        { type: 'aggregate', func: 'SUM', arg: 'salary', alias: 'total_salary' },
      ],
    };
    assert.ok(canVectorize(ast));
  });

  it('returns false without GROUP BY', () => {
    const ast = {
      columns: [{ type: 'aggregate', func: 'COUNT', arg: '*' }],
    };
    assert.ok(!canVectorize(ast));
  });

  it('returns false for expression arguments', () => {
    const ast = {
      groupBy: ['dept'],
      columns: [
        { type: 'aggregate', func: 'SUM', arg: { type: 'binary', op: '*', left: 'qty', right: 'price' } },
      ],
    };
    assert.ok(!canVectorize(ast));
  });

  it('returns false for unsupported aggregate functions', () => {
    const ast = {
      groupBy: ['dept'],
      columns: [
        { type: 'aggregate', func: 'PERCENTILE', arg: 'salary' },
      ],
    };
    assert.ok(!canVectorize(ast));
  });

  it('returns true for multiple aggregates', () => {
    const ast = {
      groupBy: ['region'],
      columns: [
        { type: 'column', name: 'region' },
        { type: 'aggregate', func: 'SUM', arg: 'sales', alias: 'total' },
        { type: 'aggregate', func: 'COUNT', arg: '*', alias: 'cnt' },
        { type: 'aggregate', func: 'AVG', arg: 'sales', alias: 'avg_sales' },
      ],
    };
    assert.ok(canVectorize(ast));
  });
});

describe('vectorizedGroupBy', () => {
  it('groups and aggregates correctly', () => {
    const rows = [
      { dept: 'Engineering', salary: 100 },
      { dept: 'Sales', salary: 80 },
      { dept: 'Engineering', salary: 120 },
      { dept: 'Sales', salary: 90 },
      { dept: 'Engineering', salary: 110 },
    ];
    
    const ast = {
      groupBy: ['dept'],
      columns: [
        { type: 'column', name: 'dept' },
        { type: 'aggregate', func: 'SUM', arg: 'salary', alias: 'total' },
        { type: 'aggregate', func: 'COUNT', arg: 'salary', alias: 'cnt' },
        { type: 'aggregate', func: 'AVG', arg: 'salary', alias: 'avg' },
      ],
    };
    
    const result = vectorizedGroupBy(rows, ast);
    assert.equal(result.length, 2);
    
    const eng = result.find(r => r.dept === 'Engineering');
    const sales = result.find(r => r.dept === 'Sales');
    
    assert.equal(eng.total, 330);
    assert.equal(eng.cnt, 3);
    assert.equal(eng.avg, 110);
    
    assert.equal(sales.total, 170);
    assert.equal(sales.cnt, 2);
    assert.equal(sales.avg, 85);
  });

  it('handles empty input', () => {
    const ast = {
      groupBy: ['dept'],
      columns: [{ type: 'aggregate', func: 'SUM', arg: 'salary', alias: 'total' }],
    };
    assert.deepEqual(vectorizedGroupBy([], ast), []);
  });

  it('COUNT(*) works', () => {
    const rows = [
      { category: 'A', val: 1 },
      { category: 'A', val: 2 },
      { category: 'B', val: 3 },
    ];
    
    const ast = {
      groupBy: ['category'],
      columns: [
        { type: 'column', name: 'category' },
        { type: 'aggregate', func: 'COUNT', arg: '*', alias: 'cnt' },
      ],
    };
    
    const result = vectorizedGroupBy(rows, ast);
    assert.equal(result.length, 2);
    assert.equal(result.find(r => r.category === 'A').cnt, 2);
    assert.equal(result.find(r => r.category === 'B').cnt, 1);
  });

  it('MIN and MAX work', () => {
    const rows = [
      { team: 'X', score: 10 },
      { team: 'X', score: 50 },
      { team: 'X', score: 30 },
      { team: 'Y', score: 20 },
      { team: 'Y', score: 40 },
    ];
    
    const ast = {
      groupBy: ['team'],
      columns: [
        { type: 'column', name: 'team' },
        { type: 'aggregate', func: 'MIN', arg: 'score', alias: 'min_score' },
        { type: 'aggregate', func: 'MAX', arg: 'score', alias: 'max_score' },
      ],
    };
    
    const result = vectorizedGroupBy(rows, ast);
    const x = result.find(r => r.team === 'X');
    const y = result.find(r => r.team === 'Y');
    
    assert.equal(x.min_score, 10);
    assert.equal(x.max_score, 50);
    assert.equal(y.min_score, 20);
    assert.equal(y.max_score, 40);
  });
});

describe('Vectorized vs Row Benchmark', () => {
  it('benchmarks 100K row aggregation', () => {
    const N = 100_000;
    const departments = ['Eng', 'Sales', 'Marketing', 'Support', 'HR'];
    const rows = [];
    for (let i = 0; i < N; i++) {
      rows.push({
        department: departments[i % departments.length],
        salary: Math.random() * 100000 | 0,
      });
    }
    
    const result = benchmarkVectorizedAgg(rows, 'department', 'salary');
    console.log(`    100K rows, ${result.groups} groups:`);
    console.log(`    Row-at-a-time: ${result.rowTime.toFixed(1)}ms`);
    console.log(`    Vectorized:    ${result.vecTime.toFixed(1)}ms`);
    console.log(`    Ratio:         ${result.speedup.toFixed(2)}x`);
    
    assert.equal(result.groups, 5);
  });
});
