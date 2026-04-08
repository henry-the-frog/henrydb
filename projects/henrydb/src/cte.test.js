// cte.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SimpleCTEEngine } from './cte.js';

describe('CTE (Common Table Expressions)', () => {
  const engine = new SimpleCTEEngine();
  engine.addTable('employees', [
    { id: 1, name: 'Alice', dept: 'eng', salary: 120000, manager_id: null },
    { id: 2, name: 'Bob', dept: 'eng', salary: 110000, manager_id: 1 },
    { id: 3, name: 'Charlie', dept: 'eng', salary: 105000, manager_id: 1 },
    { id: 4, name: 'Dana', dept: 'sales', salary: 95000, manager_id: null },
    { id: 5, name: 'Eve', dept: 'sales', salary: 90000, manager_id: 4 },
    { id: 6, name: 'Frank', dept: 'hr', salary: 85000, manager_id: null },
  ]);
  engine.addTable('departments', [
    { dept: 'eng', budget: 500000 },
    { dept: 'sales', budget: 300000 },
    { dept: 'hr', budget: 200000 },
  ]);

  it('single CTE', () => {
    const result = engine.execute({
      ctes: [{
        name: 'high_earners',
        select: '*',
        from: 'employees',
        where: { op: 'GT', left: 'salary', right: 100000 },
        query: { select: '*', from: 'employees', where: { op: 'GT', left: 'salary', right: 100000 } },
      }],
      mainQuery: { select: '*', from: 'high_earners' },
    });
    assert.equal(result.length, 3); // Alice, Bob, Charlie
    assert.ok(result.every(r => r.salary > 100000));
  });

  it('CTE with aggregation', () => {
    const result = engine.execute({
      ctes: [{
        name: 'dept_totals',
        query: {
          from: 'employees',
          groupBy: ['dept'],
          select: [{ agg: 'SUM', column: 'salary', alias: 'total_salary' }, { agg: 'COUNT', column: 'id', alias: 'headcount' }],
        },
      }],
      mainQuery: { select: '*', from: 'dept_totals', orderBy: [{ column: 'total_salary', direction: 'DESC' }] },
    });
    assert.equal(result.length, 3);
    assert.equal(result[0].dept, 'eng');
    assert.equal(result[0].total_salary, 335000);
    assert.equal(result[0].headcount, 3);
  });

  it('CTE with JOIN to base table', () => {
    const result = engine.execute({
      ctes: [{
        name: 'eng_team',
        query: { select: '*', from: 'employees', where: { op: 'EQ', left: 'dept', right: 'eng' } },
      }],
      mainQuery: {
        select: '*',
        from: 'eng_team',
        join: { table: 'departments', on: { left: 'dept', right: 'dept' } },
      },
    });
    assert.equal(result.length, 3);
    assert.ok(result.every(r => r.budget === 500000));
  });

  it('multiple CTEs (chain)', () => {
    const result = engine.execute({
      ctes: [
        {
          name: 'eng_team',
          query: { select: '*', from: 'employees', where: { op: 'EQ', left: 'dept', right: 'eng' } },
        },
        {
          name: 'senior_eng',
          query: { select: '*', from: 'eng_team', where: { op: 'GT', left: 'salary', right: 108000 } },
        },
      ],
      mainQuery: { select: '*', from: 'senior_eng' },
    });
    assert.equal(result.length, 2); // Alice (120K), Bob (110K)
    assert.ok(result.every(r => r.salary > 108000));
  });

  it('CTE with LIMIT', () => {
    const result = engine.execute({
      ctes: [{
        name: 'top3',
        query: {
          select: '*',
          from: 'employees',
          orderBy: [{ column: 'salary', direction: 'DESC' }],
          limit: 3,
        },
      }],
      mainQuery: { select: '*', from: 'top3' },
    });
    assert.equal(result.length, 3);
    assert.equal(result[0].name, 'Alice');
  });

  it('CTE used in GROUP BY in main query', () => {
    const result = engine.execute({
      ctes: [{
        name: 'all_emp',
        query: { select: '*', from: 'employees' },
      }],
      mainQuery: {
        from: 'all_emp',
        groupBy: ['dept'],
        select: [{ agg: 'AVG', column: 'salary', alias: 'avg_salary' }],
      },
    });
    assert.equal(result.length, 3);
    const eng = result.find(r => r.dept === 'eng');
    assert.ok(Math.abs(eng.avg_salary - 111666.67) < 1);
  });

  it('empty CTE', () => {
    const result = engine.execute({
      ctes: [{
        name: 'ghosts',
        query: { select: '*', from: 'employees', where: { op: 'GT', left: 'salary', right: 999999 } },
      }],
      mainQuery: { select: '*', from: 'ghosts' },
    });
    assert.equal(result.length, 0);
  });

  it('no CTEs (passthrough)', () => {
    const result = engine.execute({
      ctes: [],
      mainQuery: { select: '*', from: 'employees', limit: 2 },
    });
    assert.equal(result.length, 2);
  });
});
