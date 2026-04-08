// optimizer.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PredicatePushdown, ProjectionPushdown, SortGroupBy } from './optimizer.js';

const col = n => ({ type: 'column', name: n });
const lit = v => ({ type: 'literal', value: v });

describe('PredicatePushdown', () => {
  it('pushes single-table predicate to left', () => {
    const pp = new PredicatePushdown();
    const plan = {
      type: 'join',
      left: { table: 'users', columns: ['id', 'name', 'age'] },
      right: { table: 'orders', columns: ['order_id', 'user_id', 'amount'] },
      predicate: { type: 'COMPARE', op: 'GT', left: col('age'), right: lit(25) },
    };
    const optimized = pp.optimize(plan);
    assert.ok(optimized.left.filter); // Pushed to left
    assert.equal(optimized.predicate, null); // Removed from join
  });

  it('keeps join predicate', () => {
    const pp = new PredicatePushdown();
    const plan = {
      type: 'join',
      left: { table: 'a', columns: ['x'] },
      right: { table: 'b', columns: ['y'] },
      predicate: { type: 'COMPARE', op: 'EQ', left: col('x'), right: col('y') },
    };
    const optimized = pp.optimize(plan);
    assert.ok(optimized.predicate); // Not pushed (cross-table)
  });

  it('splits compound predicate', () => {
    const pp = new PredicatePushdown();
    const plan = {
      type: 'join',
      left: { table: 'a', columns: ['x'] },
      right: { table: 'b', columns: ['y'] },
      predicate: {
        type: 'AND',
        left: { type: 'COMPARE', op: 'GT', left: col('x'), right: lit(10) }, // → left
        right: { type: 'COMPARE', op: 'EQ', left: col('x'), right: col('y') }, // → join
      },
    };
    const optimized = pp.optimize(plan);
    assert.ok(optimized.left.filter); // x > 10 pushed
    assert.ok(optimized.predicate); // x = y stays
  });
});

describe('ProjectionPushdown', () => {
  it('adds projected columns to plan', () => {
    const pp = new ProjectionPushdown();
    const plan = { type: 'scan', table: 'users' };
    const optimized = pp.optimize(plan, new Set(['name', 'age']));
    assert.ok(optimized.projectedColumns.includes('name'));
    assert.ok(optimized.projectedColumns.includes('age'));
  });

  it('includes filter columns', () => {
    const pp = new ProjectionPushdown();
    const plan = { type: 'scan', filter: { type: 'COMPARE', op: 'GT', left: col('salary'), right: lit(100000) } };
    const optimized = pp.optimize(plan, new Set(['name']));
    assert.ok(optimized.projectedColumns.includes('salary'));
    assert.ok(optimized.projectedColumns.includes('name'));
  });
});

describe('SortGroupBy', () => {
  it('groups pre-sorted data', () => {
    const gb = new SortGroupBy(['dept'], [{ col: 'salary', func: 'SUM', alias: 'total' }]);
    const data = [
      { dept: 'eng', salary: 100 },
      { dept: 'eng', salary: 200 },
      { dept: 'hr', salary: 50 },
      { dept: 'hr', salary: 60 },
    ];
    const results = gb.process(data);
    assert.equal(results.length, 2);
    assert.equal(results[0].total, 300);
    assert.equal(results[1].total, 110);
  });

  it('multiple aggregates', () => {
    const gb = new SortGroupBy(['dept'], [
      { col: 'salary', func: 'SUM', alias: 'total' },
      { col: 'salary', func: 'COUNT', alias: 'cnt' },
      { col: 'salary', func: 'AVG', alias: 'avg' },
      { col: 'salary', func: 'MIN', alias: 'min' },
      { col: 'salary', func: 'MAX', alias: 'max' },
    ]);
    const data = [
      { dept: 'eng', salary: 100 },
      { dept: 'eng', salary: 200 },
      { dept: 'eng', salary: 300 },
    ];
    const results = gb.process(data);
    assert.equal(results[0].total, 600);
    assert.equal(results[0].cnt, 3);
    assert.equal(results[0].avg, 200);
    assert.equal(results[0].min, 100);
    assert.equal(results[0].max, 300);
  });

  it('single group', () => {
    const gb = new SortGroupBy(['dept'], [{ col: 'val', func: 'SUM', alias: 'total' }]);
    const data = [{ dept: 'a', val: 1 }, { dept: 'a', val: 2 }];
    assert.equal(gb.process(data).length, 1);
  });

  it('empty input', () => {
    const gb = new SortGroupBy(['dept'], [{ col: 'val', func: 'SUM', alias: 'total' }]);
    assert.deepEqual(gb.process([]), []);
  });

  it('benchmark: 100K pre-sorted rows', () => {
    const gb = new SortGroupBy(['dept'], [{ col: 'val', func: 'SUM', alias: 'total' }]);
    const data = Array.from({ length: 100000 }, (_, i) => ({
      dept: `dept_${Math.floor(i / 10000)}`,
      val: Math.random() * 1000,
    }));
    const t0 = Date.now();
    const results = gb.process(data);
    console.log(`    Sort group by 100K: ${Date.now() - t0}ms, ${results.length} groups`);
    assert.equal(results.length, 10);
  });
});
