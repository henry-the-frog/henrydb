// window-functions.test.js — Tests for SQL window functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ValuesIter, Sort, Window } from './volcano.js';

describe('Window Functions', () => {
  const employees = [
    { name: 'Alice', dept: 'Engineering', salary: 90000 },
    { name: 'Bob', dept: 'Engineering', salary: 85000 },
    { name: 'Charlie', dept: 'Sales', salary: 70000 },
    { name: 'Diana', dept: 'Sales', salary: 75000 },
    { name: 'Eve', dept: 'Engineering', salary: 95000 },
    { name: 'Frank', dept: 'Sales', salary: 65000 },
  ];

  describe('ROW_NUMBER', () => {
    it('assigns sequential numbers within partition', () => {
      const sorted = new Sort(new ValuesIter(employees), [{ column: 'dept' }, { column: 'salary', desc: true }]);
      const win = new Window(sorted, ['dept'], [{ column: 'salary', desc: true }], [
        { name: 'rn', func: 'ROW_NUMBER' },
      ]);
      const rows = win.toArray();
      
      const eng = rows.filter(r => r.dept === 'Engineering');
      assert.equal(eng[0].rn, 1); // Eve (95k)
      assert.equal(eng[1].rn, 2); // Alice (90k)
      assert.equal(eng[2].rn, 3); // Bob (85k)
      
      const sales = rows.filter(r => r.dept === 'Sales');
      assert.equal(sales[0].rn, 1); // Diana (75k)
      assert.equal(sales[1].rn, 2); // Charlie (70k)
      assert.equal(sales[2].rn, 3); // Frank (65k)
    });

    it('without partition assigns global numbers', () => {
      const sorted = new Sort(new ValuesIter(employees), [{ column: 'salary', desc: true }]);
      const win = new Window(sorted, [], [{ column: 'salary', desc: true }], [
        { name: 'rn', func: 'ROW_NUMBER' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].rn, 1);
      assert.equal(rows[5].rn, 6);
    });
  });

  describe('RANK', () => {
    it('handles ties (same value = same rank, gaps after)', () => {
      const data = [
        { name: 'A', score: 100 },
        { name: 'B', score: 90 },
        { name: 'C', score: 90 },
        { name: 'D', score: 80 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'score', desc: true }]);
      const win = new Window(sorted, [], [{ column: 'score', desc: true }], [
        { name: 'rank', func: 'RANK' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].rank, 1); // A: 100
      assert.equal(rows[1].rank, 2); // B: 90
      assert.equal(rows[2].rank, 2); // C: 90 (tie)
      assert.equal(rows[3].rank, 4); // D: 80 (skip 3)
    });
  });

  describe('DENSE_RANK', () => {
    it('handles ties without gaps', () => {
      const data = [
        { name: 'A', score: 100 },
        { name: 'B', score: 90 },
        { name: 'C', score: 90 },
        { name: 'D', score: 80 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'score', desc: true }]);
      const win = new Window(sorted, [], [{ column: 'score', desc: true }], [
        { name: 'dense_rank', func: 'DENSE_RANK' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].dense_rank, 1); // 100
      assert.equal(rows[1].dense_rank, 2); // 90
      assert.equal(rows[2].dense_rank, 2); // 90 (tie)
      assert.equal(rows[3].dense_rank, 3); // 80 (no gap!)
    });
  });

  describe('LAG', () => {
    it('returns previous row value', () => {
      const data = [
        { month: 1, revenue: 100 },
        { month: 2, revenue: 150 },
        { month: 3, revenue: 120 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'month' }]);
      const win = new Window(sorted, [], [{ column: 'month' }], [
        { name: 'prev_rev', func: 'LAG', arg: 'revenue' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].prev_rev, null); // No previous
      assert.equal(rows[1].prev_rev, 100);
      assert.equal(rows[2].prev_rev, 150);
    });

    it('supports custom offset', () => {
      const data = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }];
      const win = new Window(new ValuesIter(data), [], [{ column: 'x' }], [
        { name: 'lag2', func: 'LAG', arg: 'x', offset: 2 },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].lag2, null);
      assert.equal(rows[1].lag2, null);
      assert.equal(rows[2].lag2, 1);
      assert.equal(rows[3].lag2, 2);
    });
  });

  describe('LEAD', () => {
    it('returns next row value', () => {
      const data = [
        { month: 1, revenue: 100 },
        { month: 2, revenue: 150 },
        { month: 3, revenue: 120 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'month' }]);
      const win = new Window(sorted, [], [{ column: 'month' }], [
        { name: 'next_rev', func: 'LEAD', arg: 'revenue' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].next_rev, 150);
      assert.equal(rows[1].next_rev, 120);
      assert.equal(rows[2].next_rev, null); // No next
    });
  });

  describe('Running aggregates', () => {
    it('SUM computes running total', () => {
      const data = [
        { month: 1, amount: 100 },
        { month: 2, amount: 200 },
        { month: 3, amount: 150 },
      ];
      const win = new Window(new ValuesIter(data), [], [{ column: 'month' }], [
        { name: 'running_total', func: 'SUM', arg: 'amount' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].running_total, 100);
      assert.equal(rows[1].running_total, 300);
      assert.equal(rows[2].running_total, 450);
    });

    it('COUNT computes running count', () => {
      const data = [{ x: 'a' }, { x: 'b' }, { x: 'c' }];
      const win = new Window(new ValuesIter(data), [], [{ column: 'x' }], [
        { name: 'cnt', func: 'COUNT' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].cnt, 1);
      assert.equal(rows[1].cnt, 2);
      assert.equal(rows[2].cnt, 3);
    });

    it('AVG computes running average', () => {
      const data = [
        { x: 10 }, { x: 20 }, { x: 30 },
      ];
      const win = new Window(new ValuesIter(data), [], [{ column: 'x' }], [
        { name: 'avg', func: 'AVG', arg: 'x' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].avg, 10);
      assert.equal(rows[1].avg, 15);
      assert.equal(rows[2].avg, 20);
    });

    it('MIN/MAX compute running min/max', () => {
      const data = [{ x: 5 }, { x: 2 }, { x: 8 }, { x: 1 }];
      const win = new Window(new ValuesIter(data), [], [{ column: 'x' }], [
        { name: 'min', func: 'MIN', arg: 'x' },
        { name: 'max', func: 'MAX', arg: 'x' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].min, 5); assert.equal(rows[0].max, 5);
      assert.equal(rows[1].min, 2); assert.equal(rows[1].max, 5);
      assert.equal(rows[2].min, 2); assert.equal(rows[2].max, 8);
      assert.equal(rows[3].min, 1); assert.equal(rows[3].max, 8);
    });
  });

  describe('Partitioned window', () => {
    it('ROW_NUMBER + running SUM per department', () => {
      const sorted = new Sort(new ValuesIter(employees), [{ column: 'dept' }, { column: 'salary', desc: true }]);
      const win = new Window(sorted, ['dept'], [{ column: 'salary', desc: true }], [
        { name: 'rn', func: 'ROW_NUMBER' },
        { name: 'running_total', func: 'SUM', arg: 'salary' },
      ]);
      const rows = win.toArray();
      
      const eng = rows.filter(r => r.dept === 'Engineering');
      assert.equal(eng[0].rn, 1);
      assert.equal(eng[0].running_total, 95000); // Eve
      assert.equal(eng[1].running_total, 185000); // + Alice
      assert.equal(eng[2].running_total, 270000); // + Bob
      
      const sales = rows.filter(r => r.dept === 'Sales');
      assert.equal(sales[0].rn, 1);
      assert.equal(sales[0].running_total, 75000); // Diana
    });

    it('RANK within partition', () => {
      const data = [
        { dept: 'A', score: 90 },
        { dept: 'A', score: 90 },
        { dept: 'A', score: 80 },
        { dept: 'B', score: 95 },
        { dept: 'B', score: 85 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'dept' }, { column: 'score', desc: true }]);
      const win = new Window(sorted, ['dept'], [{ column: 'score', desc: true }], [
        { name: 'rank', func: 'RANK' },
      ]);
      const rows = win.toArray();
      
      const deptA = rows.filter(r => r.dept === 'A');
      assert.equal(deptA[0].rank, 1);
      assert.equal(deptA[1].rank, 1); // tie
      assert.equal(deptA[2].rank, 3); // skip
      
      const deptB = rows.filter(r => r.dept === 'B');
      assert.equal(deptB[0].rank, 1);
      assert.equal(deptB[1].rank, 2);
    });
  });

  describe('Multiple window functions', () => {
    it('ROW_NUMBER + RANK + LAG together', () => {
      const data = [
        { name: 'Alice', score: 95 },
        { name: 'Bob', score: 90 },
        { name: 'Charlie', score: 90 },
        { name: 'Diana', score: 85 },
      ];
      const sorted = new Sort(new ValuesIter(data), [{ column: 'score', desc: true }]);
      const win = new Window(sorted, [], [{ column: 'score', desc: true }], [
        { name: 'rn', func: 'ROW_NUMBER' },
        { name: 'rank', func: 'RANK' },
        { name: 'prev_score', func: 'LAG', arg: 'score' },
      ]);
      const rows = win.toArray();
      assert.equal(rows[0].rn, 1);
      assert.equal(rows[0].rank, 1);
      assert.equal(rows[0].prev_score, null);
      
      assert.equal(rows[1].rn, 2);
      assert.equal(rows[1].rank, 2);
      assert.equal(rows[1].prev_score, 95);
      
      assert.equal(rows[2].rn, 3);
      assert.equal(rows[2].rank, 2); // tie
      assert.equal(rows[2].prev_score, 90);
    });
  });

  describe('EXPLAIN', () => {
    it('shows Window in explain output', () => {
      const win = new Window(
        new ValuesIter([{ x: 1 }]),
        ['dept'],
        [{ column: 'salary', desc: true }],
        [{ name: 'rn', func: 'ROW_NUMBER' }],
      );
      const plan = win.explain();
      assert.ok(plan.includes('Window'));
      assert.ok(plan.includes('dept'));
      assert.ok(plan.includes('ROW_NUMBER'));
    });
  });
});
