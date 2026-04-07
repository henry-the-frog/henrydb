// volcano.test.js — Tests for volcano/iterator execution engine
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Iterator, SeqScan, ValuesIter, Filter, Project, Limit, Distinct,
  NestedLoopJoin, HashJoin, MergeJoin, Sort, HashAggregate, Union,
} from './volcano.js';
import { HeapFile } from './page.js';

// Helper: create a heap with rows
function makeHeap(name, rows) {
  const heap = new HeapFile(name);
  for (const values of rows) heap.insert(values);
  return heap;
}

describe('Volcano Iterator Engine', () => {
  describe('SeqScan', () => {
    it('scans all rows from a heap', () => {
      const heap = makeHeap('users', [
        [1, 'Alice'], [2, 'Bob'], [3, 'Charlie'],
      ]);
      const scan = new SeqScan(heap, ['id', 'name']);
      const rows = scan.toArray();
      assert.equal(rows.length, 3);
      assert.equal(rows[0].id, 1);
      assert.equal(rows[0].name, 'Alice');
      assert.equal(rows[2].name, 'Charlie');
    });

    it('attaches table alias as qualified names', () => {
      const heap = makeHeap('t', [[1, 'x']]);
      const scan = new SeqScan(heap, ['id', 'val'], 'u');
      const rows = scan.toArray();
      assert.equal(rows[0]['u.id'], 1);
      assert.equal(rows[0]['u.val'], 'x');
    });

    it('returns empty for empty heap', () => {
      const heap = makeHeap('empty', []);
      const scan = new SeqScan(heap, ['id']);
      assert.deepEqual(scan.toArray(), []);
    });
  });

  describe('ValuesIter', () => {
    it('iterates over literal values', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }, { x: 3 }]);
      const rows = iter.toArray();
      assert.equal(rows.length, 3);
      assert.equal(rows[1].x, 2);
    });
  });

  describe('Filter', () => {
    it('filters rows by predicate', () => {
      const iter = new ValuesIter([
        { id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' },
      ]);
      const filtered = new Filter(iter, row => row.id > 1);
      const rows = filtered.toArray();
      assert.equal(rows.length, 2);
      assert.equal(rows[0].name, 'Bob');
    });

    it('returns empty when no rows match', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }]);
      const filtered = new Filter(iter, row => row.x > 10);
      assert.deepEqual(filtered.toArray(), []);
    });
  });

  describe('Project', () => {
    it('projects specific columns', () => {
      const iter = new ValuesIter([
        { id: 1, name: 'Alice', age: 30 },
      ]);
      const proj = new Project(iter, [
        { name: 'name', expr: row => row.name },
        { name: 'age', expr: row => row.age },
      ]);
      const rows = proj.toArray();
      assert.deepEqual(rows, [{ name: 'Alice', age: 30 }]);
    });

    it('computes expressions', () => {
      const iter = new ValuesIter([{ x: 3, y: 4 }]);
      const proj = new Project(iter, [
        { name: 'sum', expr: row => row.x + row.y },
        { name: 'product', expr: row => row.x * row.y },
      ]);
      const rows = proj.toArray();
      assert.equal(rows[0].sum, 7);
      assert.equal(rows[0].product, 12);
    });
  });

  describe('Limit', () => {
    it('limits output rows', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }]);
      const limited = new Limit(iter, 3);
      const rows = limited.toArray();
      assert.equal(rows.length, 3);
      assert.equal(rows[2].x, 3);
    });

    it('handles offset', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }]);
      const limited = new Limit(iter, 2, 2); // skip 2, take 2
      const rows = limited.toArray();
      assert.equal(rows.length, 2);
      assert.equal(rows[0].x, 3);
      assert.equal(rows[1].x, 4);
    });

    it('returns fewer rows if input smaller than limit', () => {
      const iter = new ValuesIter([{ x: 1 }]);
      const limited = new Limit(iter, 100);
      assert.equal(limited.toArray().length, 1);
    });

    it('early termination: does not read beyond limit', () => {
      let reads = 0;
      const iter = {
        open() {},
        next() { reads++; return reads <= 100 ? { x: reads } : null; },
        close() {},
      };
      // Take only 3
      const limited = new Limit(iter, 3);
      const rows = limited.toArray();
      assert.equal(rows.length, 3);
      assert.equal(reads, 3); // Only read 3, not all 100
    });
  });

  describe('Distinct', () => {
    it('removes duplicate rows', () => {
      const iter = new ValuesIter([
        { x: 1, y: 'a' }, { x: 2, y: 'b' }, { x: 1, y: 'a' }, { x: 3, y: 'c' },
      ]);
      const distinct = new Distinct(iter);
      const rows = distinct.toArray();
      assert.equal(rows.length, 3);
    });

    it('deduplicates on specific keys', () => {
      const iter = new ValuesIter([
        { x: 1, y: 'a' }, { x: 1, y: 'b' }, { x: 2, y: 'a' },
      ]);
      const distinct = new Distinct(iter, ['x']);
      const rows = distinct.toArray();
      assert.equal(rows.length, 2);
    });
  });

  describe('NestedLoopJoin', () => {
    it('inner join with predicate', () => {
      const left = new ValuesIter([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
      const right = new ValuesIter([{ uid: 1, order: 'book' }, { uid: 1, order: 'pen' }, { uid: 3, order: 'hat' }]);
      const join = new NestedLoopJoin(left, right, (l, r) => l.id === r.uid);
      const rows = join.toArray();
      assert.equal(rows.length, 2); // Alice matches 2 orders
      assert.equal(rows[0].name, 'Alice');
      assert.equal(rows[0].order, 'book');
    });

    it('cross join (no predicate)', () => {
      const left = new ValuesIter([{ a: 1 }, { a: 2 }]);
      const right = new ValuesIter([{ b: 'x' }, { b: 'y' }]);
      const join = new NestedLoopJoin(left, right, null);
      assert.equal(join.toArray().length, 4); // 2 × 2
    });
  });

  describe('HashJoin', () => {
    it('equi-join on key', () => {
      const build = new ValuesIter([
        { dept_id: 1, dept: 'Engineering' },
        { dept_id: 2, dept: 'Sales' },
      ]);
      const probe = new ValuesIter([
        { name: 'Alice', dept_id: 1 },
        { name: 'Bob', dept_id: 2 },
        { name: 'Charlie', dept_id: 1 },
      ]);
      const join = new HashJoin(build, probe, 'dept_id', 'dept_id');
      const rows = join.toArray();
      assert.equal(rows.length, 3);
      const alice = rows.find(r => r.name === 'Alice');
      assert.equal(alice.dept, 'Engineering');
    });

    it('handles no matches', () => {
      const build = new ValuesIter([{ k: 99, val: 'z' }]);
      const probe = new ValuesIter([{ k: 1 }, { k: 2 }]);
      const join = new HashJoin(build, probe, 'k', 'k');
      assert.equal(join.toArray().length, 0);
    });
  });

  describe('MergeJoin', () => {
    it('joins two sorted inputs', () => {
      const left = new ValuesIter([
        { id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' },
      ]);
      const right = new ValuesIter([
        { id: 1, score: 90 }, { id: 2, score: 85 }, { id: 4, score: 70 },
      ]);
      const join = new MergeJoin(left, right, 'id', 'id');
      const rows = join.toArray();
      assert.equal(rows.length, 2); // id=1 and id=2 match
      assert.equal(rows[0].name, 'Alice');
      assert.equal(rows[0].score, 90);
    });
  });

  describe('Sort', () => {
    it('sorts by single column ascending', () => {
      const iter = new ValuesIter([{ x: 3 }, { x: 1 }, { x: 2 }]);
      const sorted = new Sort(iter, [{ column: 'x' }]);
      const rows = sorted.toArray();
      assert.deepEqual(rows.map(r => r.x), [1, 2, 3]);
    });

    it('sorts descending', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 3 }, { x: 2 }]);
      const sorted = new Sort(iter, [{ column: 'x', desc: true }]);
      assert.deepEqual(sorted.toArray().map(r => r.x), [3, 2, 1]);
    });

    it('sorts by multiple columns', () => {
      const iter = new ValuesIter([
        { dept: 'B', name: 'Bob' },
        { dept: 'A', name: 'Charlie' },
        { dept: 'A', name: 'Alice' },
      ]);
      const sorted = new Sort(iter, [{ column: 'dept' }, { column: 'name' }]);
      const rows = sorted.toArray();
      assert.equal(rows[0].name, 'Alice');
      assert.equal(rows[1].name, 'Charlie');
      assert.equal(rows[2].name, 'Bob');
    });
  });

  describe('HashAggregate', () => {
    it('computes COUNT, SUM, AVG', () => {
      const iter = new ValuesIter([
        { dept: 'A', salary: 100 },
        { dept: 'B', salary: 200 },
        { dept: 'A', salary: 150 },
        { dept: 'B', salary: 250 },
      ]);
      const agg = new HashAggregate(iter, ['dept'], [
        { name: 'cnt', func: 'COUNT', column: '*' },
        { name: 'total', func: 'SUM', column: 'salary' },
        { name: 'avg_sal', func: 'AVG', column: 'salary' },
      ]);
      const rows = agg.toArray();
      assert.equal(rows.length, 2);
      const deptA = rows.find(r => r.dept === 'A');
      assert.equal(deptA.cnt, 2);
      assert.equal(deptA.total, 250);
      assert.equal(deptA.avg_sal, 125);
    });

    it('computes MIN and MAX', () => {
      const iter = new ValuesIter([
        { g: 1, v: 10 }, { g: 1, v: 30 }, { g: 1, v: 20 },
      ]);
      const agg = new HashAggregate(iter, ['g'], [
        { name: 'min_v', func: 'MIN', column: 'v' },
        { name: 'max_v', func: 'MAX', column: 'v' },
      ]);
      const rows = agg.toArray();
      assert.equal(rows[0].min_v, 10);
      assert.equal(rows[0].max_v, 30);
    });

    it('handles no group-by (whole-table aggregate)', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }, { x: 3 }]);
      const agg = new HashAggregate(iter, [], [
        { name: 'total', func: 'SUM', column: 'x' },
        { name: 'cnt', func: 'COUNT', column: '*' },
      ]);
      const rows = agg.toArray();
      assert.equal(rows.length, 1);
      assert.equal(rows[0].total, 6);
      assert.equal(rows[0].cnt, 3);
    });
  });

  describe('Union', () => {
    it('concatenates two iterators', () => {
      const a = new ValuesIter([{ x: 1 }, { x: 2 }]);
      const b = new ValuesIter([{ x: 3 }, { x: 4 }]);
      const u = new Union(a, b);
      const rows = u.toArray();
      assert.equal(rows.length, 4);
      assert.deepEqual(rows.map(r => r.x), [1, 2, 3, 4]);
    });
  });

  describe('Composed pipelines', () => {
    it('SeqScan → Filter → Project → Limit', () => {
      const heap = makeHeap('t', [
        [1, 'Alice', 30], [2, 'Bob', 25], [3, 'Charlie', 35],
        [4, 'Diana', 28], [5, 'Eve', 22],
      ]);
      const pipeline = new Limit(
        new Project(
          new Filter(
            new SeqScan(heap, ['id', 'name', 'age']),
            row => row.age >= 25,
          ),
          [
            { name: 'name', expr: r => r.name },
            { name: 'age', expr: r => r.age },
          ],
        ),
        2, // Take 2
      );
      const rows = pipeline.toArray();
      assert.equal(rows.length, 2);
      assert.ok(rows.every(r => r.age >= 25));
      assert.ok(!('id' in rows[0])); // Project removed id
    });

    it('HashJoin → Sort → Project (department report)', () => {
      const empHeap = makeHeap('emp', [
        [1, 'Alice', 1], [2, 'Bob', 2], [3, 'Charlie', 1],
      ]);
      const deptHeap = makeHeap('dept', [
        [1, 'Engineering'], [2, 'Sales'],
      ]);

      const pipeline = new Project(
        new Sort(
          new HashJoin(
            new SeqScan(deptHeap, ['dept_id', 'dept_name'], 'd'),
            new SeqScan(empHeap, ['emp_id', 'emp_name', 'dept_id'], 'e'),
            'dept_id', 'dept_id',
          ),
          [{ column: 'emp_name' }],
        ),
        [
          { name: 'employee', expr: r => r.emp_name },
          { name: 'department', expr: r => r.dept_name },
        ],
      );

      const rows = pipeline.toArray();
      assert.equal(rows.length, 3);
      assert.equal(rows[0].employee, 'Alice');
      assert.equal(rows[0].department, 'Engineering');
    });

    it('SeqScan → HashAggregate → Sort → Filter (HAVING equivalent)', () => {
      const heap = makeHeap('orders', [
        ['Alice', 100], ['Bob', 200], ['Alice', 150],
        ['Charlie', 50], ['Bob', 300], ['Bob', 100],
      ]);

      const pipeline = new Filter(
        new Sort(
          new HashAggregate(
            new SeqScan(heap, ['customer', 'amount']),
            ['customer'],
            [
              { name: 'total', func: 'SUM', column: 'amount' },
              { name: 'orders', func: 'COUNT', column: '*' },
            ],
          ),
          [{ column: 'total', desc: true }],
        ),
        row => row.total > 200, // HAVING total > 200
      );

      const rows = pipeline.toArray();
      assert.equal(rows.length, 2); // Bob (600) and Alice (250)
      assert.equal(rows[0].customer, 'Bob');
      assert.equal(rows[0].total, 600);
      assert.equal(rows[1].customer, 'Alice');
    });

    it('for-of iteration works', () => {
      const iter = new ValuesIter([{ x: 1 }, { x: 2 }, { x: 3 }]);
      const collected = [];
      for (const row of iter) collected.push(row.x);
      assert.deepEqual(collected, [1, 2, 3]);
    });
  });
});
