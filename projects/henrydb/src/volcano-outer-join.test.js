import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SeqScan, HashJoin, NestedLoopJoin } from './volcano.js';
import { HeapFile } from './page.js';

// Helper: create a SeqScan from columnar data
function makeScan(name, columns, rows, alias) {
  const heap = new HeapFile(name);
  for (const row of rows) {
    heap.insert(columns.map(c => row[c]));
  }
  return new SeqScan(heap, columns, alias);
}

const leftData = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
];
const rightData = [
  { id: 1, dept: 'Engineering' },
  { id: 3, dept: 'Marketing' },
];

function leftScan(alias) { return makeScan('left', ['id', 'name'], leftData, alias || 'l'); }
function rightScan(alias) { return makeScan('right', ['id', 'dept'], rightData, alias || 'r'); }

function collect(iter) {
  iter.open();
  const results = [];
  let row;
  while ((row = iter.next()) !== null) results.push(row);
  iter.close();
  return results;
}

describe('RIGHT JOIN — HashJoin', () => {
  it('includes matching rows', () => {
    // HashJoin(build=right, probe=left, buildKey, probeKey, joinType)
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'right'));
    const match = results.find(r => r['l.name'] === 'Alice');
    assert.ok(match);
    assert.equal(match['r.dept'], 'Engineering');
  });

  it('includes unmatched build (right) rows with null probe columns', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'right'));
    const unmatched = results.find(r => r['r.dept'] === 'Marketing');
    assert.ok(unmatched, 'unmatched right row should be emitted');
    assert.equal(unmatched['l.name'], null);
  });

  it('excludes unmatched probe (left) rows', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'right'));
    const bob = results.find(r => r['l.name'] === 'Bob');
    assert.equal(bob, undefined, 'unmatched left row should not appear in RIGHT JOIN');
  });

  it('returns correct count', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'right'));
    assert.equal(results.length, 2); // Alice+Eng, null+Marketing
  });
});

describe('FULL JOIN — HashJoin', () => {
  it('includes matching rows', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'full'));
    const match = results.find(r => r['l.name'] === 'Alice' && r['r.dept'] === 'Engineering');
    assert.ok(match);
  });

  it('includes unmatched left rows with null right columns', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'full'));
    const bob = results.find(r => r['l.name'] === 'Bob');
    assert.ok(bob, 'unmatched left row should appear');
    assert.equal(bob['r.dept'], null);
  });

  it('includes unmatched right rows with null left columns', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'full'));
    const marketing = results.find(r => r['r.dept'] === 'Marketing');
    assert.ok(marketing, 'unmatched right row should appear');
    assert.equal(marketing['l.name'], null);
  });

  it('returns all rows (matched + unmatched from both sides)', () => {
    const results = collect(new HashJoin(rightScan(), leftScan(), 'r.id', 'l.id', 'full'));
    assert.equal(results.length, 3); // Alice+Eng, Bob+null, null+Marketing
  });
});

describe('RIGHT JOIN — NestedLoopJoin', () => {
  it('includes matching rows and unmatched inner (right) rows', () => {
    const nlj = new NestedLoopJoin(
      leftScan(), rightScan(),
      (l, r) => l['l.id'] === r['r.id'],
      'right'
    );
    const results = collect(nlj);
    assert.equal(results.length, 2);
    assert.ok(results.find(r => r['l.name'] === 'Alice'));
    const unmatched = results.find(r => r['r.dept'] === 'Marketing');
    assert.ok(unmatched);
    assert.equal(unmatched['l.name'], null);
  });

  it('excludes unmatched outer (left) rows', () => {
    const nlj = new NestedLoopJoin(
      leftScan(), rightScan(),
      (l, r) => l['l.id'] === r['r.id'],
      'right'
    );
    const results = collect(nlj);
    assert.equal(results.find(r => r['l.name'] === 'Bob'), undefined);
  });
});

describe('FULL JOIN — NestedLoopJoin', () => {
  it('includes all rows from both sides', () => {
    const nlj = new NestedLoopJoin(
      leftScan(), rightScan(),
      (l, r) => l['l.id'] === r['r.id'],
      'full'
    );
    const results = collect(nlj);
    assert.equal(results.length, 3);
    assert.ok(results.find(r => r['l.name'] === 'Alice' && r['r.dept'] === 'Engineering'));
    const bob = results.find(r => r['l.name'] === 'Bob');
    assert.ok(bob);
    assert.equal(bob['r.dept'], null);
    const marketing = results.find(r => r['r.dept'] === 'Marketing');
    assert.ok(marketing);
    assert.equal(marketing['l.name'], null);
  });
});

describe('Edge cases', () => {
  it('RIGHT JOIN with empty left side emits all right rows', () => {
    const emptyLeft = makeScan('empty', ['id', 'name'], [], 'l');
    const results = collect(new HashJoin(rightScan(), emptyLeft, 'r.id', 'l.id', 'right'));
    assert.equal(results.length, 2);
  });

  it('FULL JOIN with empty left side emits all right rows', () => {
    const emptyLeft = makeScan('empty', ['id', 'name'], [], 'l');
    const results = collect(new HashJoin(rightScan(), emptyLeft, 'r.id', 'l.id', 'full'));
    assert.equal(results.length, 2);
  });

  it('FULL JOIN with empty right side emits all left rows', () => {
    const emptyRight = makeScan('empty', ['id', 'dept'], [], 'r');
    const results = collect(new HashJoin(emptyRight, leftScan(), 'r.id', 'l.id', 'full'));
    assert.equal(results.length, 2);
  });

  it('RIGHT JOIN with duplicates on build side', () => {
    const rightDup = makeScan('right', ['id', 'dept'], [
      { id: 1, dept: 'Eng' },
      { id: 1, dept: 'Sales' },
      { id: 2, dept: 'HR' },
    ], 'r');
    const leftOne = makeScan('left', ['id', 'name'], [{ id: 1, name: 'Alice' }], 'l');
    const results = collect(new HashJoin(rightDup, leftOne, 'r.id', 'l.id', 'right'));
    // Alice matches both id=1 (Eng, Sales), HR unmatched
    assert.equal(results.length, 3);
    const hr = results.find(r => r['r.dept'] === 'HR');
    assert.ok(hr);
    assert.equal(hr['l.name'], null);
  });

  it('FULL JOIN where all rows match', () => {
    const l = makeScan('l', ['id', 'name'], [{ id: 1, name: 'Alice' }], 'l');
    const r = makeScan('r', ['id', 'dept'], [{ id: 1, dept: 'Eng' }], 'r');
    const results = collect(new HashJoin(r, l, 'r.id', 'l.id', 'full'));
    assert.equal(results.length, 1);
    assert.equal(results[0]['l.name'], 'Alice');
    assert.equal(results[0]['r.dept'], 'Eng');
  });
});
