// range-types.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { Range, RangeTypeManager } from './range-types.js';

describe('Range', () => {
  test('contains value', () => {
    const r = new Range(1, 10);
    assert.ok(r.contains(1));  // lower inclusive
    assert.ok(r.contains(5));
    assert.ok(!r.contains(10)); // upper exclusive
    assert.ok(!r.contains(0));
  });

  test('contains value with both inclusive', () => {
    const r = new Range(1, 10, { lowerInc: true, upperInc: true });
    assert.ok(r.contains(10));
  });

  test('empty range contains nothing', () => {
    const r = Range.empty();
    assert.ok(!r.contains(5));
    assert.ok(r.isEmpty());
  });

  test('containsRange', () => {
    const big = new Range(1, 100);
    const small = new Range(10, 20);
    assert.ok(big.containsRange(small));
    assert.ok(!small.containsRange(big));
  });

  test('overlaps', () => {
    const a = new Range(1, 10);
    const b = new Range(5, 15);
    const c = new Range(10, 20);
    assert.ok(a.overlaps(b));
    assert.ok(!a.overlaps(c)); // [1,10) and [10,20) don't overlap
  });

  test('overlaps with inclusive boundary', () => {
    const a = new Range(1, 10, { upperInc: true });
    const b = new Range(10, 20);
    assert.ok(a.overlaps(b));
  });

  test('intersection', () => {
    const a = new Range(1, 10);
    const b = new Range(5, 15);
    const c = a.intersection(b);
    assert.equal(c.lower, 5);
    assert.equal(c.upper, 10);
  });

  test('intersection of non-overlapping is empty', () => {
    const a = new Range(1, 5);
    const b = new Range(10, 15);
    assert.ok(a.intersection(b).isEmpty());
  });

  test('union of overlapping ranges', () => {
    const a = new Range(1, 10);
    const b = new Range(5, 15);
    const u = a.union(b);
    assert.equal(u.lower, 1);
    assert.equal(u.upper, 15);
  });

  test('isAdjacentTo', () => {
    const a = new Range(1, 5);        // [1,5)
    const b = new Range(5, 10);       // [5,10)
    assert.ok(a.isAdjacentTo(b));     // 5 exclusive + 5 inclusive = adjacent
  });

  test('unbounded range', () => {
    const r = new Range(null, 10);
    assert.ok(r.contains(-1000));
    assert.ok(r.contains(9));
    assert.ok(!r.contains(10));
  });

  test('toString', () => {
    assert.equal(new Range(1, 10).toString(), '[1,10)');
    assert.equal(new Range(1, 10, { lowerInc: false, upperInc: true }).toString(), '(1,10]');
    assert.equal(Range.empty().toString(), 'empty');
  });

  test('fromString', () => {
    const r = Range.fromString('[1,10)');
    assert.equal(r.lower, 1);
    assert.equal(r.upper, 10);
    assert.ok(r.lowerInc);
    assert.ok(!r.upperInc);
  });

  test('point range (single value)', () => {
    const r = new Range(5, 5, { lowerInc: true, upperInc: true });
    assert.ok(r.contains(5));
    assert.ok(!r.isEmpty());
  });

  test('degenerate range is empty', () => {
    const r = new Range(5, 5, { lowerInc: true, upperInc: false });
    assert.ok(r.isEmpty());
  });
});

describe('RangeTypeManager', () => {
  const rtm = new RangeTypeManager();

  test('has built-in types', () => {
    assert.ok(rtm.has('int4range'));
    assert.ok(rtm.has('numrange'));
    assert.ok(rtm.has('tsrange'));
    assert.ok(rtm.has('daterange'));
  });

  test('parse int4range', () => {
    const r = rtm.parse('int4range', '[1,100)');
    assert.ok(r.contains(50));
    assert.ok(!r.contains(100));
  });

  test('create range', () => {
    const r = rtm.create('int4range', 1, 10);
    assert.ok(r.contains(5));
  });

  test('list types', () => {
    assert.ok(rtm.list().length >= 4);
  });
});
