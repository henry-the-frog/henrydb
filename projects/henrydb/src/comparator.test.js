// comparator.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Comparators } from './comparator.js';

describe('Comparators', () => {
  it('numeric sort', () => {
    assert.deepEqual([3, 1, 2].sort(Comparators.numeric), [1, 2, 3]);
  });

  it('nullsFirst', () => {
    const cmp = Comparators.nullsFirst(Comparators.numeric);
    assert.deepEqual([3, null, 1].sort(cmp), [null, 1, 3]);
  });

  it('multiColumn', () => {
    const rows = [{ a: 1, b: 'z' }, { a: 1, b: 'a' }, { a: 2, b: 'b' }];
    const cmp = Comparators.multiColumn([
      { col: 'a', order: 'ASC', cmp: Comparators.numeric },
      { col: 'b', order: 'ASC', cmp: Comparators.string },
    ]);
    rows.sort(cmp);
    assert.equal(rows[0].b, 'a');
    assert.equal(rows[2].a, 2);
  });
});
