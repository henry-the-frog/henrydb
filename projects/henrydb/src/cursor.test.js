// cursor.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Cursor } from './cursor.js';

describe('Cursor', () => {
  it('iterate with next', () => {
    const c = new Cursor([1, 2, 3]);
    assert.equal(c.next(), 1);
    assert.equal(c.next(), 2);
    assert.equal(c.next(), 3);
    assert.equal(c.next(), null);
  });

  it('fetch batch', () => {
    const c = new Cursor([1, 2, 3, 4, 5]);
    assert.deepEqual(c.fetch(3), [1, 2, 3]);
    assert.deepEqual(c.fetch(3), [4, 5]);
  });

  it('iterator protocol', () => {
    const c = new Cursor(['a', 'b', 'c']);
    assert.deepEqual([...c], ['a', 'b', 'c']);
  });
});
