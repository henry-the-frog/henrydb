// multimap.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MultiMap } from './multimap.js';

describe('MultiMap', () => {
  it('multiple values per key', () => {
    const mm = new MultiMap();
    mm.add('color', 'red'); mm.add('color', 'blue');
    assert.deepEqual(mm.get('color'), ['red', 'blue']);
  });

  it('delete specific value', () => {
    const mm = new MultiMap();
    mm.add('x', 1); mm.add('x', 2);
    mm.delete('x', 1);
    assert.deepEqual(mm.get('x'), [2]);
  });

  it('entries iterator', () => {
    const mm = new MultiMap();
    mm.add('a', 1); mm.add('a', 2); mm.add('b', 3);
    assert.equal([...mm.entries()].length, 3);
  });
});
