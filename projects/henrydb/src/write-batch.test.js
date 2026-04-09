// write-batch.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WriteBatch } from './write-batch.js';

describe('WriteBatch', () => {
  it('batch put and apply', () => {
    const store = new Map();
    const batch = new WriteBatch();
    batch.put('a', 1).put('b', 2).put('c', 3);
    assert.equal(batch.size, 3);
    batch.apply(store);
    assert.equal(store.get('a'), 1);
    assert.equal(batch.size, 0);
  });

  it('batch delete', () => {
    const store = new Map([['a', 1], ['b', 2]]);
    new WriteBatch().delete('a').apply(store);
    assert.equal(store.has('a'), false);
  });
});
