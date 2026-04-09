// sort-external.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExternalSort } from './sort-external.js';

describe('ExternalSort', () => {
  it('sorts correctly', () => {
    const data = [5, 3, 8, 1, 9, 2, 7, 4, 6];
    assert.deepEqual(ExternalSort.sort(data, 3), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('10K elements', () => {
    const data = Array.from({ length: 10000 }, () => Math.floor(Math.random() * 100000));
    const sorted = ExternalSort.sort(data, 100);
    assert.equal(sorted.length, 10000);
    for (let i = 1; i < sorted.length; i++) assert.ok(sorted[i] >= sorted[i-1]);
  });
});
