// two-choice-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TwoChoiceHash } from './two-choice-hash.js';

describe('TwoChoiceHash', () => {
  it('insert and get', () => {
    const h = new TwoChoiceHash(16);
    h.insert('a', 1); h.insert('b', 2);
    assert.equal(h.get('a'), 1);
    assert.equal(h.get('b'), 2);
  });

  it('balanced load: max << n/buckets', () => {
    const h = new TwoChoiceHash(100);
    for (let i = 0; i < 10000; i++) h.insert(i, i);
    const max = h.maxLoad();
    const avg = h.avgLoad();
    console.log(`  max: ${max}, avg: ${avg.toFixed(1)}, ratio: ${(max/avg).toFixed(2)}`);
    assert.ok(max < avg * 3, `Max load ${max} too high vs avg ${avg.toFixed(1)}`);
  });
});
