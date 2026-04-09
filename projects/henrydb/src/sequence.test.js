// sequence.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Sequence } from './sequence.js';

describe('Sequence', () => {
  it('auto-increment', () => {
    const seq = new Sequence();
    assert.equal(seq.nextVal(), 1);
    assert.equal(seq.nextVal(), 2);
    assert.equal(seq.nextVal(), 3);
  });

  it('prefetch batch', () => {
    const seq = new Sequence();
    const batch = seq.prefetch(5);
    assert.deepEqual(batch, [1, 2, 3, 4, 5]);
    assert.equal(seq.nextVal(), 6);
  });
});
