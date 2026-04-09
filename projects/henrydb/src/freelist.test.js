// freelist.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FreeList } from './freelist.js';

describe('FreeList', () => {
  it('allocate and deallocate', () => {
    const fl = new FreeList(10);
    const p1 = fl.allocate();
    assert.ok(p1 >= 0);
    assert.equal(fl.allocatedCount, 1);
    fl.deallocate(p1);
    assert.equal(fl.allocatedCount, 0);
  });

  it('returns -1 when exhausted', () => {
    const fl = new FreeList(1);
    fl.allocate();
    assert.equal(fl.allocate(), -1);
  });
});
