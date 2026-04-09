// access-method.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { AccessMethod, AccessMethodRegistry } from './access-method.js';

describe('AccessMethod', () => {
  it('insert and lookup', () => {
    const am = new AccessMethod('pk_users', 'btree');
    am.insert(1, 100); am.insert(2, 200);
    assert.equal(am.lookup(1), 100);
  });

  it('range scan', () => {
    const am = new AccessMethod('idx', 'btree');
    for (let i = 0; i < 10; i++) am.insert(i, i * 10);
    const range = am.rangeScan(3, 7);
    assert.equal(range.length, 5);
  });

  it('registry', () => {
    const reg = new AccessMethodRegistry();
    reg.register('pk', new AccessMethod('pk', 'btree'));
    assert.deepEqual(reg.list(), ['pk']);
  });
});
