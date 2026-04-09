// join-algorithms.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { nestedLoopJoin, hashJoin, sortMergeJoin } from './join-algorithms.js';

const users = [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }];
const orders = [{ user_id: 1, item: 'Book' }, { user_id: 1, item: 'Pen' }, { user_id: 3, item: 'Hat' }];

describe('Join Algorithms', () => {
  it('nested loop join', () => {
    const r = nestedLoopJoin(users, orders, (l, r) => l.id === r.user_id);
    assert.equal(r.length, 2);
  });

  it('hash join', () => {
    const r = hashJoin(users, orders, 'id', 'user_id');
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
  });

  it('sort-merge join', () => {
    const r = sortMergeJoin(users, orders, 'id', 'user_id');
    assert.equal(r.length, 2);
  });

  it('performance: 1K × 1K hash join', () => {
    const left = Array.from({ length: 1000 }, (_, i) => ({ id: i }));
    const right = Array.from({ length: 1000 }, (_, i) => ({ fk: i }));
    const t0 = performance.now();
    const r = hashJoin(left, right, 'id', 'fk');
    const elapsed = performance.now() - t0;
    assert.equal(r.length, 1000);
    console.log(`  1K×1K hash join: ${elapsed.toFixed(1)}ms`);
  });
});
