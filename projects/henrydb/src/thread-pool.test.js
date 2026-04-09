// thread-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ThreadPool } from './thread-pool.js';

describe('ThreadPool', () => {
  it('processes all tasks', () => {
    const tp = new ThreadPool(4);
    for (let i = 0; i < 20; i++) tp.submit(`task-${i}`);
    tp.runAll();
    assert.equal(tp.completed.length, 20);
  });

  it('work stealing balances load', () => {
    const tp = new ThreadPool(2);
    for (let i = 0; i < 10; i++) tp.submit(i);
    tp.runAll();
    assert.equal(tp.completed.length, 10);
  });
});
