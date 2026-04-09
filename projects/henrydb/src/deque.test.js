// deque.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Deque } from './deque.js';

describe('Deque', () => {
  it('push and pop from both ends', () => {
    const d = new Deque();
    d.pushBack(1); d.pushBack(2); d.pushFront(0);
    assert.deepEqual(d.toArray(), [0, 1, 2]);
    assert.equal(d.popFront(), 0);
    assert.equal(d.popBack(), 2);
  });

  it('peek', () => {
    const d = new Deque();
    d.pushBack(1); d.pushBack(2);
    assert.equal(d.peekFront(), 1);
    assert.equal(d.peekBack(), 2);
  });

  it('at() random access', () => {
    const d = new Deque();
    for (let i = 0; i < 5; i++) d.pushBack(i);
    assert.equal(d.at(0), 0);
    assert.equal(d.at(4), 4);
  });

  it('grow on overflow', () => {
    const d = new Deque(2);
    d.pushBack(1); d.pushBack(2); d.pushBack(3);
    assert.equal(d.size, 3);
    assert.deepEqual(d.toArray(), [1, 2, 3]);
  });

  it('stress: 10K ops', () => {
    const d = new Deque();
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) d.pushBack(i);
    for (let i = 0; i < 5000; i++) d.popFront();
    for (let i = 0; i < 5000; i++) d.pushFront(i);
    const elapsed = performance.now() - t0;
    console.log(`  20K ops: ${elapsed.toFixed(1)}ms`);
    assert.equal(d.size, 10000);
  });
});
