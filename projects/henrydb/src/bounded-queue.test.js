// bounded-queue.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BoundedQueue } from './bounded-queue.js';

describe('BoundedQueue', () => {
  it('FIFO', () => {
    const q = new BoundedQueue(3);
    q.enqueue('a'); q.enqueue('b');
    assert.equal(q.dequeue(), 'a');
  });

  it('backpressure', () => {
    const q = new BoundedQueue(2);
    assert.equal(q.enqueue(1), true);
    assert.equal(q.enqueue(2), true);
    assert.equal(q.enqueue(3), false); // Full
    assert.equal(q.isFull, true);
  });
});
