// ring-buffer.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RingBuffer } from './ring-buffer.js';

describe('RingBuffer', () => {
  it('basic push/get', () => {
    const rb = new RingBuffer(5);
    rb.push('a'); rb.push('b'); rb.push('c');
    assert.equal(rb.get(0), 'a');
    assert.equal(rb.get(2), 'c');
    assert.equal(rb.size, 3);
  });

  it('overwrites oldest when full', () => {
    const rb = new RingBuffer(3);
    rb.push(1); rb.push(2); rb.push(3);
    assert.ok(rb.isFull);
    rb.push(4); // Overwrites 1
    assert.deepEqual(rb.toArray(), [2, 3, 4]);
  });

  it('peek and peekOldest', () => {
    const rb = new RingBuffer(5);
    rb.push('x'); rb.push('y'); rb.push('z');
    assert.equal(rb.peek(), 'z');
    assert.equal(rb.peekOldest(), 'x');
  });

  it('pop', () => {
    const rb = new RingBuffer(5);
    rb.push(1); rb.push(2); rb.push(3);
    assert.equal(rb.pop(), 3);
    assert.equal(rb.pop(), 2);
    assert.equal(rb.size, 1);
  });

  it('iteration in order', () => {
    const rb = new RingBuffer(4);
    for (let i = 0; i < 6; i++) rb.push(i); // 0,1,2,3,4,5 → keeps 2,3,4,5
    assert.deepEqual(rb.toArray(), [2, 3, 4, 5]);
  });

  it('clear', () => {
    const rb = new RingBuffer(3);
    rb.push(1); rb.push(2);
    rb.clear();
    assert.ok(rb.isEmpty);
    assert.equal(rb.size, 0);
  });

  it('empty buffer', () => {
    const rb = new RingBuffer(3);
    assert.ok(rb.isEmpty);
    assert.equal(rb.peek(), undefined);
    assert.equal(rb.pop(), undefined);
    assert.equal(rb.get(0), undefined);
  });

  it('benchmark: 1M push into size-1000 buffer', () => {
    const rb = new RingBuffer(1000);
    const t0 = Date.now();
    for (let i = 0; i < 1000000; i++) rb.push(i);
    const ms = Date.now() - t0;
    console.log(`    1M pushes: ${ms}ms`);
    assert.equal(rb.size, 1000);
    assert.equal(rb.peek(), 999999);
  });
});
