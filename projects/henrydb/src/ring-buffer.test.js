// ring-buffer.test.js — Tests for ring buffer
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RingBuffer } from './ring-buffer.js';

describe('RingBuffer', () => {
  it('push and shift (FIFO)', () => {
    const rb = new RingBuffer(4);
    rb.push('a'); rb.push('b'); rb.push('c');
    assert.equal(rb.shift(), 'a');
    assert.equal(rb.shift(), 'b');
    assert.equal(rb.shift(), 'c');
    assert.equal(rb.shift(), undefined);
  });

  it('overflow: oldest element overwritten', () => {
    const rb = new RingBuffer(3);
    rb.push(1); rb.push(2); rb.push(3);
    const overwritten = rb.push(4);
    assert.equal(overwritten, 1);
    assert.deepEqual(rb.toArray(), [2, 3, 4]);
  });

  it('peek front and back', () => {
    const rb = new RingBuffer(4);
    rb.push('first'); rb.push('middle'); rb.push('last');
    assert.equal(rb.peekFront(), 'first');
    assert.equal(rb.peekBack(), 'last');
  });

  it('at() random access', () => {
    const rb = new RingBuffer(5);
    for (let i = 0; i < 5; i++) rb.push(i * 10);
    
    assert.equal(rb.at(0), 0);
    assert.equal(rb.at(2), 20);
    assert.equal(rb.at(4), 40);
    assert.equal(rb.at(5), undefined);
  });

  it('iterator', () => {
    const rb = new RingBuffer(3);
    rb.push(1); rb.push(2); rb.push(3);
    assert.deepEqual([...rb], [1, 2, 3]);
  });

  it('clear', () => {
    const rb = new RingBuffer(4);
    rb.push(1); rb.push(2);
    rb.clear();
    assert.equal(rb.size, 0);
    assert.equal(rb.isEmpty, true);
  });

  it('isEmpty and isFull', () => {
    const rb = new RingBuffer(2);
    assert.equal(rb.isEmpty, true);
    assert.equal(rb.isFull, false);
    
    rb.push(1); rb.push(2);
    assert.equal(rb.isEmpty, false);
    assert.equal(rb.isFull, true);
  });

  it('wrap-around maintains FIFO order', () => {
    const rb = new RingBuffer(3);
    rb.push(1); rb.push(2); rb.push(3);
    rb.shift(); // Remove 1
    rb.push(4); // Wraps around
    assert.deepEqual(rb.toArray(), [2, 3, 4]);
  });

  it('overflow counter', () => {
    const rb = new RingBuffer(2);
    rb.push(1); rb.push(2);
    rb.push(3); rb.push(4); // Two overflows
    assert.equal(rb.getStats().overflows, 2);
  });

  it('stress: 100K push through 100-element buffer', () => {
    const rb = new RingBuffer(100);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) rb.push(i);
    const elapsed = performance.now() - t0;
    
    assert.equal(rb.size, 100);
    assert.equal(rb.peekFront(), 99900);
    assert.equal(rb.peekBack(), 99999);
    
    console.log(`  100K push through 100-buf: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
  });

  it('use case: recent query log', () => {
    const queryLog = new RingBuffer(5);
    
    queryLog.push({ sql: 'SELECT * FROM users', time: 1.2 });
    queryLog.push({ sql: 'INSERT INTO logs...', time: 0.5 });
    queryLog.push({ sql: 'UPDATE users SET...', time: 2.3 });
    
    // Access recent queries
    assert.equal(queryLog.peekBack().sql, 'UPDATE users SET...');
    assert.equal(queryLog.size, 3);
    
    // Overflow: old queries automatically discarded
    queryLog.push({ sql: 'q4', time: 0 });
    queryLog.push({ sql: 'q5', time: 0 });
    queryLog.push({ sql: 'q6', time: 0 }); // Overflow!
    
    assert.equal(queryLog.size, 5);
    assert.equal(queryLog.peekFront().sql, 'INSERT INTO logs...');
  });
});
