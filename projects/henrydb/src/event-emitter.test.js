// event-emitter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from './event-emitter.js';

describe('EventEmitter', () => {
  it('on and emit', () => {
    const ee = new EventEmitter();
    let val = 0;
    ee.on('test', (v) => val = v);
    ee.emit('test', 42);
    assert.equal(val, 42);
  });

  it('once fires only once', () => {
    const ee = new EventEmitter();
    let count = 0;
    ee.once('x', () => count++);
    ee.emit('x'); ee.emit('x');
    assert.equal(count, 1);
  });

  it('wildcard listener', () => {
    const ee = new EventEmitter();
    const events = [];
    ee.on('*', (name) => events.push(name));
    ee.emit('a'); ee.emit('b');
    assert.deepEqual(events, ['a', 'b']);
  });
});
