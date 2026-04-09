// db-events.test.js — Tests for database event emitter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseEventEmitter } from './db-events.js';

describe('Database Event Emitter', () => {
  it('emits and receives events', () => {
    const events = new DatabaseEventEmitter();
    let received = null;
    events.on('users', 'INSERT', (data) => { received = data; });
    events.emit('users', 'INSERT', { id: 1 });
    assert.ok(received);
    assert.equal(received.table, 'users');
    assert.equal(received.event, 'INSERT');
    assert.equal(received.data.id, 1);
  });

  it('wildcard * table matches all tables', () => {
    const events = new DatabaseEventEmitter();
    const caught = [];
    events.on('*', 'INSERT', (d) => caught.push(d));
    events.emit('users', 'INSERT', {});
    events.emit('posts', 'INSERT', {});
    assert.equal(caught.length, 2);
  });

  it('wildcard * event matches all events', () => {
    const events = new DatabaseEventEmitter();
    const caught = [];
    events.on('users', '*', (d) => caught.push(d));
    events.emit('users', 'INSERT', {});
    events.emit('users', 'UPDATE', {});
    events.emit('users', 'DELETE', {});
    assert.equal(caught.length, 3);
  });

  it('full wildcard *:* catches everything', () => {
    const events = new DatabaseEventEmitter();
    let count = 0;
    events.on('*', '*', () => count++);
    events.emit('a', 'INSERT', {});
    events.emit('b', 'UPDATE', {});
    events.emit('c', 'DELETE', {});
    assert.equal(count, 3);
  });

  it('unsubscribe works', () => {
    const events = new DatabaseEventEmitter();
    let count = 0;
    const unsub = events.on('t', 'INSERT', () => count++);
    events.emit('t', 'INSERT', {});
    assert.equal(count, 1);
    unsub();
    events.emit('t', 'INSERT', {});
    assert.equal(count, 1); // No change
  });

  it('once fires only once', () => {
    const events = new DatabaseEventEmitter();
    let count = 0;
    events.once('t', 'INSERT', () => count++);
    events.emit('t', 'INSERT', {});
    events.emit('t', 'INSERT', {});
    assert.equal(count, 1);
  });

  it('off removes table listeners', () => {
    const events = new DatabaseEventEmitter();
    let count = 0;
    events.on('users', 'INSERT', () => count++);
    events.on('users', 'UPDATE', () => count++);
    events.off('users');
    events.emit('users', 'INSERT', {});
    assert.equal(count, 0);
  });

  it('off() with no args clears all', () => {
    const events = new DatabaseEventEmitter();
    events.on('a', 'INSERT', () => {});
    events.on('b', 'UPDATE', () => {});
    events.off();
    assert.equal(events.listenerCount, 0);
  });

  it('tracks statistics', () => {
    const events = new DatabaseEventEmitter();
    events.on('t', 'INSERT', () => {});
    events.emit('t', 'INSERT', {});
    events.emit('t', 'INSERT', {});
    const s = events.stats();
    assert.equal(s.emitted, 2);
    assert.equal(s.delivered, 2);
    assert.equal(s.listeners, 1);
  });

  it('multiple listeners on same event', () => {
    const events = new DatabaseEventEmitter();
    const results = [];
    events.on('t', 'INSERT', () => results.push('a'));
    events.on('t', 'INSERT', () => results.push('b'));
    events.emit('t', 'INSERT', {});
    assert.deepEqual(results, ['a', 'b']);
  });

  it('event includes timestamp', () => {
    const events = new DatabaseEventEmitter();
    let received = null;
    events.on('t', 'INSERT', (d) => { received = d; });
    events.emit('t', 'INSERT', {});
    assert.ok(received.timestamp > 0);
  });
});
