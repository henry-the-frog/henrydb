// listen-notify.test.js — Tests for LISTEN/NOTIFY/UNLISTEN
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LISTEN/NOTIFY', () => {
  it('NOTIFY triggers callback on listener', () => {
    const db = new Database();
    const received = [];
    db.onNotify('events', (msg) => received.push(msg));
    
    db.execute("NOTIFY events, 'hello'");
    assert.equal(received.length, 1);
    assert.equal(received[0].channel, 'events');
    assert.equal(received[0].payload, 'hello');
  });

  it('NOTIFY without payload sends empty string', () => {
    const db = new Database();
    const received = [];
    db.onNotify('ping', (msg) => received.push(msg));
    
    db.execute('NOTIFY ping');
    assert.equal(received[0].payload, '');
  });

  it('multiple listeners receive notifications', () => {
    const db = new Database();
    const r1 = [], r2 = [];
    db.onNotify('ch', (msg) => r1.push(msg));
    db.onNotify('ch', (msg) => r2.push(msg));
    
    db.execute("NOTIFY ch, 'test'");
    assert.equal(r1.length, 1);
    assert.equal(r2.length, 1);
  });

  it('UNLISTEN stops notifications', () => {
    const db = new Database();
    const received = [];
    db.onNotify('ch', (msg) => received.push(msg));
    
    db.execute("NOTIFY ch, 'before'");
    assert.equal(received.length, 1);
    
    db.execute('UNLISTEN ch');
    db.execute("NOTIFY ch, 'after'");
    assert.equal(received.length, 1, 'Should not receive after UNLISTEN');
  });

  it('UNLISTEN * clears all listeners', () => {
    const db = new Database();
    const r1 = [], r2 = [];
    db.onNotify('ch1', (msg) => r1.push(msg));
    db.onNotify('ch2', (msg) => r2.push(msg));
    
    db.execute("NOTIFY ch1, 'a'");
    db.execute("NOTIFY ch2, 'b'");
    assert.equal(r1.length, 1);
    assert.equal(r2.length, 1);
    
    db.execute('UNLISTEN *');
    db.execute("NOTIFY ch1, 'c'");
    db.execute("NOTIFY ch2, 'd'");
    assert.equal(r1.length, 1, 'Should not receive after UNLISTEN *');
    assert.equal(r2.length, 1);
  });

  it('onNotify returns unsubscribe function', () => {
    const db = new Database();
    const received = [];
    const unsub = db.onNotify('ch', (msg) => received.push(msg));
    
    db.execute("NOTIFY ch, 'a'");
    assert.equal(received.length, 1);
    
    unsub();
    db.execute("NOTIFY ch, 'b'");
    assert.equal(received.length, 1, 'Should not receive after unsubscribe');
  });

  it('getNotifications returns and drains pending', () => {
    const db = new Database();
    
    db.execute("NOTIFY ch1, 'a'");
    db.execute("NOTIFY ch2, 'b'");
    
    const pending = db.getNotifications();
    assert.equal(pending.length, 2);
    assert.equal(pending[0].channel, 'ch1');
    assert.equal(pending[1].channel, 'ch2');
    
    // Should be drained
    assert.equal(db.getNotifications().length, 0);
  });

  it('notifications have timestamp and pid', () => {
    const db = new Database();
    db.execute("NOTIFY ch, 'test'");
    
    const pending = db.getNotifications();
    assert.ok(pending[0].timestamp > 0);
    assert.ok(typeof pending[0].pid === 'number');
  });

  it('SQL LISTEN registers channel', () => {
    const db = new Database();
    db.execute('LISTEN my_channel');
    
    // After LISTEN, the channel should exist
    const r = db.execute('LISTEN my_channel'); // Should not throw (idempotent)
    assert.equal(r.type, 'OK');
  });

  it('different channels are independent', () => {
    const db = new Database();
    const r1 = [], r2 = [];
    db.onNotify('ch1', (msg) => r1.push(msg));
    db.onNotify('ch2', (msg) => r2.push(msg));
    
    db.execute("NOTIFY ch1, 'only ch1'");
    assert.equal(r1.length, 1);
    assert.equal(r2.length, 0, 'ch2 should not receive ch1 notifications');
  });
});
