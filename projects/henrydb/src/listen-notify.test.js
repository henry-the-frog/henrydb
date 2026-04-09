// listen-notify.test.js — Tests for LISTEN/NOTIFY event system
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { NotificationManager } from './listen-notify.js';

let nm;

describe('NotificationManager', () => {
  beforeEach(() => {
    nm = new NotificationManager();
  });

  test('basic LISTEN and NOTIFY', () => {
    nm.listen('client1', 'events');
    const delivered = nm.notify('client2', 'events', 'hello');
    
    assert.equal(delivered, 1);
    const msgs = nm.drain('client1');
    assert.equal(msgs.length, 1);
    assert.equal(msgs[0].channel, 'events');
    assert.equal(msgs[0].payload, 'hello');
  });

  test('sender does not receive own notification', () => {
    nm.listen('client1', 'events');
    const delivered = nm.notify('client1', 'events', 'self-msg');
    
    assert.equal(delivered, 0);
    const msgs = nm.drain('client1');
    assert.equal(msgs.length, 0);
  });

  test('multiple listeners receive notification', () => {
    nm.listen('client1', 'events');
    nm.listen('client2', 'events');
    nm.listen('client3', 'events');
    
    const delivered = nm.notify('sender', 'events', 'broadcast');
    assert.equal(delivered, 3);
    
    assert.equal(nm.drain('client1').length, 1);
    assert.equal(nm.drain('client2').length, 1);
    assert.equal(nm.drain('client3').length, 1);
  });

  test('UNLISTEN removes subscription', () => {
    nm.listen('client1', 'events');
    nm.unlisten('client1', 'events');
    
    nm.notify('sender', 'events', 'msg');
    assert.equal(nm.drain('client1').length, 0);
  });

  test('UNLISTEN * removes all subscriptions', () => {
    nm.listen('client1', 'channel_a');
    nm.listen('client1', 'channel_b');
    nm.listen('client1', 'channel_c');
    
    nm.unlisten('client1', '*');
    
    nm.notify('sender', 'channel_a', 'msg');
    nm.notify('sender', 'channel_b', 'msg');
    assert.equal(nm.drain('client1').length, 0);
  });

  test('notifications accumulate in queue', () => {
    nm.listen('client1', 'events');
    
    nm.notify('sender', 'events', 'msg1');
    nm.notify('sender', 'events', 'msg2');
    nm.notify('sender', 'events', 'msg3');
    
    const msgs = nm.drain('client1');
    assert.equal(msgs.length, 3);
    assert.equal(msgs[0].payload, 'msg1');
    assert.equal(msgs[2].payload, 'msg3');
  });

  test('drain clears queue', () => {
    nm.listen('client1', 'events');
    nm.notify('sender', 'events', 'msg');
    
    assert.equal(nm.drain('client1').length, 1);
    assert.equal(nm.drain('client1').length, 0); // Empty after drain
  });

  test('peek does not clear queue', () => {
    nm.listen('client1', 'events');
    nm.notify('sender', 'events', 'msg');
    
    assert.equal(nm.peek('client1').length, 1);
    assert.equal(nm.peek('client1').length, 1); // Still there
    assert.equal(nm.drain('client1').length, 1); // Now drain
    assert.equal(nm.peek('client1').length, 0);
  });

  test('multiple channels independent', () => {
    nm.listen('client1', 'channel_a');
    nm.listen('client2', 'channel_b');
    
    nm.notify('sender', 'channel_a', 'for_a');
    nm.notify('sender', 'channel_b', 'for_b');
    
    const a = nm.drain('client1');
    const b = nm.drain('client2');
    assert.equal(a.length, 1);
    assert.equal(a[0].payload, 'for_a');
    assert.equal(b.length, 1);
    assert.equal(b[0].payload, 'for_b');
  });

  test('notify to empty channel returns 0', () => {
    const delivered = nm.notify('sender', 'nonexistent', 'msg');
    assert.equal(delivered, 0);
  });

  test('channel names are case-insensitive', () => {
    nm.listen('client1', 'MyChannel');
    nm.notify('sender', 'mychannel', 'msg');
    
    assert.equal(nm.drain('client1').length, 1);
  });

  test('notification includes timestamp', () => {
    nm.listen('client1', 'events');
    const before = Date.now();
    nm.notify('sender', 'events', 'msg');
    const after = Date.now();
    
    const msgs = nm.drain('client1');
    assert.ok(msgs[0].timestamp >= before);
    assert.ok(msgs[0].timestamp <= after);
  });

  test('notification includes sender info', () => {
    nm.listen('client1', 'events');
    nm.notify('client99', 'events', 'msg');
    
    const msgs = nm.drain('client1');
    assert.equal(msgs[0].senderId, 'client99');
  });

  test('removeListener_id cleans up everything', () => {
    nm.listen('client1', 'a');
    nm.listen('client1', 'b');
    nm.notify('sender', 'a', 'msg');
    
    nm.removeListener_id('client1');
    
    assert.equal(nm.getListenerChannels('client1').length, 0);
    assert.equal(nm.peek('client1').length, 0);
  });

  test('getChannels returns active channels', () => {
    nm.listen('c1', 'events');
    nm.listen('c2', 'events');
    nm.listen('c3', 'logs');
    
    const channels = nm.getChannels();
    assert.equal(channels.length, 2);
    
    const eventsChannel = channels.find(c => c.channel === 'events');
    assert.equal(eventsChannel.subscribers, 2);
  });

  test('getListenerChannels returns subscriptions', () => {
    nm.listen('client1', 'a');
    nm.listen('client1', 'b');
    nm.listen('client1', 'c');
    
    const channels = nm.getListenerChannels('client1');
    assert.equal(channels.length, 3);
    assert.ok(channels.includes('a'));
    assert.ok(channels.includes('b'));
    assert.ok(channels.includes('c'));
  });

  test('waitForNotification resolves on notification', async () => {
    nm.listen('client1', 'events');
    
    // Send notification after a small delay
    setTimeout(() => nm.notify('sender', 'events', 'async_msg'), 20);
    
    const msgs = await nm.waitForNotification('client1', 1000);
    assert.equal(msgs.length, 1);
    assert.equal(msgs[0].payload, 'async_msg');
  });

  test('waitForNotification times out', async () => {
    nm.listen('client1', 'events');
    
    const msgs = await nm.waitForNotification('client1', 50);
    assert.equal(msgs.length, 0);
  });

  test('waitForNotification returns existing queued messages', async () => {
    nm.listen('client1', 'events');
    nm.notify('sender', 'events', 'already_queued');
    
    const msgs = await nm.waitForNotification('client1', 50);
    assert.equal(msgs.length, 1);
    assert.equal(msgs[0].payload, 'already_queued');
  });

  test('stats tracking', () => {
    nm.listen('c1', 'events');
    nm.listen('c2', 'events');
    nm.notify('sender', 'events', 'msg');
    nm.unlisten('c1', 'events');
    
    const stats = nm.getStats();
    assert.equal(stats.totalListens, 2);
    assert.equal(stats.totalUnlistens, 1);
    assert.equal(stats.totalNotifications, 1);
    assert.equal(stats.totalDeliveries, 2);
    assert.equal(stats.activeChannels, 1);
    assert.equal(stats.activeListeners, 1);
  });

  test('empty payload defaults to empty string', () => {
    nm.listen('client1', 'events');
    nm.notify('sender', 'events');
    
    const msgs = nm.drain('client1');
    assert.equal(msgs[0].payload, '');
  });
});
