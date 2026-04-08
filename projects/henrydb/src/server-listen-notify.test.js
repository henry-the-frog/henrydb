// server-listen-notify.test.js — Tests for LISTEN/NOTIFY pub-sub
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15490;

describe('LISTEN/NOTIFY', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('basic LISTEN + NOTIFY', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    // Subscribe
    await listener.query('LISTEN test_channel');

    // Collect notifications
    const notifications = [];
    listener.on('notification', (msg) => {
      notifications.push(msg);
    });

    // Send notification
    await sender.query("NOTIFY test_channel, 'hello world'");

    // Wait for delivery
    await new Promise(r => setTimeout(r, 200));

    assert.strictEqual(notifications.length, 1);
    assert.strictEqual(notifications[0].channel, 'test_channel');
    assert.strictEqual(notifications[0].payload, 'hello world');

    await listener.end();
    await sender.end();
  });

  it('multiple channels', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN channel_a');
    await listener.query('LISTEN channel_b');

    const notifications = [];
    listener.on('notification', (msg) => notifications.push(msg));

    await sender.query("NOTIFY channel_a, 'message_a'");
    await sender.query("NOTIFY channel_b, 'message_b'");
    await sender.query("NOTIFY channel_c, 'message_c'"); // nobody listening

    await new Promise(r => setTimeout(r, 200));

    assert.strictEqual(notifications.length, 2);
    assert.strictEqual(notifications[0].channel, 'channel_a');
    assert.strictEqual(notifications[1].channel, 'channel_b');

    await listener.end();
    await sender.end();
  });

  it('multiple listeners on same channel', async () => {
    const l1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const l2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await l1.connect();
    await l2.connect();
    await sender.connect();

    await l1.query('LISTEN shared');
    await l2.query('LISTEN shared');

    const n1 = [], n2 = [];
    l1.on('notification', (msg) => n1.push(msg));
    l2.on('notification', (msg) => n2.push(msg));

    await sender.query("NOTIFY shared, 'broadcast'");
    await new Promise(r => setTimeout(r, 200));

    assert.strictEqual(n1.length, 1);
    assert.strictEqual(n2.length, 1);
    assert.strictEqual(n1[0].payload, 'broadcast');
    assert.strictEqual(n2[0].payload, 'broadcast');

    await l1.end();
    await l2.end();
    await sender.end();
  });

  it('UNLISTEN stops notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN unsub_test');
    const notifications = [];
    listener.on('notification', (msg) => notifications.push(msg));

    // First notification should arrive
    await sender.query("NOTIFY unsub_test, 'first'");
    await new Promise(r => setTimeout(r, 100));
    assert.strictEqual(notifications.length, 1);

    // Unsubscribe
    await listener.query('UNLISTEN unsub_test');

    // Second notification should NOT arrive
    await sender.query("NOTIFY unsub_test, 'second'");
    await new Promise(r => setTimeout(r, 100));
    assert.strictEqual(notifications.length, 1); // Still 1

    await listener.end();
    await sender.end();
  });

  it('UNLISTEN * stops all notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN chan1');
    await listener.query('LISTEN chan2');
    await listener.query('LISTEN chan3');
    await listener.query('UNLISTEN *');

    const notifications = [];
    listener.on('notification', (msg) => notifications.push(msg));

    await sender.query("NOTIFY chan1, 'a'");
    await sender.query("NOTIFY chan2, 'b'");
    await sender.query("NOTIFY chan3, 'c'");
    await new Promise(r => setTimeout(r, 100));

    assert.strictEqual(notifications.length, 0);

    await listener.end();
    await sender.end();
  });

  it('NOTIFY without payload', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN empty_payload');
    const notifications = [];
    listener.on('notification', (msg) => notifications.push(msg));

    await sender.query('NOTIFY empty_payload');
    await new Promise(r => setTimeout(r, 100));

    assert.strictEqual(notifications.length, 1);
    assert.strictEqual(notifications[0].channel, 'empty_payload');
    assert.strictEqual(notifications[0].payload, '');

    await listener.end();
    await sender.end();
  });

  it('self-notify (sender also receives)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('LISTEN self_test');
    const notifications = [];
    client.on('notification', (msg) => notifications.push(msg));

    await client.query("NOTIFY self_test, 'self'");
    await new Promise(r => setTimeout(r, 100));

    assert.strictEqual(notifications.length, 1);
    assert.strictEqual(notifications[0].payload, 'self');

    await client.end();
  });

  it('rapid-fire notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN rapid');
    const notifications = [];
    listener.on('notification', (msg) => notifications.push(msg));

    // Send 20 notifications rapidly
    for (let i = 0; i < 20; i++) {
      await sender.query(`NOTIFY rapid, '${i}'`);
    }
    await new Promise(r => setTimeout(r, 300));

    assert.strictEqual(notifications.length, 20);
    assert.strictEqual(notifications[0].payload, '0');
    assert.strictEqual(notifications[19].payload, '19');

    await listener.end();
    await sender.end();
  });

  it('disconnect cleans up subscriptions', async () => {
    const listener = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sender = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await listener.connect();
    await sender.connect();

    await listener.query('LISTEN cleanup_test');

    // Check channel exists
    assert.ok(server._channels.has('cleanup_test'));

    // Disconnect listener
    await listener.end();
    await new Promise(r => setTimeout(r, 100));

    // Channel should be cleaned up (no more listeners)
    assert.ok(!server._channels.has('cleanup_test') || server._channels.get('cleanup_test').size === 0);

    await sender.end();
  });

  it('LISTEN + regular queries work together', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('LISTEN mixed');
    const notifications = [];
    client.on('notification', (msg) => notifications.push(msg));

    // Regular queries should still work
    await client.query('CREATE TABLE notify_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO notify_test VALUES (1, 'test')");
    const r = await client.query('SELECT * FROM notify_test');
    assert.strictEqual(r.rows.length, 1);

    // Notifications should also work
    await client.query("NOTIFY mixed, 'works_with_queries'");
    await new Promise(r => setTimeout(r, 100));
    assert.strictEqual(notifications.length, 1);

    await client.end();
  });
});
