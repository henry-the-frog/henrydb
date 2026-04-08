// server-cdc.test.js — Change Data Capture pattern via LISTEN/NOTIFY
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15524;

describe('Change Data Capture (CDC)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('captures INSERT events via NOTIFY', async () => {
    const publisher = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const subscriber = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await publisher.connect();
    await subscriber.connect();

    await publisher.query('CREATE TABLE cdc_orders (id INTEGER, product TEXT, amount REAL)');

    // Subscribe to changes
    const notifications = [];
    subscriber.on('notification', msg => notifications.push(msg));
    await subscriber.query('LISTEN order_changes');

    // Publish change events
    await publisher.query("NOTIFY order_changes, 'INSERT:1:Widget:29.99'");
    await publisher.query("NOTIFY order_changes, 'INSERT:2:Gadget:49.99'");

    await new Promise(r => setTimeout(r, 100));

    assert.ok(notifications.length >= 2);
    assert.ok(notifications[0].payload.includes('INSERT:1'));
    assert.ok(notifications[1].payload.includes('INSERT:2'));

    await publisher.end();
    await subscriber.end();
  });

  it('multi-channel CDC (orders + inventory)', async () => {
    const pub = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sub = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pub.connect();
    await sub.connect();

    const orderEvents = [];
    const inventoryEvents = [];
    sub.on('notification', msg => {
      if (msg.channel === 'orders') orderEvents.push(msg.payload);
      if (msg.channel === 'inventory') inventoryEvents.push(msg.payload);
    });

    await sub.query('LISTEN orders');
    await sub.query('LISTEN inventory');

    await pub.query("NOTIFY orders, 'new_order:1001'");
    await pub.query("NOTIFY inventory, 'stock_update:widget:-1'");
    await pub.query("NOTIFY orders, 'new_order:1002'");

    await new Promise(r => setTimeout(r, 100));

    assert.strictEqual(orderEvents.length, 2);
    assert.strictEqual(inventoryEvents.length, 1);

    await pub.end();
    await sub.end();
  });

  it('multiple subscribers receive same events', async () => {
    const pub = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sub1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sub2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pub.connect();
    await sub1.connect();
    await sub2.connect();

    const events1 = [];
    const events2 = [];
    sub1.on('notification', msg => events1.push(msg.payload));
    sub2.on('notification', msg => events2.push(msg.payload));

    await sub1.query('LISTEN changes');
    await sub2.query('LISTEN changes');

    await pub.query("NOTIFY changes, 'update:user:123'");

    await new Promise(r => setTimeout(r, 100));

    assert.ok(events1.length >= 1);
    assert.ok(events2.length >= 1);
    assert.strictEqual(events1[0], events2[0]);

    await pub.end();
    await sub1.end();
    await sub2.end();
  });

  it('UNLISTEN stops receiving events', async () => {
    const pub = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const sub = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pub.connect();
    await sub.connect();

    const events = [];
    sub.on('notification', msg => events.push(msg.payload));

    await sub.query('LISTEN test_channel');
    await pub.query("NOTIFY test_channel, 'before_unlisten'");
    await new Promise(r => setTimeout(r, 100));

    await sub.query('UNLISTEN test_channel');
    await pub.query("NOTIFY test_channel, 'after_unlisten'");
    await new Promise(r => setTimeout(r, 100));

    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0], 'before_unlisten');

    await pub.end();
    await sub.end();
  });
});
