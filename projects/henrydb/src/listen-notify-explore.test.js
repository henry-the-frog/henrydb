// listen-notify-explore.test.js — Testing LISTEN/NOTIFY pub/sub
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 27000 + Math.floor(Math.random() * 10000);
}

describe('LISTEN/NOTIFY Pub/Sub', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-ln-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('basic LISTEN/NOTIFY', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await notifier.connect();
    
    const notifications = [];
    listener.on('notification', (msg) => {
      notifications.push(msg);
    });
    
    await listener.query('LISTEN test_channel');
    
    // Give it a moment
    await new Promise(r => setTimeout(r, 50));
    
    await notifier.query("NOTIFY test_channel, 'hello world'");
    
    // Wait for notification
    await new Promise(r => setTimeout(r, 100));
    
    console.log('Notifications received:', notifications);
    assert.equal(notifications.length, 1);
    assert.equal(notifications[0].channel, 'test_channel');
    assert.equal(notifications[0].payload, 'hello world');
    
    await listener.end();
    await notifier.end();
  });

  it('multiple listeners on same channel', async () => {
    const listener1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const listener2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener1.connect();
    await listener2.connect();
    await notifier.connect();
    
    const notifs1 = [];
    const notifs2 = [];
    listener1.on('notification', msg => notifs1.push(msg));
    listener2.on('notification', msg => notifs2.push(msg));
    
    await listener1.query('LISTEN broadcast');
    await listener2.query('LISTEN broadcast');
    
    await new Promise(r => setTimeout(r, 50));
    await notifier.query("NOTIFY broadcast, 'to everyone'");
    await new Promise(r => setTimeout(r, 100));
    
    console.log('Listener 1:', notifs1.length, 'Listener 2:', notifs2.length);
    assert.equal(notifs1.length, 1, 'Listener 1 should receive');
    assert.equal(notifs2.length, 1, 'Listener 2 should receive');
    
    await listener1.end();
    await listener2.end();
    await notifier.end();
  });

  it('UNLISTEN stops notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await notifier.connect();
    
    const notifs = [];
    listener.on('notification', msg => notifs.push(msg));
    
    await listener.query('LISTEN temp_channel');
    await new Promise(r => setTimeout(r, 50));
    
    await notifier.query("NOTIFY temp_channel, 'first'");
    await new Promise(r => setTimeout(r, 100));
    assert.equal(notifs.length, 1);
    
    await listener.query('UNLISTEN temp_channel');
    await new Promise(r => setTimeout(r, 50));
    
    await notifier.query("NOTIFY temp_channel, 'second'");
    await new Promise(r => setTimeout(r, 100));
    assert.equal(notifs.length, 1, 'Should not receive after UNLISTEN');
    
    await listener.end();
    await notifier.end();
  });

  it('NOTIFY without payload', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await notifier.connect();
    
    const notifs = [];
    listener.on('notification', msg => notifs.push(msg));
    
    await listener.query('LISTEN ping');
    await new Promise(r => setTimeout(r, 50));
    
    await notifier.query("NOTIFY ping");
    await new Promise(r => setTimeout(r, 100));
    
    assert.equal(notifs.length, 1);
    assert.equal(notifs[0].channel, 'ping');
    console.log('Empty payload:', JSON.stringify(notifs[0].payload));
    
    await listener.end();
    await notifier.end();
  });

  it('multiple channels per listener', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await notifier.connect();
    
    const notifs = [];
    listener.on('notification', msg => notifs.push(msg));
    
    await listener.query('LISTEN channel_a');
    await listener.query('LISTEN channel_b');
    await listener.query('LISTEN channel_c');
    await new Promise(r => setTimeout(r, 50));
    
    await notifier.query("NOTIFY channel_b, 'only b'");
    await new Promise(r => setTimeout(r, 100));
    
    assert.equal(notifs.length, 1);
    assert.equal(notifs[0].channel, 'channel_b');
    assert.equal(notifs[0].payload, 'only b');
    
    await listener.end();
    await notifier.end();
  });
});
