// server-pubsub.test.js — LISTEN/NOTIFY pub/sub through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 30000 + Math.floor(Math.random() * 5000);
}

describe('LISTEN/NOTIFY Pub/Sub', () => {
  let server, port;
  
  before(async () => {
    port = getPort();
    server = new HenryDBServer({ port });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
  });

  it('basic notification delivery', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await listener.connect();
    await notifier.connect();
    
    await listener.query('LISTEN events');
    
    const received = [];
    listener.on('notification', msg => received.push(msg));
    
    await notifier.query("NOTIFY events, 'hello'");
    await new Promise(r => setTimeout(r, 200));
    
    assert.equal(received.length, 1);
    assert.equal(received[0].channel, 'events');
    assert.equal(received[0].payload, 'hello');
    
    await listener.end();
    await notifier.end();
  });

  it('multiple notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await listener.connect();
    await notifier.connect();
    
    await listener.query('LISTEN updates');
    
    const received = [];
    listener.on('notification', msg => received.push(msg.payload));
    
    await notifier.query("NOTIFY updates, 'msg1'");
    await notifier.query("NOTIFY updates, 'msg2'");
    await notifier.query("NOTIFY updates, 'msg3'");
    await new Promise(r => setTimeout(r, 200));
    
    assert.equal(received.length, 3);
    assert.deepEqual(received, ['msg1', 'msg2', 'msg3']);
    
    await listener.end();
    await notifier.end();
  });

  it('multiple channels', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await listener.connect();
    await notifier.connect();
    
    await listener.query('LISTEN channel_a');
    await listener.query('LISTEN channel_b');
    
    const received = [];
    listener.on('notification', msg => received.push({ ch: msg.channel, payload: msg.payload }));
    
    await notifier.query("NOTIFY channel_a, 'from-a'");
    await notifier.query("NOTIFY channel_b, 'from-b'");
    await notifier.query("NOTIFY channel_c, 'from-c'"); // Not listening to this
    await new Promise(r => setTimeout(r, 200));
    
    assert.equal(received.length, 2);
    assert.equal(received[0].ch, 'channel_a');
    assert.equal(received[1].ch, 'channel_b');
    
    await listener.end();
    await notifier.end();
  });

  it('UNLISTEN stops notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await listener.connect();
    await notifier.connect();
    
    await listener.query('LISTEN temp');
    
    const received = [];
    listener.on('notification', msg => received.push(msg.payload));
    
    await notifier.query("NOTIFY temp, 'before'");
    await new Promise(r => setTimeout(r, 200));
    assert.equal(received.length, 1);
    
    await listener.query('UNLISTEN temp');
    await notifier.query("NOTIFY temp, 'after'");
    await new Promise(r => setTimeout(r, 200));
    assert.equal(received.length, 1); // Should not receive 'after'
    
    await listener.end();
    await notifier.end();
  });

  it('multiple listeners on same channel', async () => {
    const l1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const l2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const notifier = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await l1.connect();
    await l2.connect();
    await notifier.connect();
    
    await l1.query('LISTEN broadcast');
    await l2.query('LISTEN broadcast');
    
    const r1 = [], r2 = [];
    l1.on('notification', msg => r1.push(msg.payload));
    l2.on('notification', msg => r2.push(msg.payload));
    
    await notifier.query("NOTIFY broadcast, 'to-all'");
    await new Promise(r => setTimeout(r, 200));
    
    assert.equal(r1.length, 1);
    assert.equal(r2.length, 1);
    assert.equal(r1[0], 'to-all');
    assert.equal(r2[0], 'to-all');
    
    await l1.end();
    await l2.end();
    await notifier.end();
  });
});
