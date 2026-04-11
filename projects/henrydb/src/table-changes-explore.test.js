// table-changes-explore.test.js — Testing automatic table change notifications
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 31000 + Math.floor(Math.random() * 10000);
}

describe('Table Change Notifications', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-changes-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    const setup = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await setup.connect();
    await setup.query("CREATE TABLE users (id INT, name TEXT, email TEXT)");
    await setup.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')");
    await setup.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('INSERT triggers table_changes notification', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const writer = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await writer.connect();
    
    const changes = [];
    listener.on('notification', msg => {
      changes.push(JSON.parse(msg.payload));
    });
    
    await listener.query('LISTEN table_changes');
    await new Promise(r => setTimeout(r, 50));
    
    await writer.query("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com')");
    await new Promise(r => setTimeout(r, 100));
    
    console.log('Changes:', changes);
    assert.equal(changes.length, 1);
    assert.equal(changes[0].action, 'INSERT');
    assert.equal(changes[0].table, 'users');
    assert.ok(changes[0].timestamp > 0);
    
    await listener.end();
    await writer.end();
  });

  it('UPDATE triggers notification', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const writer = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await writer.connect();
    
    const changes = [];
    listener.on('notification', msg => changes.push(JSON.parse(msg.payload)));
    
    await listener.query('LISTEN table_changes');
    await new Promise(r => setTimeout(r, 50));
    
    await writer.query("UPDATE users SET name = 'Updated' WHERE id = 1");
    await new Promise(r => setTimeout(r, 100));
    
    assert.equal(changes.length, 1);
    assert.equal(changes[0].action, 'UPDATE');
    assert.equal(changes[0].table, 'users');
    
    await listener.end();
    await writer.end();
  });

  it('DELETE triggers notification', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const writer = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await writer.connect();
    
    const changes = [];
    listener.on('notification', msg => changes.push(JSON.parse(msg.payload)));
    
    await listener.query('LISTEN table_changes');
    await new Promise(r => setTimeout(r, 50));
    
    // Insert then delete so we have something to delete
    await writer.query("INSERT INTO users VALUES (99, 'temp', 'temp@test.com')");
    await writer.query("DELETE FROM users WHERE id = 99");
    await new Promise(r => setTimeout(r, 100));
    
    assert.equal(changes.length, 2); // INSERT + DELETE
    assert.equal(changes[0].action, 'INSERT');
    assert.equal(changes[1].action, 'DELETE');
    
    await listener.end();
    await writer.end();
  });

  it('multiple changes produce multiple notifications', async () => {
    const listener = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const writer = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    
    await listener.connect();
    await writer.connect();
    
    const changes = [];
    listener.on('notification', msg => changes.push(JSON.parse(msg.payload)));
    
    await listener.query('LISTEN table_changes');
    await new Promise(r => setTimeout(r, 50));
    
    await writer.query("INSERT INTO users VALUES (10, 'a', 'a@test.com')");
    await writer.query("INSERT INTO users VALUES (11, 'b', 'b@test.com')");
    await writer.query("UPDATE users SET name = 'updated' WHERE id = 10");
    await new Promise(r => setTimeout(r, 100));
    
    console.log('All changes:', changes.map(c => `${c.action}:${c.table}`));
    assert.equal(changes.length, 3);
    
    await listener.end();
    await writer.end();
  });

  it('writer does not receive its own notifications', async () => {
    const both = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await both.connect();
    
    const changes = [];
    both.on('notification', msg => changes.push(JSON.parse(msg.payload)));
    
    await both.query('LISTEN table_changes');
    await new Promise(r => setTimeout(r, 50));
    
    await both.query("INSERT INTO users VALUES (20, 'self', 'self@test.com')");
    await new Promise(r => setTimeout(r, 100));
    
    // Self-notifications should be excluded (we send to all EXCEPT source)
    console.log('Self-notifications:', changes.length);
    assert.equal(changes.length, 0, 'Writer should not receive its own change notifications');
    
    await both.end();
  });
});
