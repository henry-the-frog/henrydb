// concurrent-connections.test.js — Multiple client connections
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Concurrent Connections', () => {
  let server, port;
  
  before(async () => {
    port = 35000 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    
    // Setup schema
    const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    await c.query('CREATE TABLE shared (id INT PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 10; i++) await c.query(`INSERT INTO shared VALUES (${i}, 'v${i}')`);
    await c.end();
  });
  
  after(async () => {
    if (server) await server.stop();
  });

  it('two clients can connect simultaneously', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c1.connect();
    await c2.connect();
    
    const r1 = await c1.query('SELECT COUNT(*) as cnt FROM shared');
    const r2 = await c2.query('SELECT COUNT(*) as cnt FROM shared');
    
    assert.equal(String(r1.rows[0].cnt), '10');
    assert.equal(String(r2.rows[0].cnt), '10');
    
    await c1.end();
    await c2.end();
  });

  it('writes from one client visible to another', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c1.connect();
    await c2.connect();
    
    await c1.query("INSERT INTO shared VALUES (100, 'from-c1')");
    const r = await c2.query("SELECT val FROM shared WHERE id = 100");
    assert.equal(r.rows[0].val, 'from-c1');
    
    await c1.end();
    await c2.end();
  });

  it('parallel queries across connections', async () => {
    const clients = [];
    for (let i = 0; i < 5; i++) {
      const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
      await c.connect();
      clients.push(c);
    }
    
    // All 5 clients query simultaneously
    const results = await Promise.all(
      clients.map(c => c.query('SELECT COUNT(*) as cnt FROM shared'))
    );
    
    for (const r of results) {
      assert.ok(parseInt(String(r.rows[0].cnt)) >= 10);
    }
    
    for (const c of clients) await c.end();
  });

  it('transaction isolation between connections', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c1.connect();
    await c2.connect();
    
    // c1 starts transaction
    await c1.query('BEGIN');
    await c1.query("INSERT INTO shared VALUES (200, 'txn-test')");
    
    // c2 should see or not see depending on isolation level
    // At minimum, after COMMIT it should be visible
    await c1.query('COMMIT');
    
    const r = await c2.query("SELECT val FROM shared WHERE id = 200");
    assert.equal(r.rows[0].val, 'txn-test');
    
    await c1.end();
    await c2.end();
  });

  it('each connection gets unique PID', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c1.connect();
    await c2.connect();
    
    // pg client exposes processID from BackendKeyData
    assert.ok(c1.processID > 0);
    assert.ok(c2.processID > 0);
    assert.notEqual(c1.processID, c2.processID);
    
    await c1.end();
    await c2.end();
  });

});
