// advisory-locks.test.js — Tests for pg_advisory_lock/unlock/try
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 35000 + Math.floor(Math.random() * 10000);
}

describe('Advisory Locks', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-adv-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('pg_advisory_lock and unlock', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('SELECT pg_advisory_lock(123)');
    const unlock = await client.query('SELECT pg_advisory_unlock(123)');
    assert.equal(unlock.rows[0].pg_advisory_unlock, "t");
    
    await client.end();
  });

  it('pg_try_advisory_lock returns true when available', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const result = await client.query('SELECT pg_try_advisory_lock(456)');
    assert.equal(result.rows[0].pg_try_advisory_lock, "t");
    
    await client.query('SELECT pg_advisory_unlock(456)');
    await client.end();
  });

  it('pg_try_advisory_lock returns false when held', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();
    
    // c1 takes the lock
    await c1.query('SELECT pg_advisory_lock(789)');
    
    // c2 tries — should fail
    const result = await c2.query('SELECT pg_try_advisory_lock(789)');
    assert.equal(result.rows[0].pg_try_advisory_lock, "f");
    
    // c1 releases
    await c1.query('SELECT pg_advisory_unlock(789)');
    
    // c2 should now succeed
    const result2 = await c2.query('SELECT pg_try_advisory_lock(789)');
    assert.equal(result2.rows[0].pg_try_advisory_lock, "t");
    
    await c2.query('SELECT pg_advisory_unlock(789)');
    await c1.end();
    await c2.end();
  });

  it('locks released on disconnect', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();
    
    // c1 takes lock then disconnects
    await c1.query('SELECT pg_advisory_lock(999)');
    await c1.end();
    
    // Wait for disconnect
    await new Promise(r => setTimeout(r, 50));
    
    // c2 should be able to take it
    const result = await c2.query('SELECT pg_try_advisory_lock(999)');
    assert.equal(result.rows[0].pg_try_advisory_lock, "t");
    
    await c2.query('SELECT pg_advisory_unlock(999)');
    await c2.end();
  });

  it('same connection can re-acquire its own lock', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('SELECT pg_advisory_lock(111)');
    // Same connection should be able to lock again (it already holds it)
    const result = await client.query('SELECT pg_try_advisory_lock(111)');
    assert.equal(result.rows[0].pg_try_advisory_lock, "t");
    
    await client.query('SELECT pg_advisory_unlock(111)');
    await client.end();
  });

  it('different keys are independent', async () => {
    const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();
    
    await c1.query('SELECT pg_advisory_lock(100)');
    
    // c2 can take a different key
    const result = await c2.query('SELECT pg_try_advisory_lock(200)');
    assert.equal(result.rows[0].pg_try_advisory_lock, "t");
    
    await c1.query('SELECT pg_advisory_unlock(100)');
    await c2.query('SELECT pg_advisory_unlock(200)');
    await c1.end();
    await c2.end();
  });
});
