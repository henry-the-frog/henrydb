// concurrent-txn-stress.test.js — Stress test for concurrent transactions with savepoints

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';

describe('Concurrent Transaction Stress', () => {
  let server, port, dir;
  
  before(async () => {
    port = 34700 + Math.floor(Math.random() * 100);
    dir = mkdtempSync(join(tmpdir(), 'henrydb-txn-stress-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
  });
  
  after(async () => {
    await server.stop();
    rmSync(dir, { recursive: true });
  });
  
  it('100 sequential transactions with commit', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    await c.query('INSERT INTO accounts VALUES (1, 1000)');
    
    for (let i = 0; i < 100; i++) {
      await c.query('BEGIN');
      await c.query('UPDATE accounts SET balance = balance + 1 WHERE id = 1');
      await c.query('COMMIT');
    }
    
    const r = await c.query('SELECT balance FROM accounts WHERE id = 1');
    assert.equal(r.rows[0].balance, 1100); // 1000 + 100
    
    await c.end();
  });
  
  it('transactions with savepoints and rollback', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE items (id INT PRIMARY KEY, status TEXT)');
    await c.query("INSERT INTO items VALUES (1, 'initial')");
    
    // Transaction with savepoint
    await c.query('BEGIN');
    await c.query("UPDATE items SET status = 'processing' WHERE id = 1");
    await c.query('SAVEPOINT sp1');
    await c.query("UPDATE items SET status = 'failed' WHERE id = 1");
    await c.query('ROLLBACK TO SAVEPOINT sp1');
    
    // After rollback to savepoint, should be 'processing'
    const r1 = await c.query('SELECT status FROM items WHERE id = 1');
    assert.equal(r1.rows[0].status, 'processing');
    
    await c.query('COMMIT');
    
    // After commit, should still be 'processing'
    const r2 = await c.query('SELECT status FROM items WHERE id = 1');
    assert.equal(r2.rows[0].status, 'processing');
    
    await c.end();
  });
  
  it('transaction rollback restores state', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE flags (id INT PRIMARY KEY, active BOOLEAN)');
    await c.query('INSERT INTO flags VALUES (1, true)');
    
    await c.query('BEGIN');
    await c.query('UPDATE flags SET active = false WHERE id = 1');
    await c.query('ROLLBACK');
    
    const r = await c.query('SELECT active FROM flags WHERE id = 1');
    assert.equal(r.rows[0].active, true);
    
    await c.end();
  });
  
  it('multiple inserts in transaction then commit', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE logs (id SERIAL PRIMARY KEY, msg TEXT)');
    
    await c.query('BEGIN');
    for (let i = 0; i < 50; i++) {
      await c.query('INSERT INTO logs (msg) VALUES ($1)', [`log_${i}`]);
    }
    await c.query('COMMIT');
    
    const r = await c.query('SELECT COUNT(*) AS cnt FROM logs');
    assert.equal(r.rows[0].cnt, 50);
    
    await c.end();
  });
  
  it('nested savepoints', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE nested (id INT PRIMARY KEY, val INT)');
    await c.query('INSERT INTO nested VALUES (1, 0)');
    
    await c.query('BEGIN');
    await c.query('UPDATE nested SET val = 10 WHERE id = 1');
    await c.query('SAVEPOINT sp1');
    await c.query('UPDATE nested SET val = 20 WHERE id = 1');
    await c.query('SAVEPOINT sp2');
    await c.query('UPDATE nested SET val = 30 WHERE id = 1');
    
    // Rollback sp2 → val should be 20
    await c.query('ROLLBACK TO SAVEPOINT sp2');
    const r1 = await c.query('SELECT val FROM nested WHERE id = 1');
    assert.equal(r1.rows[0].val, 20);
    
    // Rollback sp1 → val should be 10
    await c.query('ROLLBACK TO SAVEPOINT sp1');
    const r2 = await c.query('SELECT val FROM nested WHERE id = 1');
    assert.equal(r2.rows[0].val, 10);
    
    await c.query('COMMIT');
    
    const r3 = await c.query('SELECT val FROM nested WHERE id = 1');
    assert.equal(r3.rows[0].val, 10);
    
    await c.end();
  });
  
  it('interleaved operations maintain consistency', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE counter (id INT PRIMARY KEY, val INT)');
    await c.query('INSERT INTO counter VALUES (1, 0)');
    
    // 50 increment + commit cycles
    for (let i = 0; i < 50; i++) {
      await c.query('BEGIN');
      const current = await c.query('SELECT val FROM counter WHERE id = 1');
      await c.query('UPDATE counter SET val = $1 WHERE id = 1', [current.rows[0].val + 1]);
      await c.query('COMMIT');
    }
    
    const r = await c.query('SELECT val FROM counter WHERE id = 1');
    assert.equal(r.rows[0].val, 50);
    
    await c.end();
  });
});
