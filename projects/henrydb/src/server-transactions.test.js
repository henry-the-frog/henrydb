// server-transactions.test.js — Tests for BEGIN/COMMIT/ROLLBACK through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 21000 + Math.floor(Math.random() * 10000);
}

async function connect(port) {
  const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
  await client.connect();
  return client;
}

describe('Server Transaction Support', () => {

  describe('Basic Transactions (non-transactional mode)', () => {
    let server, port;
    
    before(async () => {
      port = getPort();
      server = new HenryDBServer({ port });
      await server.start();
      const client = await connect(port);
      await client.query('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
      await client.query('INSERT INTO accounts VALUES (1, 1000)');
      await client.query('INSERT INTO accounts VALUES (2, 2000)');
      await client.end();
    });
    
    after(async () => {
      if (server) await server.stop();
    });

    it('BEGIN/COMMIT syntax works', async () => {
      const client = await connect(port);
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 1500 WHERE id = 1');
      await client.query('COMMIT');
      
      const r = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(r.rows[0].balance), '1500');
      
      // Restore
      await client.query('UPDATE accounts SET balance = 1000 WHERE id = 1');
      await client.end();
    });

    it('ROLLBACK syntax works', async () => {
      const client = await connect(port);
      const before = await client.query('SELECT balance FROM accounts WHERE id = 1');
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 9999 WHERE id = 1');
      await client.query('ROLLBACK');
      
      // In non-transactional mode, ROLLBACK is "best effort" — 
      // the UPDATE may already be applied. We just test that the syntax works.
      await client.end();
    });
  });

  describe('Full Transactional Mode', () => {
    let server, port, dir;
    
    before(async () => {
      port = getPort();
      dir = mkdtempSync(join(tmpdir(), 'henrydb-tx-server-'));
      server = new HenryDBServer({ port, dataDir: dir, transactional: true });
      await server.start();
      
      const client = await connect(port);
      await client.query('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
      await client.query('INSERT INTO accounts VALUES (1, 1000)');
      await client.query('INSERT INTO accounts VALUES (2, 2000)');
      await client.end();
    });
    
    after(async () => {
      if (server) await server.stop();
      if (dir) rmSync(dir, { recursive: true });
    });

    it('committed transaction is visible', async () => {
      const client = await connect(port);
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 1500 WHERE id = 1');
      await client.query('COMMIT');
      
      const r = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(r.rows[0].balance), '1500');
      await client.end();
    });

    it('rolled back INSERT is not visible', async () => {
      const client = await connect(port);
      
      await client.query('BEGIN');
      await client.query('INSERT INTO accounts VALUES (99, 9999)');
      await client.query('ROLLBACK');
      
      const after = await client.query('SELECT * FROM accounts WHERE id = 99');
      assert.equal(after.rows.length, 0, 'Rolled back INSERT should not be visible');
      await client.end();
    });

    it('multi-statement transaction atomicity', async () => {
      const client = await connect(port);
      
      // Get initial total
      const before = await client.query('SELECT SUM(balance) as total FROM accounts');
      const initialTotal = parseInt(String(before.rows[0].total));
      
      // Transfer 500 from account 1 to account 2 (conserves total)
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = balance - 500 WHERE id = 1');
      await client.query('UPDATE accounts SET balance = balance + 500 WHERE id = 2');
      await client.query('COMMIT');
      
      const after = await client.query('SELECT SUM(balance) as total FROM accounts');
      const finalTotal = parseInt(String(after.rows[0].total));
      
      // Total should be preserved
      assert.equal(finalTotal, initialTotal, `Total should be ${initialTotal}, got ${finalTotal}`);
      await client.end();
    });

    it('autocommit works without BEGIN', async () => {
      const client = await connect(port);
      
      // Without BEGIN, each statement auto-commits
      await client.query('INSERT INTO accounts VALUES (3, 3000)');
      
      const r = await client.query('SELECT balance FROM accounts WHERE id = 3');
      assert.equal(String(r.rows[0].balance), '3000');
      
      await client.query('DELETE FROM accounts WHERE id = 3');
      await client.end();
    });

    it('handles INSERT and SELECT within transaction', async () => {
      const client = await connect(port);
      
      await client.query('BEGIN');
      await client.query('INSERT INTO accounts VALUES (10, 5000)');
      const r = await client.query('SELECT balance FROM accounts WHERE id = 10');
      assert.equal(String(r.rows[0].balance), '5000');
      await client.query('ROLLBACK');
      
      // After rollback, row should not exist
      const r2 = await client.query('SELECT * FROM accounts WHERE id = 10');
      assert.equal(r2.rows.length, 0, 'Rolled back INSERT should not be visible');
      await client.end();
    });
  });
});
