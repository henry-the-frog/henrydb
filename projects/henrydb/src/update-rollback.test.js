// update-rollback.test.js — Tests for UPDATE rollback correctness
// Verifies that ROLLBACK properly undoes UPDATE operations through all paths:
// TransactionalDatabase API, wire protocol, and concurrent scenarios.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';
// import { HenryDBServer } from './server.js'; // TODO: re-enable when PG wire protocol is implemented

const { Client } = pg;

function getPort() {
  return 22000 + Math.floor(Math.random() * 10000);
}

function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'henrydb-upd-rb-'));
  const db = TransactionalDatabase.open(dir);
  db.execute("CREATE TABLE accounts (id INT, name TEXT, balance INT)");
  db.execute("INSERT INTO accounts VALUES (1, 'Alice', 100)");
  db.execute("INSERT INTO accounts VALUES (2, 'Bob', 200)");
  return { db, dir };
}

describe('UPDATE Rollback', () => {

  describe('TransactionalDatabase API', () => {

    it('single UPDATE rollback restores original value', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("UPDATE accounts SET balance = 50 WHERE id = 1");
      
      const during = s.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(during.rows[0].balance, 50);
      
      s.rollback();
      
      const after = db.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(after.rows[0].balance, 100);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('multi-row UPDATE rollback restores all values', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("UPDATE accounts SET balance = balance + 1000");
      
      const during = s.execute("SELECT SUM(balance) as total FROM accounts");
      assert.equal(during.rows[0].total, 2300); // (100+1000) + (200+1000)
      
      s.rollback();
      
      const after = db.execute("SELECT SUM(balance) as total FROM accounts");
      assert.equal(after.rows[0].total, 300); // back to 100+200
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('UPDATE then COMMIT persists', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("UPDATE accounts SET balance = 150 WHERE id = 1");
      s.commit();
      
      const after = db.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(after.rows[0].balance, 150);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('mixed INSERT + UPDATE + DELETE rollback', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("INSERT INTO accounts VALUES (3, 'Charlie', 300)");
      s.execute("UPDATE accounts SET balance = 0 WHERE id = 1");
      s.execute("DELETE FROM accounts WHERE id = 2");
      s.rollback();
      
      const after = db.execute("SELECT * FROM accounts ORDER BY id");
      assert.equal(after.rows.length, 2);
      assert.equal(after.rows[0].balance, 100);
      assert.equal(after.rows[1].balance, 200);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('multiple sequential UPDATE rollbacks on fresh sessions', () => {
      const { db, dir } = freshDb();
      for (let i = 0; i < 5; i++) {
        const s = db.session();
        s.begin();
        s.execute(`UPDATE accounts SET balance = ${i * 100} WHERE id = 1`);
        s.rollback();
        s.close();
      }
      
      const after = db.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(after.rows[0].balance, 100);
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('UPDATE same row twice in one transaction then rollback', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("UPDATE accounts SET balance = 50 WHERE id = 1");
      s.execute("UPDATE accounts SET balance = 25 WHERE id = 1");
      
      const during = s.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(during.rows[0].balance, 25);
      
      s.rollback();
      
      const after = db.execute("SELECT balance FROM accounts WHERE id = 1");
      assert.equal(after.rows[0].balance, 100);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('UPDATE with WHERE matching zero rows then rollback is no-op', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      s.execute("UPDATE accounts SET balance = 999 WHERE id = 999");
      s.rollback();
      
      const after = db.execute("SELECT * FROM accounts ORDER BY id");
      assert.equal(after.rows.length, 2);
      assert.equal(after.rows[0].balance, 100);
      assert.equal(after.rows[1].balance, 200);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });
  });

  describe('Wire Protocol (server)', () => {
    let server, port, dir;
    
    before(async () => {
      port = getPort();
      dir = mkdtempSync(join(tmpdir(), 'henrydb-upd-rb-wire-'));
      server = new HenryDBServer({ port, dataDir: dir, transactional: true });
      await server.start();
      
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      await client.query("CREATE TABLE accounts (id INT, name TEXT, balance INT)");
      await client.query("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
      await client.query("INSERT INTO accounts VALUES (2, 'Bob', 2000)");
      await client.end();
    });
    
    after(async () => {
      if (server) await server.stop();
      if (dir) rmSync(dir, { recursive: true });
    });

    it('UPDATE rollback through wire protocol', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 500 WHERE id = 1');
      
      const during = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(during.rows[0].balance), '500');
      
      await client.query('ROLLBACK');
      
      const after = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(after.rows[0].balance), '1000');
      await client.end();
    });

    it('multi-row UPDATE rollback through wire protocol', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      const before = await client.query('SELECT SUM(balance) as total FROM accounts');
      const originalTotal = parseInt(String(before.rows[0].total));
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = balance + 10000');
      await client.query('ROLLBACK');
      
      const after = await client.query('SELECT SUM(balance) as total FROM accounts');
      assert.equal(parseInt(String(after.rows[0].total)), originalTotal);
      await client.end();
    });

    it('bank transfer rollback preserves balances', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      const before = await client.query('SELECT * FROM accounts ORDER BY id');
      const aliceBefore = parseInt(String(before.rows[0].balance));
      const bobBefore = parseInt(String(before.rows[1].balance));
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = balance - 500 WHERE id = 1');
      await client.query('UPDATE accounts SET balance = balance + 500 WHERE id = 2');
      await client.query('ROLLBACK');
      
      const after = await client.query('SELECT * FROM accounts ORDER BY id');
      assert.equal(parseInt(String(after.rows[0].balance)), aliceBefore);
      assert.equal(parseInt(String(after.rows[1].balance)), bobBefore);
      await client.end();
    });

    it('UPDATE commit then UPDATE rollback', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 1500 WHERE id = 1');
      await client.query('COMMIT');
      
      const mid = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(mid.rows[0].balance), '1500');
      
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 9999 WHERE id = 1');
      await client.query('ROLLBACK');
      
      const after = await client.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(after.rows[0].balance), '1500');
      
      // Restore for other tests
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 1000 WHERE id = 1');
      await client.query('COMMIT');
      await client.end();
    });

    it('concurrent session isolation during UPDATE rollback', async () => {
      const c1 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      const c2 = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await c1.connect();
      await c2.connect();
      
      await c1.query('BEGIN');
      await c1.query('UPDATE accounts SET balance = 0 WHERE id = 1');
      
      // c2 should still see old value
      const c2sees = await c2.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(c2sees.rows[0].balance), '1000');
      
      await c1.query('ROLLBACK');
      
      const c2after = await c2.query('SELECT balance FROM accounts WHERE id = 1');
      assert.equal(String(c2after.rows[0].balance), '1000');
      
      await c1.end();
      await c2.end();
    });

    it('query cache invalidated on ROLLBACK (bug fix)', async () => {
      // This test specifically validates the query cache bug fix:
      // Before the fix, cached SELECT results from during a transaction
      // would persist after ROLLBACK.
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      // Prime the cache
      await client.query('SELECT * FROM accounts WHERE id = 1');
      
      // UPDATE in transaction, causing cache to be populated with new value
      await client.query('BEGIN');
      await client.query('UPDATE accounts SET balance = 7777 WHERE id = 1');
      await client.query('SELECT * FROM accounts WHERE id = 1'); // may cache balance=7777
      await client.query('ROLLBACK');
      
      // After rollback, must see original value (not cached 7777)
      const after = await client.query('SELECT * FROM accounts WHERE id = 1');
      assert.equal(String(after.rows[0].balance), '1000');
      await client.end();
    });
  });
});
