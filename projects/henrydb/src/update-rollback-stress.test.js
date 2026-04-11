// update-rollback-stress.test.js — Stress tests for UPDATE rollback
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';
import { TransactionalDatabase } from './transactional-db.js';

const { Client } = pg;

function getPort() {
  return 23000 + Math.floor(Math.random() * 10000);
}

describe('UPDATE Rollback Stress Tests', () => {

  describe('TransactionalDatabase stress', () => {
    
    it('10 sequential UPDATE/ROLLBACK cycles on same row', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
      const db = TransactionalDatabase.open(dir);
      db.execute("CREATE TABLE counter (id INT, val INT)");
      db.execute("INSERT INTO counter VALUES (1, 0)");
      
      for (let i = 0; i < 10; i++) {
        const s = db.session();
        s.begin();
        s.execute(`UPDATE counter SET val = ${(i + 1) * 100} WHERE id = 1`);
        
        const during = s.execute("SELECT val FROM counter WHERE id = 1");
        assert.equal(during.rows[0].val, (i + 1) * 100);
        
        s.rollback();
        s.close();
      }
      
      const after = db.execute("SELECT val FROM counter WHERE id = 1");
      assert.equal(after.rows[0].val, 0, 'Value should still be 0 after 10 rollbacks');
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('alternating COMMIT and ROLLBACK', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
      const db = TransactionalDatabase.open(dir);
      db.execute("CREATE TABLE tracker (id INT, balance INT)");
      db.execute("INSERT INTO tracker VALUES (1, 1000)");
      
      let expected = 1000;
      for (let i = 0; i < 8; i++) {
        const s = db.session();
        s.begin();
        s.execute("UPDATE tracker SET balance = balance - 100 WHERE id = 1");
        
        if (i % 2 === 0) {
          s.commit(); // even: commit -100
          expected -= 100;
        } else {
          s.rollback(); // odd: rollback
        }
        s.close();
      }
      
      const after = db.execute("SELECT balance FROM tracker WHERE id = 1");
      // 4 commits of -100 = 600
      assert.equal(after.rows[0].balance, expected);
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('large batch UPDATE rollback (100 rows)', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
      const db = TransactionalDatabase.open(dir);
      db.execute("CREATE TABLE batch (id INT, val INT)");
      
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO batch VALUES (${i}, ${i})`);
      }
      
      const s = db.session();
      s.begin();
      s.execute("UPDATE batch SET val = val * 2");
      
      // Verify all doubled
      const during = s.execute("SELECT SUM(val) as total FROM batch");
      assert.equal(during.rows[0].total, 99 * 100); // 2 * sum(0..99) = 2 * 4950 = 9900
      
      s.rollback();
      
      const after = db.execute("SELECT SUM(val) as total FROM batch");
      assert.equal(after.rows[0].total, 4950); // sum(0..99) = 4950
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('UPDATE with computed expressions then rollback', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
      const db = TransactionalDatabase.open(dir);
      db.execute("CREATE TABLE compute (id INT, a INT, b INT)");
      db.execute("INSERT INTO compute VALUES (1, 10, 20)");
      db.execute("INSERT INTO compute VALUES (2, 30, 40)");
      
      const s = db.session();
      s.begin();
      // Swap a and b values
      s.execute("UPDATE compute SET a = b, b = a");
      
      const during = s.execute("SELECT * FROM compute ORDER BY id");
      assert.equal(during.rows[0].a, 20);
      assert.equal(during.rows[0].b, 10);
      
      s.rollback();
      
      const after = db.execute("SELECT * FROM compute ORDER BY id");
      assert.equal(after.rows[0].a, 10);
      assert.equal(after.rows[0].b, 20);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });
  });

  describe('Wire protocol stress', () => {
    let server, port, dir;
    
    before(async () => {
      port = getPort();
      dir = mkdtempSync(join(tmpdir(), 'henrydb-wire-stress-'));
      server = new HenryDBServer({ port, dataDir: dir, transactional: true });
      await server.start();
    });
    
    after(async () => {
      if (server) await server.stop();
      if (dir) rmSync(dir, { recursive: true });
    });

    it('rapid UPDATE/ROLLBACK through wire protocol', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query("CREATE TABLE rapid (id INT, cnt INT)");
      await client.query("INSERT INTO rapid VALUES (1, 0)");
      
      for (let i = 0; i < 5; i++) {
        await client.query('BEGIN');
        await client.query(`UPDATE rapid SET cnt = ${i + 1} WHERE id = 1`);
        await client.query('ROLLBACK');
      }
      
      const after = await client.query('SELECT cnt FROM rapid WHERE id = 1');
      assert.equal(String(after.rows[0].cnt), '0');
      await client.end();
    });

    it('concurrent connections: one updates+rolls back, other reads', async () => {
      const writer = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      const reader = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await writer.connect();
      await reader.connect();
      
      await writer.query("CREATE TABLE iso_test (id INT, val TEXT)");
      await writer.query("INSERT INTO iso_test VALUES (1, 'original')");
      
      // Writer starts transaction
      await writer.query('BEGIN');
      await writer.query("UPDATE iso_test SET val = 'modified' WHERE id = 1");
      
      // Reader should see original
      const readerSees = await reader.query("SELECT val FROM iso_test WHERE id = 1");
      assert.equal(readerSees.rows[0].val, 'original');
      
      // Writer rolls back
      await writer.query('ROLLBACK');
      
      // Reader still sees original
      const readerAfter = await reader.query("SELECT val FROM iso_test WHERE id = 1");
      assert.equal(readerAfter.rows[0].val, 'original');
      
      await writer.end();
      await reader.end();
    });

    it('UPDATE all rows then rollback preserves everything', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query("CREATE TABLE bulk (id INT, val INT)");
      for (let i = 1; i <= 20; i++) {
        await client.query(`INSERT INTO bulk VALUES (${i}, ${i * 10})`);
      }
      
      const beforeSum = await client.query('SELECT SUM(val) as s FROM bulk');
      
      await client.query('BEGIN');
      await client.query('UPDATE bulk SET val = 0');
      
      const duringSum = await client.query('SELECT SUM(val) as s FROM bulk');
      assert.equal(String(duringSum.rows[0].s), '0');
      
      await client.query('ROLLBACK');
      
      const afterSum = await client.query('SELECT SUM(val) as s FROM bulk');
      assert.equal(String(afterSum.rows[0].s), String(beforeSum.rows[0].s));
      
      await client.end();
    });

    it('bank transfer simulation with error → rollback', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query("CREATE TABLE bank (acct INT, balance INT)");
      await client.query("INSERT INTO bank VALUES (1, 500)");
      await client.query("INSERT INTO bank VALUES (2, 500)");
      
      // Simulate: debit succeeds, then "error" before credit
      await client.query('BEGIN');
      await client.query('UPDATE bank SET balance = balance - 300 WHERE acct = 1');
      // Simulate error: rollback instead of completing the transfer
      await client.query('ROLLBACK');
      
      // Both accounts should be intact
      const r = await client.query('SELECT * FROM bank ORDER BY acct');
      assert.equal(String(r.rows[0].balance), '500');
      assert.equal(String(r.rows[1].balance), '500');
      
      await client.end();
    });
  });
});
