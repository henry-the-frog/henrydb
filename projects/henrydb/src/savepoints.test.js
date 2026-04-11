// savepoints.test.js — Tests for SAVEPOINT, ROLLBACK TO SAVEPOINT, RELEASE SAVEPOINT
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 24000 + Math.floor(Math.random() * 10000);
}

function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'henrydb-sp-'));
  const db = TransactionalDatabase.open(dir);
  db.execute("CREATE TABLE t (id INT, val INT)");
  db.execute("INSERT INTO t VALUES (1, 10)");
  db.execute("INSERT INTO t VALUES (2, 20)");
  return { db, dir };
}

describe('Savepoints', () => {

  describe('TransactionalDatabase API', () => {

    it('basic SAVEPOINT and ROLLBACK TO', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("UPDATE t SET val = 100 WHERE id = 1");
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 200 WHERE id = 1");
      
      const during = s.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(during.rows[0].val, 200);
      
      s.execute("ROLLBACK TO SAVEPOINT sp1");
      
      const afterRollbackTo = s.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(afterRollbackTo.rows[0].val, 100, 'Should be at savepoint value, not original');
      
      s.commit();
      
      const afterCommit = db.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(afterCommit.rows[0].val, 100, 'Committed value should be savepoint value');
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('ROLLBACK TO preserves work before savepoint', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("UPDATE t SET val = 50 WHERE id = 2"); // Before savepoint
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 999 WHERE id = 2"); // After savepoint
      s.execute("ROLLBACK TO SAVEPOINT sp1");
      
      const result = s.execute("SELECT val FROM t WHERE id = 2");
      assert.equal(result.rows[0].val, 50, 'Pre-savepoint work should survive');
      
      s.commit();
      const final = db.execute("SELECT val FROM t WHERE id = 2");
      assert.equal(final.rows[0].val, 50);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('nested savepoints', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 100 WHERE id = 1");
      
      s.execute("SAVEPOINT sp2");
      s.execute("UPDATE t SET val = 200 WHERE id = 1");
      
      s.execute("SAVEPOINT sp3");
      s.execute("INSERT INTO t VALUES (3, 300)");
      
      // Rollback sp3 (removes INSERT)
      s.execute("ROLLBACK TO SAVEPOINT sp3");
      const r1 = s.execute("SELECT * FROM t WHERE id = 3");
      assert.equal(r1.rows.length, 0, 'INSERT after sp3 should be undone');
      
      // Rollback sp2 (undoes val=200)
      s.execute("ROLLBACK TO SAVEPOINT sp2");
      const r2 = s.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(r2.rows[0].val, 100, 'Should be at sp2 value');
      
      // Rollback sp1 (undoes val=100)
      s.execute("ROLLBACK TO SAVEPOINT sp1");
      const r3 = s.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(r3.rows[0].val, 10, 'Should be at original value');
      
      s.commit();
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('RELEASE SAVEPOINT', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 100 WHERE id = 1");
      s.execute("RELEASE SAVEPOINT sp1");
      
      // Can't rollback to released savepoint
      assert.throws(() => {
        s.execute("ROLLBACK TO SAVEPOINT sp1");
      }, /does not exist/);
      
      s.commit();
      const final = db.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(final.rows[0].val, 100);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('ROLLBACK TO then continue working', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 999 WHERE id = 1");
      s.execute("ROLLBACK TO SAVEPOINT sp1");
      
      // Continue with new work after rollback
      s.execute("UPDATE t SET val = 42 WHERE id = 1");
      s.commit();
      
      const final = db.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(final.rows[0].val, 42);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('full transaction rollback undoes everything including savepoint work', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("SAVEPOINT sp1");
      s.execute("UPDATE t SET val = 100 WHERE id = 1");
      s.execute("RELEASE SAVEPOINT sp1"); // Released but still in tx
      
      s.rollback(); // Full rollback
      
      const final = db.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(final.rows[0].val, 10, 'Full rollback undoes even released savepoint work');
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });

    it('savepoint with INSERT then rollback', () => {
      const { db, dir } = freshDb();
      const s = db.session();
      s.begin();
      
      s.execute("INSERT INTO t VALUES (3, 30)");
      s.execute("SAVEPOINT sp1");
      s.execute("INSERT INTO t VALUES (4, 40)");
      s.execute("INSERT INTO t VALUES (5, 50)");
      
      s.execute("ROLLBACK TO SAVEPOINT sp1");
      
      const result = s.execute("SELECT * FROM t ORDER BY id");
      assert.equal(result.rows.length, 3, 'Should have 3 rows (2 original + 1 before savepoint)');
      
      s.commit();
      const final = db.execute("SELECT * FROM t ORDER BY id");
      assert.equal(final.rows.length, 3);
      assert.equal(final.rows[2].id, 3);
      s.close();
      db.close();
      rmSync(dir, { recursive: true });
    });
  });

  describe('Wire Protocol', () => {
    let server, port, dir;
    
    before(async () => {
      port = getPort();
      dir = mkdtempSync(join(tmpdir(), 'henrydb-sp-wire-'));
      server = new HenryDBServer({ port, dataDir: dir, transactional: true });
      await server.start();
      
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      await client.query("CREATE TABLE t (id INT, val INT)");
      await client.query("INSERT INTO t VALUES (1, 10)");
      await client.query("INSERT INTO t VALUES (2, 20)");
      await client.end();
    });
    
    after(async () => {
      if (server) await server.stop();
      if (dir) rmSync(dir, { recursive: true });
    });

    it('SAVEPOINT + ROLLBACK TO through wire protocol', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query('BEGIN');
      await client.query('UPDATE t SET val = 100 WHERE id = 1');
      await client.query('SAVEPOINT sp1');
      await client.query('UPDATE t SET val = 200 WHERE id = 1');
      await client.query('ROLLBACK TO SAVEPOINT sp1');
      await client.query('COMMIT');
      
      const result = await client.query('SELECT val FROM t WHERE id = 1');
      assert.equal(String(result.rows[0].val), '100');
      
      // Restore
      await client.query('BEGIN');
      await client.query('UPDATE t SET val = 10 WHERE id = 1');
      await client.query('COMMIT');
      await client.end();
    });

    it('nested savepoints through wire protocol', async () => {
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query('BEGIN');
      await client.query('SAVEPOINT a');
      await client.query('UPDATE t SET val = 100 WHERE id = 1');
      await client.query('SAVEPOINT b');
      await client.query('UPDATE t SET val = 200 WHERE id = 1');
      
      // Rollback to b
      await client.query('ROLLBACK TO SAVEPOINT b');
      const r1 = await client.query('SELECT val FROM t WHERE id = 1');
      assert.equal(String(r1.rows[0].val), '100');
      
      // Rollback to a
      await client.query('ROLLBACK TO SAVEPOINT a');
      const r2 = await client.query('SELECT val FROM t WHERE id = 1');
      assert.equal(String(r2.rows[0].val), '10');
      
      await client.query('COMMIT');
      await client.end();
    });

    it('error recovery with savepoints', async () => {
      // PostgreSQL pattern: use savepoints for error recovery
      const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
      await client.connect();
      
      await client.query('BEGIN');
      await client.query('UPDATE t SET val = 50 WHERE id = 1');
      await client.query('SAVEPOINT before_risky');
      
      // Simulate a risky operation that might fail
      try {
        await client.query('INSERT INTO nonexistent_table VALUES (1)');
      } catch (e) {
        // Expected error — rollback to savepoint
        await client.query('ROLLBACK TO SAVEPOINT before_risky');
      }
      
      // Continue with the transaction — the UPDATE should still be there
      await client.query('COMMIT');
      
      const result = await client.query('SELECT val FROM t WHERE id = 1');
      assert.equal(String(result.rows[0].val), '50');
      
      // Restore
      await client.query('BEGIN');
      await client.query('UPDATE t SET val = 10 WHERE id = 1');
      await client.query('COMMIT');
      await client.end();
    });
  });
});
