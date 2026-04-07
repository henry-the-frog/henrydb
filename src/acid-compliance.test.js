// acid-compliance.test.js — ACID compliance test suite for TransactionalDatabase
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function tmpDir() {
  return join(tmpdir(), `henrydb-acid-${Date.now()}-${Math.random().toString(36).slice(2)}`);
}

describe('ACID Compliance', () => {
  let dir, db;

  beforeEach(() => { dir = tmpDir(); mkdirSync(dir, { recursive: true }); });
  afterEach(() => {
    try { if (db) db.close(); } catch (e) {}
    try { rmSync(dir, { recursive: true, force: true }); } catch (e) {}
  });

  // ===== ATOMICITY =====
  describe('Atomicity', () => {
    it('rollback undoes all operations in a transaction', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE accounts (id INT, balance INT)');
      db.execute('INSERT INTO accounts VALUES (1, 100)');
      db.execute('INSERT INTO accounts VALUES (2, 200)');

      const s = db.session();
      s.begin();
      s.execute('INSERT INTO accounts VALUES (3, 300)');
      s.execute('INSERT INTO accounts VALUES (4, 400)');
      s.rollback();

      const result = db.execute('SELECT * FROM accounts ORDER BY id');
      assert.equal(result.rows.length, 2); // Only original 2 rows
    });

    it('partial failure rolls back entire transaction', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');

      const s = db.session();
      s.begin();
      s.execute('INSERT INTO t VALUES (1)');
      s.execute('INSERT INTO t VALUES (2)');
      // Simulate failure by rolling back
      s.rollback();

      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows.length, 0);
    });

    it('crash simulation: uncommitted data not visible after reopen', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      db.execute('INSERT INTO t VALUES (1)'); // This is committed

      const s = db.session();
      s.begin();
      s.execute('INSERT INTO t VALUES (2)'); // Not committed
      // "Crash" — close without committing
      db.close();

      // Reopen
      db = TransactionalDatabase.open(dir);
      const result = db.execute('SELECT * FROM t ORDER BY x');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].x, 1);
    });
  });

  // ===== CONSISTENCY =====
  describe('Consistency', () => {
    it('constraint violation does not corrupt state', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE users (id INT, name TEXT)');
      db.execute("INSERT INTO users VALUES (1, 'Alice')");

      // Try to insert into non-existent table
      const s = db.session();
      s.begin();
      s.execute("INSERT INTO users VALUES (2, 'Bob')");
      try {
        s.execute("INSERT INTO nonexistent VALUES (3, 'Charlie')");
      } catch (e) {
        // Expected error
      }
      s.rollback();

      // State should be clean — only Alice
      const result = db.execute('SELECT * FROM users');
      assert.equal(result.rows.length, 1);
    });

    it('database state is consistent after multiple committed transactions', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE counter (id INT, val INT)');
      db.execute('INSERT INTO counter VALUES (1, 0)');

      // Run 10 increments
      for (let i = 0; i < 10; i++) {
        const s = db.session();
        s.begin();
        const r = s.execute('SELECT val FROM counter WHERE id = 1');
        const newVal = r.rows[0].val + 1;
        s.execute(`UPDATE counter SET val = ${newVal} WHERE id = 1`);
        s.commit();
      }

      const result = db.execute('SELECT val FROM counter WHERE id = 1');
      assert.equal(result.rows[0].val, 10);
    });
  });

  // ===== ISOLATION =====
  describe('Isolation', () => {
    it('dirty read prevention: uncommitted writes invisible', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      db.execute('INSERT INTO t VALUES (1)');

      const writer = db.session();
      const reader = db.session();

      writer.begin();
      writer.execute('INSERT INTO t VALUES (2)');

      reader.begin();
      const result = reader.execute('SELECT * FROM t');
      assert.equal(result.rows.length, 1); // Only sees committed row

      writer.commit();
      reader.commit();
    });

    it('repeatable read: snapshot does not change during transaction', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      db.execute('INSERT INTO t VALUES (1)');

      const reader = db.session();
      reader.begin();
      const r1 = reader.execute('SELECT * FROM t');
      assert.equal(r1.rows.length, 1);

      // Another transaction inserts and commits
      db.execute('INSERT INTO t VALUES (2)');

      // Reader still sees only 1 row (snapshot taken at BEGIN)
      const r2 = reader.execute('SELECT * FROM t');
      assert.equal(r2.rows.length, 1);

      reader.commit();
    });

    it('write skew detection (snapshot isolation allows write skew)', () => {
      // Write skew is allowed under snapshot isolation (not serializable)
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE doctors (name TEXT, oncall INT)');
      db.execute("INSERT INTO doctors VALUES ('Alice', 1)");
      db.execute("INSERT INTO doctors VALUES ('Bob', 1)");

      const s1 = db.session();
      const s2 = db.session();

      s1.begin();
      s2.begin();

      // Both read: 2 doctors on call
      const r1 = s1.execute('SELECT COUNT(*) as cnt FROM doctors WHERE oncall = 1');
      const r2 = s2.execute('SELECT COUNT(*) as cnt FROM doctors WHERE oncall = 1');
      assert.equal(r1.rows[0].cnt, 2);
      assert.equal(r2.rows[0].cnt, 2);

      // Each takes themselves off call (different rows — no write-write conflict)
      s1.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Alice'");
      s2.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Bob'");

      // Both commit (write skew — now 0 doctors on call!)
      s1.commit();
      s2.commit();

      const final = db.execute('SELECT COUNT(*) as cnt FROM doctors WHERE oncall = 1');
      assert.equal(final.rows[0].cnt, 0); // Write skew occurred
    });

    it('phantom prevention: new rows from concurrent tx invisible to snapshot', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('INSERT INTO t VALUES (2)');
      db.execute('INSERT INTO t VALUES (3)');

      const reader = db.session();
      reader.begin();
      const r1 = reader.execute('SELECT * FROM t WHERE x > 0');
      assert.equal(r1.rows.length, 3);

      // Insert new row in another transaction
      db.execute('INSERT INTO t VALUES (4)');

      // Reader doesn't see phantom
      const r2 = reader.execute('SELECT * FROM t WHERE x > 0');
      assert.equal(r2.rows.length, 3);

      reader.commit();
    });
  });

  // ===== DURABILITY =====
  describe('Durability', () => {
    it('committed data survives close and reopen', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');

      const s = db.session();
      s.begin();
      s.execute('INSERT INTO t VALUES (42)');
      s.commit();
      db.close();

      db = TransactionalDatabase.open(dir);
      const result = db.execute('SELECT x FROM t');
      assert.equal(result.rows[0].x, 42);
    });

    it('multiple committed transactions survive restart', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE txlog (seq INT, msg TEXT)');

      for (let i = 0; i < 5; i++) {
        db.execute(`INSERT INTO txlog VALUES (${i}, 'message ${i}')`);
      }
      db.close();

      db = TransactionalDatabase.open(dir);
      const result = db.execute('SELECT * FROM txlog ORDER BY seq');
      assert.equal(result.rows.length, 5);
      assert.equal(result.rows[4].msg, 'message 4');
    });

    it('WAL ensures durability even without explicit flush', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('INSERT INTO t VALUES (2)');
      db.execute('INSERT INTO t VALUES (3)');
      // Close triggers flush
      db.close();

      db = TransactionalDatabase.open(dir);
      const result = db.execute('SELECT COUNT(*) as cnt FROM t');
      assert.equal(result.rows[0].cnt, 3);
    });
  });

  // ===== STRESS TEST =====
  describe('Stress Tests', () => {
    it('100 sequential transactions with commit', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE counter (id INT, val INT)');
      db.execute('INSERT INTO counter VALUES (1, 0)');

      for (let i = 0; i < 100; i++) {
        const s = db.session();
        s.begin();
        s.execute(`UPDATE counter SET val = ${i + 1} WHERE id = 1`);
        s.commit();
      }

      const result = db.execute('SELECT val FROM counter WHERE id = 1');
      assert.equal(result.rows[0].val, 100);
    });

    it('50 transactions with interleaved commit/rollback', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');

      for (let i = 0; i < 50; i++) {
        const s = db.session();
        s.begin();
        s.execute(`INSERT INTO t VALUES (${i})`);
        if (i % 3 === 0) {
          s.rollback(); // Every 3rd transaction rolls back
        } else {
          s.commit();
        }
      }

      const result = db.execute('SELECT COUNT(*) as cnt FROM t');
      // 50 total, every 3rd (0, 3, 6, 9, ..., 48) rolls back = 17 rollbacks
      const expected = 50 - 17;
      assert.equal(result.rows[0].cnt, expected);
    });

    it('concurrent readers during writes', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE t (x INT)');
      for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);

      // Start 5 reader sessions
      const readers = [];
      for (let i = 0; i < 5; i++) {
        const r = db.session();
        r.begin();
        readers.push(r);
      }

      // Write 10 more rows
      for (let i = 10; i < 20; i++) db.execute(`INSERT INTO t VALUES (${i})`);

      // All readers still see only 10 rows (snapshot isolation)
      for (const r of readers) {
        const result = r.execute('SELECT COUNT(*) as cnt FROM t');
        assert.equal(result.rows[0].cnt, 10);
        r.commit();
      }

      // New query sees all 20
      const final = db.execute('SELECT COUNT(*) as cnt FROM t');
      assert.equal(final.rows[0].cnt, 20);
    });

    it('100 transactions survive close/reopen', () => {
      db = TransactionalDatabase.open(dir);
      db.execute('CREATE TABLE txlog (seq INT)');

      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO txlog VALUES (${i})`);
      }
      db.close();

      db = TransactionalDatabase.open(dir);
      const result = db.execute('SELECT COUNT(*) as cnt FROM txlog');
      assert.equal(result.rows[0].cnt, 100);
    });
  });
});
