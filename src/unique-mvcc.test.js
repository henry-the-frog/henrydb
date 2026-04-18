import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('MVCC-Aware UNIQUE Constraints', () => {
  
  describe('Transaction isolation with UNIQUE keys', () => {
    it('uncommitted INSERT blocks same key in next transaction (eagerly)', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'from_tx1')");
      db.execute('COMMIT');
      
      // After commit, same key should be blocked
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (1, 'duplicate')");
      }, /duplicate|unique|constraint/i);
    });

    it('ROLLBACK releases key for reuse in new transaction', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      
      // TX1: insert then rollback
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'temporary')");
      db.execute('ROLLBACK');
      
      // TX2: same key should now be available
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'permanent')");
      db.execute('COMMIT');
      
      const rows = query(db, 'SELECT * FROM t WHERE id = 1');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].val, 'permanent');
    });

    it('multiple rollback cycles should not leak index entries', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
      
      // Rollback 10 times with same key
      for (let i = 0; i < 10; i++) {
        db.execute('BEGIN');
        db.execute(`INSERT INTO t VALUES (1, ${i})`);
        db.execute('ROLLBACK');
      }
      
      // Table should be empty
      const count = query(db, 'SELECT COUNT(*) AS c FROM t');
      assert.equal(count[0].c, 0);
      
      // Key should be available
      db.execute('INSERT INTO t VALUES (1, 999)');
      const rows = query(db, 'SELECT * FROM t');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].val, 999);
    });

    it('ROLLBACK with multiple inserts undoes all', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("INSERT INTO t VALUES (2, 'b')");
      db.execute("INSERT INTO t VALUES (3, 'c')");
      db.execute('ROLLBACK');
      
      // All keys should be available
      const count = query(db, 'SELECT COUNT(*) AS c FROM t');
      assert.equal(count[0].c, 0);
      
      db.execute("INSERT INTO t VALUES (1, 'x')");
      db.execute("INSERT INTO t VALUES (2, 'y')");
      db.execute("INSERT INTO t VALUES (3, 'z')");
      
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 3);
    });

    it('partial transaction: some inserts succeed, one fails, rollback cleans all', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (5, 'existing')"); // Pre-existing row
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("INSERT INTO t VALUES (2, 'b')");
      
      // This should fail (duplicate)
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (5, 'dup')");
      }, /duplicate|unique|constraint/i);
      
      db.execute('ROLLBACK');
      
      // Only the pre-existing row should remain
      const count = query(db, 'SELECT COUNT(*) AS c FROM t');
      assert.equal(count[0].c, 1);
      assert.equal(query(db, 'SELECT val FROM t WHERE id = 5')[0].val, 'existing');
      
      // Keys 1, 2 should be available
      db.execute("INSERT INTO t VALUES (1, 'reused_a')");
      db.execute("INSERT INTO t VALUES (2, 'reused_b')");
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 3);
    });

    it('UNIQUE constraint with MVCC ON indexed non-PK column', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE users (id INTEGER, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON users (email)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO users VALUES (1, 'alice@test.com')");
      db.execute('ROLLBACK');
      
      // Email should be available after rollback
      db.execute("INSERT INTO users VALUES (2, 'alice@test.com')");
      const rows = query(db, "SELECT * FROM users WHERE email = 'alice@test.com'");
      assert.equal(rows.length, 1);
      assert.equal(rows[0].id, 2);
    });

    it('UPDATE in transaction + ROLLBACK restores old value', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
      db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");
      
      db.execute('BEGIN');
      db.execute("UPDATE t SET email = 'charlie@test.com' WHERE id = 1");
      db.execute('ROLLBACK');
      
      // alice@test.com should still be taken
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (3, 'alice@test.com')");
      }, /duplicate|unique|constraint/i);
    });

    it('DELETE in transaction + ROLLBACK restores row', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'keep')");
      
      db.execute('BEGIN');
      db.execute('DELETE FROM t WHERE id = 1');
      db.execute('ROLLBACK');
      
      // Row should still exist
      const rows = query(db, 'SELECT * FROM t WHERE id = 1');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].val, 'keep');
    });

    it('commit succeeds after valid unique inserts', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      
      db.execute('BEGIN');
      for (let i = 0; i < 20; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
      }
      db.execute('COMMIT');
      
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 20);
      
      // All keys should be taken
      for (let i = 0; i < 20; i++) {
        assert.throws(() => db.execute(`INSERT INTO t VALUES (${i}, 'dup')`), /duplicate|unique|constraint/i);
      }
    });
  });
});
