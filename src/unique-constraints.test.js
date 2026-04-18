import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { MVCCManager } from './mvcc.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('UNIQUE Constraint Enforcement', () => {

  describe('Basic UNIQUE (non-MVCC)', () => {
    it('should reject duplicate INSERT on PRIMARY KEY', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (1, 'Bob')");
      }, /duplicate|unique|constraint/i);
    });

    it('should reject duplicate INSERT on UNIQUE index', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
      
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
      }, /duplicate|unique|constraint/i);
    });

    it('should allow same value after DELETE', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('DELETE FROM t WHERE id = 1');
      db.execute("INSERT INTO t VALUES (1, 'Bob')");
      
      const rows = query(db, 'SELECT * FROM t');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, 'Bob');
    });

    it('should reject duplicate on UPDATE', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
      db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");
      
      assert.throws(() => {
        db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 2");
      }, /duplicate|unique|constraint/i);
    });

    it('should allow NULL in UNIQUE index (NULLs are distinct)', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute('INSERT INTO t VALUES (1, null)');
      db.execute('INSERT INTO t VALUES (2, null)');
      
      const rows = query(db, 'SELECT * FROM t');
      assert.equal(rows.length, 2);
    });

    it('should enforce UNIQUE across multiple inserts in sequence', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      
      for (let i = 0; i < 50; i++) {
        db.execute(`INSERT INTO t VALUES (${i})`);
      }
      
      // All 50 should succeed
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 50);
      
      // Inserting any duplicate should fail
      assert.throws(() => db.execute('INSERT INTO t VALUES (25)'), /duplicate|unique|constraint/i);
    });

    it('should enforce UNIQUE on CREATE INDEX (reject if data has dupes)', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'same')");
      db.execute("INSERT INTO t VALUES (2, 'same')");
      
      assert.throws(() => {
        db.execute('CREATE UNIQUE INDEX idx_val ON t (val)');
      }, /duplicate|unique|constraint/i);
    });
  });

  describe('UNIQUE with MVCC transactions', () => {
    it('should enforce UNIQUE within a transaction', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (1, 'Bob')");
      }, /duplicate|unique|constraint/i);
      
      db.execute('ROLLBACK');
    });

    it('should enforce UNIQUE after COMMIT', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('COMMIT');
      
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (1, 'Bob')");
      }, /duplicate|unique|constraint/i);
    });

    it('should allow same key after rollback', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('ROLLBACK');
      
      // After rollback, key should be available
      db.execute("INSERT INTO t VALUES (1, 'Bob')");
      const rows = query(db, 'SELECT * FROM t');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, 'Bob');
    });

    it('should maintain count correctness with UNIQUE violations', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
      
      // Insert 10 rows
      for (let i = 0; i < 10; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      }
      
      // Try 10 duplicate inserts — all should fail
      let failures = 0;
      for (let i = 0; i < 10; i++) {
        try {
          db.execute(`INSERT INTO t VALUES (${i}, 999)`);
        } catch (e) {
          failures++;
        }
      }
      
      assert.equal(failures, 10);
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 10);
    });
  });

  describe('UNIQUE edge cases', () => {
    it('UPDATE to same value should not violate UNIQUE', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
      
      // Updating to the same value should be fine
      db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 1");
      
      const rows = query(db, 'SELECT * FROM t');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].email, 'alice@test.com');
    });

    it('should enforce UNIQUE after VACUUM', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      
      for (let i = 0; i < 10; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
      }
      
      // Delete some rows
      for (let i = 0; i < 5; i++) {
        db.execute(`DELETE FROM t WHERE id = ${i}`);
      }
      
      db.execute('VACUUM');
      
      // Re-insert deleted keys — should work
      for (let i = 0; i < 5; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, 'new_v${i}')`);
      }
      
      // Duplicate on remaining key should fail
      assert.throws(() => db.execute("INSERT INTO t VALUES (5, 'dup')"), /duplicate|unique|constraint/i);
      
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 10);
    });

    it('should enforce UNIQUE after HOT updates', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, indexed_val TEXT, data TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_val ON t (indexed_val)');
      db.execute("INSERT INTO t VALUES (1, 'unique1', 'initial')");
      db.execute("INSERT INTO t VALUES (2, 'unique2', 'initial')");
      
      // HOT update (non-indexed column)
      db.execute("UPDATE t SET data = 'updated' WHERE id = 1");
      
      // UNIQUE should still be enforced
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (3, 'unique1', 'new')");
      }, /duplicate|unique|constraint/i);
    });
  });
});
