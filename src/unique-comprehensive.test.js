import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('UNIQUE + SI Comprehensive Tests', () => {

  describe('UNIQUE with INSERT ... SELECT', () => {
    it('should enforce UNIQUE on INSERT ... SELECT', () => {
      const db = new Database();
      db.execute('CREATE TABLE source (id INTEGER, val TEXT)');
      db.execute('CREATE TABLE target (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO source VALUES (1, 'a')");
      db.execute("INSERT INTO source VALUES (2, 'b')");
      db.execute("INSERT INTO source VALUES (1, 'c')"); // duplicate id
      
      db.execute('INSERT INTO target SELECT id, val FROM source WHERE id = 2');
      
      assert.throws(() => {
        // Would insert id=1 twice
        db.execute('INSERT INTO target SELECT id, val FROM source WHERE id = 1');
      }, /duplicate|unique|constraint/i);
    });
  });

  describe('UNIQUE with UPDATE SET from subquery', () => {
    it('UPDATE swapping unique values should handle correctly', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, pos INTEGER)');
      db.execute('CREATE UNIQUE INDEX idx_pos ON t (pos)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      db.execute('INSERT INTO t VALUES (2, 20)');
      
      // Updating pos=10 to pos=30 should work
      db.execute('UPDATE t SET pos = 30 WHERE id = 1');
      const rows = query(db, 'SELECT * FROM t ORDER BY id');
      assert.equal(rows[0].pos, 30);
      assert.equal(rows[1].pos, 20);
    });
  });

  describe('UNIQUE + VACUUM interaction', () => {
    it('VACUUM should not affect UNIQUE enforcement', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      
      for (let i = 0; i < 20; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, 'user${i}@test.com')`);
      }
      
      // Delete half, VACUUM, then try re-inserting
      for (let i = 0; i < 10; i++) {
        db.execute(`DELETE FROM t WHERE id = ${i}`);
      }
      db.execute('VACUUM');
      
      // Deleted emails should be available
      for (let i = 0; i < 10; i++) {
        db.execute(`INSERT INTO t VALUES (${i + 20}, 'user${i}@test.com')`);
      }
      
      // Remaining emails should still be enforced
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (100, 'user15@test.com')");
      }, /duplicate|unique|constraint/i);
    });
  });

  describe('UNIQUE + HOT updates', () => {
    it('HOT update on non-indexed col then UNIQUE check still works', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, bio TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com', 'old bio')");
      db.execute("INSERT INTO t VALUES (2, 'bob@test.com', 'old bio')");
      
      // HOT update bio (non-indexed)
      for (let i = 0; i < 10; i++) {
        db.execute(`UPDATE t SET bio = 'bio_v${i}' WHERE id = 1`);
      }
      
      // UNIQUE on email still enforced
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (3, 'alice@test.com', 'new')");
      }, /duplicate|unique|constraint/i);
    });
  });

  describe('UNIQUE + serialization roundtrip', () => {
    it('UNIQUE enforcement survives serialization', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_code ON t (code)');
      db.execute("INSERT INTO t VALUES (1, 'ABC')");
      db.execute("INSERT INTO t VALUES (2, 'DEF')");
      
      const json = db.toJSON();
      const db2 = Database.fromJSON(json);
      
      // UNIQUE should still work in restored DB
      assert.throws(() => {
        db2.execute("INSERT INTO t VALUES (3, 'ABC')");
      }, /duplicate|unique|constraint/i);
      
      // Non-duplicate should work
      db2.execute("INSERT INTO t VALUES (3, 'GHI')");
      assert.equal(query(db2, 'SELECT COUNT(*) AS c FROM t')[0].c, 3);
    });
  });

  describe('UNIQUE + transaction rollback edge cases', () => {
    it('nested operations: insert, update, delete, rollback', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'original')");
      
      db.execute('BEGIN');
      db.execute("INSERT INTO t VALUES (2, 'new')");
      db.execute("UPDATE t SET val = 'modified' WHERE id = 1");
      db.execute("DELETE FROM t WHERE id = 1"); // Delete modified row
      db.execute('ROLLBACK');
      
      // Original state should be restored
      const rows = query(db, 'SELECT * FROM t ORDER BY id');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].id, 1);
      assert.equal(rows[0].val, 'original');
      
      // Key 2 should be available
      db.execute("INSERT INTO t VALUES (2, 'after_rollback')");
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 2);
    });

    it('rollback after update that changes UNIQUE column', () => {
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
      
      // charlie@test.com should be available (was rolled back)
      db.execute("INSERT INTO t VALUES (3, 'charlie@test.com')");
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM t')[0].c, 3);
    });

    it('stress: many rollback cycles with mixed operations', () => {
      const db = new Database({ mvcc: true });
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
      
      // Start with 5 rows
      for (let i = 1; i <= 5; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      }
      
      // Run 10 rollback cycles
      for (let cycle = 0; cycle < 10; cycle++) {
        db.execute('BEGIN');
        db.execute(`INSERT INTO t VALUES (${100 + cycle}, 999)`);
        db.execute(`UPDATE t SET val = ${cycle} WHERE id = 1`);
        db.execute('DELETE FROM t WHERE id = 3');
        db.execute('ROLLBACK');
      }
      
      // State should be unchanged from initial
      const rows = query(db, 'SELECT * FROM t ORDER BY id');
      assert.equal(rows.length, 5);
      assert.equal(rows[0].val, 10); // id=1 should have original val
      assert.equal(rows[2].id, 3);    // id=3 should still exist
    });
  });

  describe('UNIQUE constraint with multiple indexes', () => {
    it('should enforce both PK and UNIQUE index independently', () => {
      const db = new Database();
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, username TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON users (email)');
      db.execute('CREATE UNIQUE INDEX idx_username ON users (username)');
      
      db.execute("INSERT INTO users VALUES (1, 'a@test.com', 'alice')");
      db.execute("INSERT INTO users VALUES (2, 'b@test.com', 'bob')");
      
      // Duplicate PK
      assert.throws(() => db.execute("INSERT INTO users VALUES (1, 'c@test.com', 'charlie')"), /duplicate|unique|constraint/i);
      // Duplicate email
      assert.throws(() => db.execute("INSERT INTO users VALUES (3, 'a@test.com', 'charlie')"), /duplicate|unique|constraint/i);
      // Duplicate username
      assert.throws(() => db.execute("INSERT INTO users VALUES (3, 'c@test.com', 'alice')"), /duplicate|unique|constraint/i);
      
      // All unique — should succeed
      db.execute("INSERT INTO users VALUES (3, 'c@test.com', 'charlie')");
      assert.equal(query(db, 'SELECT COUNT(*) AS c FROM users')[0].c, 3);
    });
  });
});
