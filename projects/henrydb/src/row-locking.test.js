// row-locking.test.js — SELECT FOR UPDATE / FOR SHARE row-level locking
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function q(db, sql) {
  const r = db.execute(sql);
  return r.rows || r || [];
}

describe('Row-Level Locking (SELECT FOR UPDATE/SHARE)', () => {
  describe('basic FOR UPDATE', () => {
    it('FOR UPDATE acquires lock and returns rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      const r = q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      assert.equal(r.length, 1);
      assert.equal(r[0].val, 100);
      
      // Verify lock exists
      assert.equal(db._rowLocks.size, 1);
      const lock = [...db._rowLocks.values()][0];
      assert.equal(lock.mode, 'UPDATE');
      
      db.execute('COMMIT');
    });

    it('COMMIT releases all locks', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      db.execute('INSERT INTO t VALUES (2, 200)');
      
      db.execute('BEGIN');
      q(db, 'SELECT * FROM t FOR UPDATE');
      assert.ok(db._rowLocks.size >= 1, 'Locks acquired');
      
      db.execute('COMMIT');
      assert.equal(db._rowLocks.size, 0, 'All locks released on commit');
    });

    it('ROLLBACK releases all locks', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      q(db, 'SELECT * FROM t FOR UPDATE');
      assert.ok(db._rowLocks.size >= 1);
      
      db.execute('ROLLBACK');
      assert.equal(db._rowLocks.size, 0, 'All locks released on rollback');
    });
  });

  describe('conflict detection', () => {
    it('UPDATE on FOR UPDATE locked row throws', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      
      // Different tx tries to update
      const saved = db._currentTxId;
      db._currentTxId = 200;
      assert.throws(
        () => db.execute('UPDATE t SET val = 999 WHERE id = 1'),
        /locked by transaction 100/
      );
      
      db._currentTxId = saved;
      db.execute('COMMIT');
    });

    it('DELETE on FOR UPDATE locked row throws', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      
      const saved = db._currentTxId;
      db._currentTxId = 200;
      assert.throws(
        () => db.execute('DELETE FROM t WHERE id = 1'),
        /locked by transaction 100/
      );
      
      db._currentTxId = saved;
      db.execute('COMMIT');
    });

    it('same transaction can UPDATE its own locked rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      
      // Same tx can update its own locked row
      db.execute('UPDATE t SET val = 200 WHERE id = 1');
      const r = q(db, 'SELECT val FROM t WHERE id = 1');
      assert.equal(r[0].val, 200);
      
      db.execute('COMMIT');
    });

    it('after COMMIT, other transaction can update', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      db.execute('COMMIT');
      
      // After release, different tx can update
      db._currentTxId = 200;
      db.execute('UPDATE t SET val = 999 WHERE id = 1');
      db._currentTxId = 0;
      
      const r = q(db, 'SELECT val FROM t WHERE id = 1');
      assert.equal(r[0].val, 999);
    });
  });

  describe('FOR SHARE', () => {
    it('FOR SHARE allows concurrent read locks', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      // TX 1: share lock
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR SHARE');
      
      // TX 2: share lock should NOT conflict
      const saved = db._currentTxId;
      db._currentTxId = 200;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR SHARE');
      
      // Both locks should exist
      assert.ok(db._rowLocks.size >= 1);
      
      db._currentTxId = saved;
      db.execute('COMMIT');
    });
  });

  describe('NOWAIT', () => {
    it('FOR UPDATE NOWAIT throws immediately on conflict', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE');
      
      const saved = db._currentTxId;
      db._currentTxId = 200;
      assert.throws(
        () => db.execute('SELECT * FROM t WHERE id = 1 FOR UPDATE NOWAIT'),
        /Could not obtain lock/
      );
      
      db._currentTxId = saved;
      db.execute('COMMIT');
    });
  });

  describe('SKIP LOCKED', () => {
    it('FOR UPDATE SKIP LOCKED skips locked rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      db.execute('INSERT INTO t VALUES (2, 200)');
      
      db.execute('BEGIN');
      db._currentTxId = 100;
      q(db, 'SELECT * FROM t WHERE id = 1 FOR UPDATE'); // Lock row 1
      
      const saved = db._currentTxId;
      db._currentTxId = 200;
      // SKIP LOCKED should return only unlocked rows
      const r = q(db, 'SELECT * FROM t FOR UPDATE SKIP LOCKED');
      // Row 1 is locked, row 2 should be returned
      assert.equal(r.length, 1);
      assert.equal(r[0].id, 2);
      
      db._currentTxId = saved;
      db.execute('COMMIT');
    });
  });

  describe('multi-row locking', () => {
    it('locks multiple rows with FOR UPDATE', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      
      db.execute('BEGIN');
      q(db, 'SELECT * FROM t WHERE id <= 3 FOR UPDATE');
      assert.ok(db._rowLocks.size >= 1, 'Multiple rows locked');
      
      db.execute('COMMIT');
      assert.equal(db._rowLocks.size, 0);
    });
  });
});
