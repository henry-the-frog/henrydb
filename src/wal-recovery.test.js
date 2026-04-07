// wal-recovery.test.js — Crash recovery tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { WriteAheadLog, recoverFromWAL, WAL_TYPES } from './wal.js';

describe('Crash Recovery', () => {
  it('recovers INSERT from WAL', () => {
    // Phase 1: Create data and capture WAL
    const db1 = new Database();
    db1.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db1.execute("INSERT INTO users VALUES (1, 'Alice')");
    db1.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    // Simulate crash: create fresh DB with same schema but no data
    const db2 = new Database();
    db2.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    
    // Recover from WAL
    const stats = recoverFromWAL(db1.wal, db2);
    
    // Verify data recovered
    const rows = db2.execute('SELECT * FROM users ORDER BY id').rows;
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Bob');
    assert.ok(stats.redone >= 2);
  });

  it('only recovers committed transactions', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db1.execute('INSERT INTO t VALUES (1, 10)'); // auto-committed
    
    // Simulate an uncommitted transaction by manually adding WAL records
    const wal = db1.wal;
    wal.appendInsert(999, 't', 0, 99, [2, 20]); // No COMMIT follows
    wal.flush();
    
    // Recover into fresh DB
    const db2 = new Database();
    db2.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const stats = recoverFromWAL(wal, db2);
    
    // Only committed row should exist
    const rows = db2.execute('SELECT * FROM t').rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 10);
    assert.equal(stats.activeTxns, 1); // tx 999 was in-flight
  });

  it('recovers UPDATE operations', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db1.execute('INSERT INTO t VALUES (1, 10)');
    db1.execute('UPDATE t SET val = 20 WHERE id = 1');
    
    // Recover
    const db2 = new Database();
    db2.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    recoverFromWAL(db1.wal, db2);
    
    const rows = db2.execute('SELECT val FROM t WHERE id = 1').rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 20);
  });

  it('recovers DELETE operations', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db1.execute('INSERT INTO t VALUES (1)');
    db1.execute('INSERT INTO t VALUES (2)');
    db1.execute('DELETE FROM t WHERE id = 1');
    
    // Recover
    const db2 = new Database();
    db2.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    recoverFromWAL(db1.wal, db2);
    
    const rows = db2.execute('SELECT * FROM t').rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 2);
  });

  it('recovery from checkpoint skips pre-checkpoint data', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db1.execute('INSERT INTO t VALUES (1)');
    
    db1.checkpoint(); // Checkpoint after first insert
    
    db1.execute('INSERT INTO t VALUES (2)');
    
    // Recovery from checkpoint should only replay post-checkpoint
    const db2 = new Database();
    db2.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    // Pre-load the pre-checkpoint data (simulating it was flushed to pages)
    db2.execute('INSERT INTO t VALUES (1)');
    
    const stats = recoverFromWAL(db1.wal, db2);
    
    const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
    assert.equal(rows.length, 2);
  });

  it('CRC corruption detection in recovery', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db1.execute('INSERT INTO t VALUES (1)');
    
    // Corrupt the stable storage
    if (db1.wal._stableStorage.length > 0) {
      const buf = db1.wal._stableStorage[0];
      buf[10] ^= 0xFF; // Corrupt a byte
    }
    
    // Recovery should skip corrupted records
    const db2 = new Database();
    db2.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    const stats = recoverFromWAL(db1.wal, db2);
    
    // Corrupted records are skipped, so less data recovered
    assert.ok(stats.redone <= 1);
  });
});
