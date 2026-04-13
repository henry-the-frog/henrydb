// unique-constraints-depth.test.js — Comprehensive UNIQUE constraint tests through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-unique-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('UNIQUE Constraints Depth', () => {
  afterEach(cleanup);

  it('PRIMARY KEY prevents duplicate values', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'b')"), /duplicate|unique/i);
  });

  it('UNIQUE index prevents duplicate values', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, email TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, 'alice@test.com')"), /duplicate|unique/i);
  });

  it('UNIQUE allows different values', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
  });

  it('UNIQUE enforcement survives close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.close();
    db = TransactionalDatabase.open(dir);
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'b')"), /duplicate|unique/i);
    // But different PK should work
    db.execute("INSERT INTO t VALUES (2, 'b')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('DELETE + re-INSERT same key works', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'old')");
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute("INSERT INTO t VALUES (1, 'new')");
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 'new');
  });

  it('UPDATE preserving unique value works', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("UPDATE t SET name = 'Charlie' WHERE id = 1");
    assert.equal(db.execute('SELECT name FROM t WHERE id = 1').rows[0].name, 'Charlie');
  });

  it('UNIQUE in session transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (1, 'a')");
    // Duplicate within same transaction
    assert.throws(() => s.execute("INSERT INTO t VALUES (1, 'b')"), /duplicate|unique/i);
    s.commit();
    s.close();
  });

  it('rolled-back INSERT frees unique key', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (1, 'a')");
    s.rollback();
    s.close();
    // Key 1 should be available again
    db.execute("INSERT INTO t VALUES (1, 'b')");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'b');
  });

  it('UNIQUE with NULL values (SQL standard: multiple NULLs allowed)', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, email TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
    // Multiple NULLs should be allowed per SQL standard
    db.execute('INSERT INTO t (id) VALUES (1)'); // email = NULL
    db.execute('INSERT INTO t (id) VALUES (2)'); // email = NULL
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('many inserts with unique constraint', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    }
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 100);
    // Duplicate should fail
    assert.throws(() => db.execute("INSERT INTO t VALUES (50, 'dup')"), /duplicate|unique/i);
  });

  it('UNIQUE after bulk delete and re-insert', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    }
    db.execute('DELETE FROM t WHERE id >= 10');
    // Re-insert deleted keys
    for (let i = 10; i < 20; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'new${i}')`);
    }
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 20);
  });

  it('UNIQUE enforcement after UPDATE changes key to duplicate', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    // Try to UPDATE id 2 to 1 (duplicate PK)
    assert.throws(() => db.execute('UPDATE t SET id = 1 WHERE id = 2'), /duplicate|unique/i);
  });
});
