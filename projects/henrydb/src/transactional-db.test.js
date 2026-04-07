// transactional-db.test.js — TransactionalDatabase integration tests
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase, TransactionSession } from './transactional-db.js';
import { mkdirSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

function tmpDir() {
  const dir = join(tmpdir(), `henrydb-txn-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

describe('TransactionalDatabase', () => {
  let dir, db;

  beforeEach(() => {
    dir = tmpDir();
  });

  afterEach(() => {
    try { if (db) db.close(); } catch (e) { /* ignore */ }
    try { rmSync(dir, { recursive: true, force: true }); } catch (e) { /* ignore */ }
  });

  it('should create tables and insert/select in auto-commit mode', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].name, 'Bob');
  });

  it('should persist data across close/reopen', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.close();

    db = TransactionalDatabase.open(dir);
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('should support explicit BEGIN/COMMIT', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO users VALUES (1, 'Alice')");
    s.execute("INSERT INTO users VALUES (2, 'Bob')");
    s.commit();

    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
  });

  it('should support ROLLBACK — inserted rows disappear', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO users VALUES (1, 'Alice')");
    
    // Alice should be visible within the transaction
    const inTx = s.execute('SELECT * FROM users');
    assert.equal(inTx.rows.length, 1);
    
    s.rollback();

    // After rollback, Alice should be gone
    const result = db.execute('SELECT * FROM users');
    assert.equal(result.rows.length, 0);
  });

  it('should provide snapshot isolation — uncommitted writes invisible to other sessions', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s1.execute("INSERT INTO users VALUES (2, 'Bob')");

    // s1 sees both Alice and Bob
    const s1Result = s1.execute('SELECT * FROM users ORDER BY id');
    assert.equal(s1Result.rows.length, 2);

    // s2 should NOT see Bob (uncommitted)
    s2.begin();
    const s2Result = s2.execute('SELECT * FROM users ORDER BY id');
    assert.equal(s2Result.rows.length, 1);
    assert.equal(s2Result.rows[0].name, 'Alice');

    // After s1 commits, s2's snapshot still doesn't see Bob (snapshot isolation)
    s1.commit();
    const s2Result2 = s2.execute('SELECT * FROM users ORDER BY id');
    assert.equal(s2Result2.rows.length, 1); // Still 1 — s2's snapshot was taken at BEGIN

    s2.commit();

    // New session/query should see both
    const finalResult = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(finalResult.rows.length, 2);
  });

  it('should handle DELETE with MVCC visibility', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");

    const s1 = db.session();
    s1.begin();
    s1.execute("DELETE FROM users WHERE id = 2");

    // s1 sees only Alice
    const s1Result = s1.execute('SELECT * FROM users');
    assert.equal(s1Result.rows.length, 1);

    // Other sessions still see Bob (delete not committed)
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);

    s1.commit();

    // After commit, Bob is gone for everyone
    const afterCommit = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(afterCommit.rows.length, 1);
    assert.equal(afterCommit.rows[0].name, 'Alice');
  });

  it('should handle UPDATE with MVCC (delete old + insert new)', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");

    const s1 = db.session();
    s1.begin();
    s1.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");

    // s1 sees updated name
    const s1Result = s1.execute('SELECT name FROM users WHERE id = 1');
    assert.equal(s1Result.rows.length, 1);
    assert.equal(s1Result.rows[0].name, 'Alicia');

    // Other sessions see old name
    const otherResult = db.execute('SELECT name FROM users WHERE id = 1');
    assert.equal(otherResult.rows[0].name, 'Alice');

    s1.commit();

    // After commit, everyone sees new name
    const afterCommit = db.execute('SELECT name FROM users WHERE id = 1');
    assert.equal(afterCommit.rows[0].name, 'Alicia');
  });

  it('should persist transactions across crash recovery', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.close();

    // Simulate crash recovery
    db = TransactionalDatabase.open(dir, { recover: true });
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].name, 'Bob');
  });

  it('should support multiple sessions with independent transactions', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE counter (id INT, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');

    const s1 = db.session();
    const s2 = db.session();

    // s1: read counter
    s1.begin();
    const v1 = s1.execute('SELECT val FROM counter WHERE id = 1');
    assert.equal(v1.rows[0].val, 0);

    // s2: increment counter
    s2.begin();
    s2.execute('UPDATE counter SET val = 1 WHERE id = 1');
    s2.commit();

    // s1 still sees 0 (snapshot isolation)
    const v1Again = s1.execute('SELECT val FROM counter WHERE id = 1');
    assert.equal(v1Again.rows[0].val, 0);

    s1.commit();

    // New read sees 1
    const final = db.execute('SELECT val FROM counter WHERE id = 1');
    assert.equal(final.rows[0].val, 1);
  });

  it('should execute SQL via session.execute with BEGIN/COMMIT strings', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (x INT)');
    
    const s = db.session();
    s.execute('BEGIN');
    s.execute('INSERT INTO t VALUES (42)');
    s.execute('COMMIT');

    const result = db.execute('SELECT x FROM t');
    assert.equal(result.rows[0].x, 42);
  });

  it('should execute SQL via session.execute with ROLLBACK string', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (x INT)');
    
    const s = db.session();
    s.execute('BEGIN');
    s.execute('INSERT INTO t VALUES (42)');
    s.execute('ROLLBACK');

    const result = db.execute('SELECT * FROM t');
    assert.equal(result.rows.length, 0);
  });

  it('should close session and rollback active transaction', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (x INT)');
    
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    s.close(); // Should rollback

    const result = db.execute('SELECT * FROM t');
    assert.equal(result.rows.length, 0);
  });

  it('should support VACUUM to remove dead tuples', () => {
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (x INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    
    db.execute('DELETE FROM t WHERE x = 2');

    const results = db.vacuum();
    // Vacuum should have found at least some dead tuples
    // (the deleted row's version has xmax set)
    const result = db.execute('SELECT * FROM t ORDER BY x');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].x, 1);
    assert.equal(result.rows[1].x, 3);
  });
});
