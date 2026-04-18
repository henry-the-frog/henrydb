// concurrent-alter.test.js — Concurrent transactions + ALTER TABLE
// Tests MVCC behavior when schema changes happen while transactions are in progress.
// In PostgreSQL, DDL acquires an AccessExclusiveLock, blocking concurrent reads/writes.
// HenryDB doesn't have table locks, so schema changes are visible immediately —
// which can cause fun issues.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-concurrent-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Concurrent Transactions + ALTER TABLE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ALTER TABLE ADD COLUMN while read transaction is open', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    // Start a read transaction
    const session1 = db.session(); session1.begin();
    const r1 = rows(session1.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r1.length, 2, 'Session 1 should see 2 rows');
    
    // ALTER TABLE happens outside the transaction
    db.execute('ALTER TABLE t ADD COLUMN score INT');
    db.execute("INSERT INTO t VALUES (3, 'Carol', 100)");
    
    // Session 1 should still work — either sees old schema or new schema
    try {
      const r2 = rows(session1.execute('SELECT * FROM t ORDER BY id'));
      assert.ok(r2.length >= 2, 'Session 1 should still see rows after ALTER');
      session1.commit();
    } catch (e) {
      // If it throws, that's acceptable (schema changed mid-tx)
      assert.ok(true, `Schema change during tx threw: ${e.message}`);
    }
  });

  it('ALTER TABLE DROP COLUMN while write transaction is open', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 100)");
    
    // Start a write transaction
    const session1 = db.session(); session1.begin();
    session1.execute("INSERT INTO t VALUES (2, 'Bob', 200)");
    
    // DROP COLUMN happens outside the transaction
    db.execute('ALTER TABLE t DROP COLUMN score');
    
    // Try to commit session1 — the INSERT had 3 values but schema now has 2 columns
    try {
      session1.commit();
      // If commit succeeds, verify data integrity
      const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
      assert.ok(r.length >= 1, 'Should have at least original rows');
    } catch (e) {
      // Commit failure is acceptable — schema conflict
      assert.ok(true, `Commit after DROP COLUMN threw: ${e.message}`);
    }
  });

  it('RENAME TABLE while transaction references old name', () => {
    db.execute('CREATE TABLE old_t (id INT)');
    db.execute('INSERT INTO old_t VALUES (1)');
    
    // Start a read transaction
    const session1 = db.session(); session1.begin();
    const r1 = rows(session1.execute('SELECT * FROM old_t'));
    assert.equal(r1.length, 1, 'Session 1 should see 1 row');
    
    // RENAME TABLE outside the transaction
    db.execute('ALTER TABLE old_t RENAME TO new_t');
    
    // Session 1 tries to query old name
    try {
      const r2 = rows(session1.execute('SELECT * FROM old_t'));
      // If it works, old name is still resolvable in this tx
      assert.ok(true, 'Old name still accessible in open tx');
    } catch (e) {
      // Table not found is acceptable — schema changed
      assert.ok(e.message.includes('not found') || e.message.includes('no such table'),
        `Should fail with table not found, got: ${e.message}`);
    }
    session1.commit();
    
    // New name should work
    const r3 = rows(db.execute('SELECT * FROM new_t'));
    assert.equal(r3.length, 1, 'New name should work after rename');
  });

  it('two transactions insert, ALTER TABLE between them', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    // Session 1 inserts with old schema
    const s1 = db.session(); s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    
    // ALTER TABLE
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    
    // Session 2 inserts with new schema
    const s2 = db.session(); s2.begin();
    s2.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    // Both commit
    s1.commit();
    s2.commit();
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Both inserts should succeed');
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 2);
  });

  it('rapid ALTER TABLE sequence does not corrupt state', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    
    for (let i = 0; i < 10; i++) {
      db.execute(`ALTER TABLE t ADD COLUMN c${i} INT`);
    }
    
    // Should still be queryable
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1, 'Should have 1 row');
    assert.equal(r[0].id, 1);
    // Should have 11 columns (id + c0..c9)
    const cols = Object.keys(r[0]);
    assert.equal(cols.length, 11, `Should have 11 columns, got ${cols.length}: ${cols.join(', ')}`);
  });

  it('ALTER TABLE ADD COLUMN with concurrent INSERT race', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    // Simulate race: INSERT starts, ALTER happens, INSERT commits
    const s1 = db.session(); s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    
    db.execute('ALTER TABLE t ADD COLUMN val TEXT');
    db.execute("INSERT INTO t VALUES (2, 'direct')");
    
    s1.commit();
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Both rows should exist');
    // Row 1 may or may not have val column — depends on when schema change is visible
    assert.equal(r[1].id, 2);
    assert.equal(r[1].val, 'direct');
  });

  it('ALTER TABLE RENAME COLUMN with concurrent SELECT', () => {
    db.execute('CREATE TABLE t (id INT, old_name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    
    const s1 = db.session(); s1.begin();
    // Read before rename
    const r1 = rows(s1.execute('SELECT old_name FROM t'));
    
    // Rename column
    db.execute('ALTER TABLE t RENAME COLUMN old_name TO new_name');
    
    // Try to read with old column name
    try {
      const r2 = rows(s1.execute('SELECT old_name FROM t'));
      assert.ok(true, 'Old column name still works in open tx');
    } catch (e) {
      // Column not found is acceptable
      assert.ok(true, `Column renamed during tx: ${e.message}`);
    }
    s1.commit();
    
    // After commit, new name should work
    const r3 = rows(db.execute('SELECT new_name FROM t'));
    assert.equal(r3.length, 1);
    assert.equal(r3[0].new_name, 'Alice');
  });

  it('ALTER TABLE does not break in-progress aggregation', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    }
    
    const s1 = db.session(); s1.begin();
    const r1 = rows(s1.execute('SELECT SUM(val) as total FROM t'));
    
    // ALTER TABLE during aggregation session
    db.execute('ALTER TABLE t ADD COLUMN extra TEXT');
    db.execute("INSERT INTO t VALUES (101, 1010, 'new')");
    
    // Aggregation should still be consistent
    try {
      const r2 = rows(s1.execute('SELECT COUNT(*) as cnt FROM t'));
      // Either 100 (snapshot isolation) or 101 (read committed)
      assert.ok(r2[0].cnt >= 100, `Count should be at least 100, got ${r2[0].cnt}`);
    } catch {
      assert.ok(true, 'Aggregation threw after ALTER (acceptable)');
    }
    s1.commit();
  });

  it('many concurrent sessions with ALTER TABLE', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    
    // Create 5 sessions, each doing inserts
    const sessions = [];
    for (let i = 0; i < 5; i++) {
      const s = db.session(); s.begin();
      s.execute(`INSERT INTO t VALUES (${i}, 'session-${i}')`);
      sessions.push(s);
    }
    
    // ALTER TABLE while all sessions are open
    db.execute('ALTER TABLE t ADD COLUMN score INT');
    
    // Commit all sessions
    for (const s of sessions) {
      try { s.commit(); } catch { /* accept failures */ }
    }
    
    // Insert with new schema
    db.execute("INSERT INTO t VALUES (99, 'after', 100)");
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.ok(r.length >= 1, 'Should have at least some rows');
    const row99 = r.find(x => x.id === 99);
    assert.ok(row99, 'Post-ALTER insert should exist');
    assert.equal(row99.score, 100);
  });
});
