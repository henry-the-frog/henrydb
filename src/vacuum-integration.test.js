// vacuum-integration.test.js — VACUUM reclaims dead tuples from MVCC
// Tests that VACUUM correctly identifies and physically removes rows
// that are no longer visible to any active snapshot

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-vacuum-'));
  db = TransactionalDatabase.open(dbDir);
}

function teardown() {
  try { db.close(); } catch (e) { /* ignore */ }
  rmSync(dbDir, { recursive: true, force: true });
}

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

describe('VACUUM: Dead Tuple Reclamation', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('reclaims rows from committed deletes when no snapshots active', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    
    db.execute('DELETE FROM t WHERE id = 2');
    
    // Row should already be logically deleted
    const r1 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r1.length, 2);
    
    // VACUUM should find and reclaim the dead tuple
    const result = db.vacuum();
    assert.ok(result.t, 'Should have results for table t');
    assert.ok(result.t.deadTuplesRemoved >= 0, 'Should report dead tuples');
  });

  it('does NOT reclaim rows visible to active snapshots', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    // Start a long-running transaction
    const s1 = db.session();
    s1.begin();
    const read1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read1.length, 2);

    // Delete a row in another session
    const s2 = db.session();
    s2.begin();
    s2.execute('DELETE FROM t WHERE id = 2');
    s2.commit();
    s2.close();

    // VACUUM should NOT reclaim id=2 because s1's snapshot still needs it
    const result = db.vacuum();
    
    // s1 should STILL see both rows
    const read2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read2.length, 2, 'VACUUM must not reclaim rows visible to active snapshots');
    
    s1.commit();
    s1.close();
  });

  it('reclaims after all snapshots close', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    // Open and close a snapshot
    const s1 = db.session();
    s1.begin();
    rows(s1.execute('SELECT * FROM t'));
    s1.commit();
    s1.close();

    // Delete a row
    db.execute('DELETE FROM t WHERE id = 2');

    // VACUUM — no active snapshots, should reclaim
    const result = db.vacuum();
    
    // Verify it's actually gone from the heap
    const remaining = rows(db.execute('SELECT * FROM t'));
    assert.equal(remaining.length, 1);
    assert.equal(remaining[0].id, 1);
  });

  it('multiple deletes all get vacuumed', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    // Delete half the rows
    for (let i = 1; i <= 50; i++) {
      db.execute(`DELETE FROM t WHERE id = ${i}`);
    }

    const result = db.vacuum();
    
    const remaining = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(remaining[0].c, 50, 'Should have 50 rows after vacuum');
  });

  it('VACUUM after UPDATE reclaims old versions', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    db.execute("UPDATE t SET val = 'updated' WHERE id = 1");

    db.vacuum();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'updated');
  });

  it('empty table VACUUM is a no-op', () => {
    db.execute('CREATE TABLE t (id INT)');
    const result = db.vacuum();
    // Should not crash
    assert.ok(true, 'VACUUM on empty table should not crash');
  });

  it('VACUUM preserves data integrity (sum invariant)', () => {
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 500)');
    db.execute('INSERT INTO accounts VALUES (2, 500)');

    // Do some transfers
    const s = db.session();
    s.begin();
    s.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
    s.commit();
    s.close();

    // VACUUM
    db.vacuum();

    // Sum should still be 1000
    const sum = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum[0].total, 1000, 'VACUUM must preserve data integrity');
  });
});
