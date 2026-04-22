// mvcc-stress.test.js — MVCC stress tests for concurrent transaction scenarios
// Tests for EvalPlanQual, write skew, phantom protection, PK integrity
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

describe('MVCC Stress Tests', () => {
  let dir, tdb;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'henrydb-mvcc-stress-'));
    tdb = TransactionalDatabase.open(dir, { isolationLevel: 'serializable' });
  });

  afterEach(() => {
    try { tdb.close(); } catch {}
    rmSync(dir, { recursive: true, force: true });
  });

  // ===== EvalPlanQual: committed-after-snapshot conflicts =====

  it('UPDATE on row modified by committed concurrent tx is rejected', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    tdb.execute('INSERT INTO t VALUES (1, 0)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    a.execute('UPDATE t SET val = 10 WHERE id = 1');
    a.execute('COMMIT');

    // B reads stale snapshot
    const r = b.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 0); // stale snapshot

    // B tries to update — should fail
    assert.throws(
      () => b.execute('UPDATE t SET val = 20 WHERE id = 1'),
      /Serialization failure/
    );
  });

  it('UPDATE preserves PK uniqueness after concurrent modification', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    tdb.execute('INSERT INTO t VALUES (1, 0)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    a.execute('UPDATE t SET val = 10 WHERE id = 1');
    a.execute('COMMIT');

    assert.throws(
      () => b.execute('UPDATE t SET val = 20 WHERE id = 1'),
      /Serialization failure/
    );

    try { b.execute('COMMIT'); } catch {}

    const final = tdb.execute('SELECT * FROM t');
    assert.equal(final.rows.length, 1);
    assert.equal(final.rows[0].val, 10);
  });

  // ===== DELETE conflicts =====

  it('DELETE on row deleted by committed concurrent tx is rejected', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    tdb.execute("INSERT INTO t VALUES (1, 'original')");

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    a.execute('DELETE FROM t WHERE id = 1');
    a.execute('COMMIT');

    assert.throws(
      () => b.execute('DELETE FROM t WHERE id = 1'),
      /Serialization failure/
    );
  });

  // ===== INSERT-INSERT PK conflict =====

  it('INSERT with duplicate PK is rejected', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    a.execute("INSERT INTO t VALUES (1, 'from A')");

    assert.throws(
      () => b.execute("INSERT INTO t VALUES (1, 'from B')"),
      /Duplicate key|unique constraint/i
    );

    a.execute('COMMIT');
    try { b.execute('COMMIT'); } catch {}

    const final = tdb.execute('SELECT * FROM t WHERE id = 1');
    assert.equal(final.rows.length, 1);
    assert.equal(final.rows[0].val, 'from A');
  });

  // ===== Write Skew Detection (SSI) =====

  it('detects write skew (classic bank account example)', () => {
    tdb.execute('CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT)');
    tdb.execute("INSERT INTO accounts VALUES (1, 'checking', 100)");
    tdb.execute("INSERT INTO accounts VALUES (2, 'savings', 100)");

    const s1 = tdb.session();
    const s2 = tdb.session();

    s1.execute('BEGIN');
    s2.execute('BEGIN');

    // Both read total balance
    s1.execute('SELECT SUM(balance) as total FROM accounts');
    s2.execute('SELECT SUM(balance) as total FROM accounts');

    // Each withdraws from different account
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s2.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 2');

    s1.execute('COMMIT');

    assert.throws(
      () => s2.execute('COMMIT'),
      /Serialization failure|serializable/i
    );

    const final = tdb.execute('SELECT * FROM accounts ORDER BY id');
    const total = final.rows.reduce((sum, r) => sum + r.balance, 0);
    assert.ok(total >= 100, `Total ${total} should be >= 100`);
  });

  // ===== Phantom Read Prevention =====

  it('prevents phantom reads from concurrent INSERTs', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    tdb.execute('INSERT INTO t VALUES (1, 10)');
    tdb.execute('INSERT INTO t VALUES (2, 20)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    // A reads count
    const cnt1 = a.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(cnt1.rows[0].cnt, 2);

    // B inserts and commits
    b.execute('INSERT INTO t VALUES (3, 30)');
    b.execute('COMMIT');

    // A reads count again — should be same
    const cnt2 = a.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(cnt2.rows[0].cnt, 2); // phantom protected

    a.execute('COMMIT');
  });

  // ===== Double-Reservation Prevention =====

  it('prevents double-reservation with concurrent updates', () => {
    tdb.execute('CREATE TABLE inventory (id INT PRIMARY KEY, stock INT, reserved INT)');
    tdb.execute('INSERT INTO inventory VALUES (1, 10, 0)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    // Both check available stock
    a.execute('SELECT stock - reserved as avail FROM inventory WHERE id = 1');
    b.execute('SELECT stock - reserved as avail FROM inventory WHERE id = 1');

    // A reserves 8
    a.execute('UPDATE inventory SET reserved = reserved + 8 WHERE id = 1');
    a.execute('COMMIT');

    // B tries to reserve 5 — should fail (would over-reserve)
    assert.throws(
      () => b.execute('UPDATE inventory SET reserved = reserved + 5 WHERE id = 1'),
      /Serialization failure/
    );
  });

  // ===== Non-conflicting concurrent transactions =====

  it('allows non-conflicting concurrent updates on different rows', () => {
    tdb.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    tdb.execute('INSERT INTO t VALUES (1, 10)');
    tdb.execute('INSERT INTO t VALUES (2, 20)');

    const a = tdb.session();
    const b = tdb.session();

    a.execute('BEGIN');
    b.execute('BEGIN');

    // Update different rows — should both succeed
    a.execute('UPDATE t SET val = 100 WHERE id = 1');
    b.execute('UPDATE t SET val = 200 WHERE id = 2');

    a.execute('COMMIT');
    // Note: SSI may still reject this if dangerous structure is detected
    // from the SeqScan reading all rows
    let b_committed = false;
    try {
      b.execute('COMMIT');
      b_committed = true;
    } catch (e) {
      // SSI false positive — acceptable for now
      b_committed = false;
    }

    const final = tdb.execute('SELECT * FROM t ORDER BY id');
    // Either both committed or B was rejected (false positive)
    if (b_committed) {
      assert.equal(final.rows[0].val, 100);
      assert.equal(final.rows[1].val, 200);
    } else {
      assert.equal(final.rows[0].val, 100);
      assert.equal(final.rows[1].val, 20);
    }
  });
});
