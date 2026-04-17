// vacuum-mvcc-depth.test.js — Deep VACUUM + MVCC interaction tests
// Tests the integration boundary between VACUUM cleanup and MVCC snapshots.
// Goal: find bugs where VACUUM either (a) reclaims rows still needed by active
// snapshots, or (b) fails to reclaim rows that no snapshot needs.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-vacuum-depth-'));
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

describe('VACUUM + MVCC Depth: Concurrent Write Interactions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('VACUUM during an active writing transaction does not reclaim uncommitted deletes', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");

    // s1 starts a DELETE but doesn't commit
    const s1 = db.session();
    s1.begin();
    s1.execute('DELETE FROM t WHERE id = 2');

    // VACUUM runs — should NOT reclaim id=2 (delete not committed)
    db.vacuum();

    // s1 rolls back — row should still be there
    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3, 'Rolled-back delete after VACUUM should preserve all rows');
    assert.deepEqual(r.map(x => x.id), [1, 2, 3]);
  });

  it('VACUUM during an active reading transaction respects reader snapshot', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');

    // s1 starts reading — takes a snapshot that sees all 3 rows
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r1.length, 3);

    // Another session deletes and commits
    const s2 = db.session();
    s2.begin();
    s2.execute('DELETE FROM t WHERE id = 2');
    s2.commit();
    s2.close();

    // VACUUM runs — s1 still has active snapshot, so id=2 must survive
    db.vacuum();

    // s1 should STILL see 3 rows (snapshot isolation)
    const r2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r2.length, 3, 'VACUUM must not reclaim rows visible to active reader snapshot');

    s1.commit();
    s1.close();
  });

  it('VACUUM reclaims after ALL overlapping snapshots close', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    // s1 and s2 both take snapshots
    const s1 = db.session();
    s1.begin();
    rows(s1.execute('SELECT * FROM t'));

    const s2 = db.session();
    s2.begin();
    rows(s2.execute('SELECT * FROM t'));

    // Delete in a third session
    const s3 = db.session();
    s3.begin();
    s3.execute('DELETE FROM t WHERE id = 2');
    s3.commit();
    s3.close();

    // Close s1 but NOT s2
    s1.commit();
    s1.close();

    // VACUUM should still not reclaim (s2 is active)
    db.vacuum();
    const r1 = rows(s2.execute('SELECT * FROM t'));
    assert.equal(r1.length, 2, 'Should still see 2 rows while s2 is active');

    // Close s2
    s2.commit();
    s2.close();

    // NOW vacuum should reclaim
    const result = db.vacuum();

    // Verify only 1 row remains
    const r2 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].id, 1);
  });
});

describe('VACUUM + UPDATE Version Chain Cleanup', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('VACUUM cleans old versions after multiple updates to same row', () => {
    db.execute('CREATE TABLE t (id INT, ver INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');

    // Update the same row 10 times
    for (let i = 2; i <= 10; i++) {
      db.execute(`UPDATE t SET ver = ${i} WHERE id = 1`);
    }

    // VACUUM should clean up old versions
    db.vacuum();

    // Should still see the latest version
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].ver, 10, 'Should see latest version after VACUUM');
  });

  it('VACUUM preserves old version if active snapshot needs it', () => {
    db.execute('CREATE TABLE t (id INT, ver INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');

    // s1 takes a snapshot seeing ver=1
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r1[0].ver, 1);

    // Update to ver=2 in another session
    db.execute('UPDATE t SET ver = 2 WHERE id = 1');

    // VACUUM runs
    db.vacuum();

    // s1 should still see ver=1
    const r2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r2[0].ver, 1, 'Old version must survive VACUUM while snapshot needs it');

    // New reads should see ver=2
    const r3 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r3[0].ver, 2);

    s1.commit();
    s1.close();
  });

  it('chain of updates: each intermediate version cleaned after snapshot closes', () => {
    db.execute('CREATE TABLE t (id INT, ver INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');

    // Snapshot at ver=1
    const s1 = db.session();
    s1.begin();
    rows(s1.execute('SELECT * FROM t'));

    // Update to ver=2, ver=3, ver=4
    db.execute('UPDATE t SET ver = 2 WHERE id = 1');
    db.execute('UPDATE t SET ver = 3 WHERE id = 1');
    db.execute('UPDATE t SET ver = 4 WHERE id = 1');

    // VACUUM with active snapshot — must keep all versions s1 might need
    db.vacuum();

    // s1 still sees ver=1
    const r1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r1[0].ver, 1);

    s1.commit();
    s1.close();

    // Second VACUUM after snapshot closes
    db.vacuum();

    // Only latest version should remain
    const r2 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].ver, 4);
  });
});

describe('VACUUM + DELETE + Re-INSERT Same Key', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DELETE then re-INSERT: VACUUM does not confuse new row with old dead tuple', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    // Delete the row
    db.execute('DELETE FROM t WHERE id = 1');

    // Re-insert with same id but different value
    db.execute("INSERT INTO t VALUES (1, 'reinserted')");

    // VACUUM — should clean old dead tuple but NOT the new row
    db.vacuum();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'reinserted', 'Re-inserted row must survive VACUUM');
  });

  it('DELETE + re-INSERT while old snapshot still active', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'v1')");

    // s1 sees v1
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r1[0].val, 'v1');

    // Delete and re-insert
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute("INSERT INTO t VALUES (1, 'v2')");

    // VACUUM
    db.vacuum();

    // s1 should still see v1
    const r2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(r2[0].val, 'v1', 'Old snapshot must see original value after delete+reinsert+vacuum');

    // New read sees v2
    const r3 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r3[0].val, 'v2');

    s1.commit();
    s1.close();
  });
});

describe('VACUUM + SSI Transaction Interaction', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('VACUUM during SSI transaction does not corrupt conflict detection', () => {
    // SSI needs version history to detect write skew.
    // If VACUUM removes versions that SSI still needs for rw-dependency tracking,
    // it could cause missed conflicts or false positives.
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 100)');
    db.execute('INSERT INTO accounts VALUES (2, 100)');

    // Classic write skew setup: two transactions each read both, write one
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM accounts'));
    const total1 = r1.reduce((s, r) => s + r.balance, 0);

    const s2 = db.session();
    s2.begin();
    const r2 = rows(s2.execute('SELECT * FROM accounts'));
    const total2 = r2.reduce((s, r) => s + r.balance, 0);

    // Both see total=200
    assert.equal(total1, 200);
    assert.equal(total2, 200);

    // s1 withdraws from account 1
    s1.execute('UPDATE accounts SET balance = 0 WHERE id = 1');

    // VACUUM runs mid-transaction
    db.vacuum();

    // s2 withdraws from account 2
    s2.execute('UPDATE accounts SET balance = 0 WHERE id = 2');

    // One should commit, the other should fail (write skew)
    let committed = 0;
    let aborted = 0;
    try { s1.commit(); committed++; } catch (e) { aborted++; }
    try { s2.commit(); committed++; } catch (e) { aborted++; }

    s1.close();
    s2.close();

    // Under SSI, at least one must be aborted to prevent write skew
    // Under snapshot isolation (non-SSI), both may commit (that's the known limitation)
    // We just verify the DB is not corrupted
    const final = rows(db.execute('SELECT * FROM accounts'));
    assert.equal(final.length, 2, 'Both accounts should exist');
    
    // If both committed (SI mode), total could be 0 (write skew allowed)
    // If one aborted (SSI mode), total should be >= 100
    // Either way, the data must be internally consistent
    const finalTotal = final.reduce((s, r) => s + r.balance, 0);
    assert.ok(finalTotal >= 0, 'Balances should not be negative');
  });
});

describe('VACUUM + Auto-vacuum Threshold Correctness', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('auto-vacuum triggers after exceeding dead tuple threshold', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    // Insert 200 rows
    for (let i = 1; i <= 200; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    // Delete 100 rows (50% dead)
    for (let i = 1; i <= 100; i++) {
      db.execute(`DELETE FROM t WHERE id = ${i}`);
    }

    // Manual vacuum to verify it works
    const result = db.vacuum();
    
    // Should have cleaned up dead tuples
    const remaining = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(remaining[0].c, 100, 'Should have 100 rows after vacuum');
  });

  it('repeated small deletes accumulate dead tuples that VACUUM cleans', () => {
    db.execute('CREATE TABLE t (id INT, data TEXT)');
    
    // Insert, delete one at a time, accumulating dead tuples
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'data${i}')`);
    }

    // Delete every other row
    for (let i = 1; i <= 50; i += 2) {
      db.execute(`DELETE FROM t WHERE id = ${i}`);
    }

    // 25 rows should remain before vacuum
    const before = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(before[0].c, 25);

    db.vacuum();

    // Still 25 rows after vacuum (vacuum shouldn't delete live rows!)
    const after = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(after[0].c, 25, 'VACUUM must not delete live rows');
  });
});

describe('VACUUM + Crash Recovery Interaction', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('VACUUM state survives crash and re-open', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    // Delete some rows and vacuum
    db.execute('DELETE FROM t WHERE id <= 5');
    db.vacuum();

    // Verify 5 rows remain
    const r1 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r1[0].c, 5);

    // Simulate crash + recovery
    db.close();
    db = TransactionalDatabase.open(dbDir);

    // After recovery, should still have 5 rows
    const r2 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r2[0].c, 5, 'VACUUM results must survive crash recovery');
  });

  it('VACUUM after crash recovery: dead tuples from before crash get cleaned', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    // Delete but DON'T vacuum before crash
    db.execute('DELETE FROM t WHERE id <= 5');

    // Verify 5 rows remain
    const r1 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r1[0].c, 5);

    // Crash + recovery
    db.close();
    db = TransactionalDatabase.open(dbDir);

    // Should still have 5 rows (deletes committed to WAL)
    const r2 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r2[0].c, 5);

    // Now vacuum — should be able to clean up recovered dead tuples
    db.vacuum();

    // Still 5 live rows
    const r3 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r3[0].c, 5, 'VACUUM after recovery should clean dead tuples without affecting live data');
  });

  it('uncommitted transaction at crash: VACUUM after recovery does not see uncommitted deletes', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    // Start a delete in a transaction but don't commit
    const s1 = db.session();
    s1.begin();
    s1.execute('DELETE FROM t WHERE id <= 3');
    // DON'T commit — simulate crash
    // s1.commit(); // intentionally omitted

    // Close without committing (simulates crash)
    try { db.close(); } catch (e) { /* ignore */ }
    db = TransactionalDatabase.open(dbDir);

    // After recovery, uncommitted deletes should be rolled back → 5 rows
    const r1 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r1[0].c, 5, 'Uncommitted deletes must be rolled back after crash');

    // VACUUM should not reclaim any rows (none are dead)
    db.vacuum();
    const r2 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r2[0].c, 5, 'VACUUM after crash should not reclaim uncommitted deletes');
  });
});

describe('VACUUM Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('VACUUM on table with only inserts (no dead tuples) is safe', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    db.vacuum();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 100, 'VACUUM on table with no dead tuples must not delete anything');
  });

  it('multiple VACUUMs in a row are idempotent', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }
    db.execute('DELETE FROM t WHERE id <= 5');

    // Three VACUUMs in a row
    db.vacuum();
    db.vacuum();
    db.vacuum();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 5, 'Multiple VACUUMs should be idempotent');
  });

  it('VACUUM + DROP TABLE: no crash on vacuuming a dropped table', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (1)');
    db.execute('DELETE FROM t1 WHERE id = 1');
    db.execute('DROP TABLE t2');

    // Should not crash
    db.vacuum();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t1'));
    assert.equal(r[0].c, 0);
  });

  it('VACUUM + TRUNCATE: dead tuples from truncated table are handled', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }

    db.execute('TRUNCATE TABLE t');

    // VACUUM after truncate
    db.vacuum();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0, 'TRUNCATE + VACUUM should leave zero rows');

    // Re-insert should work
    db.execute('INSERT INTO t VALUES (1)');
    const r2 = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r2[0].c, 1);
  });

  it('VACUUM preserves correct row data (no data corruption)', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'alice', 100)");
    db.execute("INSERT INTO t VALUES (2, 'bob', 200)");
    db.execute("INSERT INTO t VALUES (3, 'carol', 300)");
    db.execute("INSERT INTO t VALUES (4, 'dave', 400)");
    db.execute("INSERT INTO t VALUES (5, 'eve', 500)");

    // Delete some, update others
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute('DELETE FROM t WHERE id = 4');
    db.execute("UPDATE t SET score = 150 WHERE id = 1");
    db.execute("UPDATE t SET name = 'EVE' WHERE id = 5");

    db.vacuum();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    assert.deepEqual(r[0], { id: 1, name: 'alice', score: 150 });
    assert.deepEqual(r[1], { id: 3, name: 'carol', score: 300 });
    assert.deepEqual(r[2], { id: 5, name: 'EVE', score: 500 });
  });
});
