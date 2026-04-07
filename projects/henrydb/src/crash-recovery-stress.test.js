// crash-recovery-stress.test.js — Crash recovery under concurrent workloads
// Simulates crashes mid-transaction by closing the database without proper shutdown,
// then reopening and verifying WAL recovery produces correct state.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, existsSync, closeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;

function openDb() {
  return TransactionalDatabase.open(dbDir);
}

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
}

function teardown() {
  rmSync(dbDir, { recursive: true, force: true });
}

// Helper
function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

// Simulate crash: close file descriptors without clean shutdown
// In a real crash:
// - WAL records that were fsynced survive
// - Buffer pool dirty pages may or may not have been written
// - No clean header writes happen
function crashDb(db) {
  // Force WAL flush (simulates: committed data's WAL records were fsynced)
  try { db._wal.flush(); } catch (e) { /* ok */ }
  
  // Close WAL fd without header write
  if (db._wal._fd >= 0) {
    try { closeSync(db._wal._fd); } catch(e) {}
    db._wal._fd = -1;
  }
  
  // Close DiskManager fds WITHOUT writing header (simulates crash)
  for (const dm of db._diskManagers.values()) {
    if (dm._fd >= 0) {
      try { closeSync(dm._fd); } catch(e) {}
      dm._fd = -1;
    }
  }
}

// ===== 1. COMMITTED DATA SURVIVES CRASH =====

describe('Crash Recovery: Committed Data Survives', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('committed rows survive crash and reopen', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alpha')");
    db.execute("INSERT INTO t VALUES (2, 'beta')");
    db.execute("INSERT INTO t VALUES (3, 'gamma')");
    crashDb(db);

    // Reopen — WAL recovery should restore data
    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].val, 'alpha');
    assert.equal(r[1].val, 'beta');
    assert.equal(r[2].val, 'gamma');
    db2.close();
  });

  it('explicit transaction commit survives crash', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT)');

    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    s.execute('INSERT INTO t VALUES (2)');
    s.commit();
    s.close();

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 2, 'Committed transaction data lost after crash');
    db2.close();
  });

  it('multiple committed transactions all survive', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, batch INT)');

    for (let batch = 0; batch < 5; batch++) {
      const s = db.session();
      s.begin();
      for (let i = 0; i < 10; i++) {
        s.execute(`INSERT INTO t VALUES (${batch * 10 + i}, ${batch})`);
      }
      s.commit();
      s.close();
    }

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 50, 'Not all committed batches survived');
    db2.close();
  });
});

// ===== 2. UNCOMMITTED DATA DOES NOT SURVIVE =====

describe('Crash Recovery: Uncommitted Data Lost', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('uncommitted insert is lost after crash', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');  // committed

    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (2)');  // uncommitted
    // DON'T commit — crash!
    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t'));
    assert.equal(r.length, 1, 'Uncommitted row survived crash');
    assert.equal(r[0].id, 1);
    db2.close();
  });

  it('mix of committed and uncommitted: only committed survives', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, status TEXT)');

    // Committed transaction
    const s1 = db.session();
    s1.begin();
    s1.execute("INSERT INTO t VALUES (1, 'committed')");
    s1.execute("INSERT INTO t VALUES (2, 'committed')");
    s1.commit();
    s1.close();

    // Uncommitted transaction
    const s2 = db.session();
    s2.begin();
    s2.execute("INSERT INTO t VALUES (3, 'uncommitted')");
    s2.execute("INSERT INTO t VALUES (4, 'uncommitted')");
    // Don't commit!

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Should only have committed rows');
    assert.equal(r[0].status, 'committed');
    assert.equal(r[1].status, 'committed');
    db2.close();
  });
});

// ===== 3. CRASH DURING CONCURRENT SESSIONS =====

describe('Crash Recovery: Concurrent Sessions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('crash with one committed and one uncommitted session', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, session_id INT)');

    const s1 = db.session();
    const s2 = db.session();

    // s1 commits
    s1.begin();
    s1.execute('INSERT INTO t VALUES (1, 1)');
    s1.execute('INSERT INTO t VALUES (2, 1)');
    s1.commit();

    // s2 starts but doesn't commit
    s2.begin();
    s2.execute('INSERT INTO t VALUES (3, 2)');
    s2.execute('INSERT INTO t VALUES (4, 2)');

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Only s1 committed rows should survive');
    assert.equal(r[0].session_id, 1);
    assert.equal(r[1].session_id, 1);
    db2.close();
  });

  it('crash with 5 committed + 3 uncommitted concurrent sessions', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, session_id INT, committed INT)');

    const sessions = [];
    for (let i = 0; i < 8; i++) {
      const s = db.session();
      s.begin();
      for (let j = 0; j < 5; j++) {
        s.execute(`INSERT INTO t VALUES (${i * 5 + j}, ${i}, ${i < 5 ? 1 : 0})`);
      }
      if (i < 5) {
        s.commit(); // First 5 sessions commit
      }
      // Last 3 sessions left uncommitted
      sessions.push(s);
    }

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT COUNT(*) AS c FROM t WHERE committed = 1'));
    assert.equal(r[0].c, 25, 'All 25 committed rows should survive');

    const total = rows(db2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(total[0].c, 25, 'Only committed rows should exist after recovery');
    db2.close();
  });
});

// ===== 4. DELETE + CRASH RECOVERY =====

describe('Crash Recovery: Delete Operations', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('committed DELETE persists after crash', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');

    db.execute('DELETE FROM t WHERE id = 2');
    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.deepEqual(r.map(x => x.id), [1, 3]);
    db2.close();
  });

  it('uncommitted DELETE does not persist after crash', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    const s = db.session();
    s.begin();
    s.execute('DELETE FROM t WHERE id = 1');
    // Don't commit!

    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Uncommitted delete should not persist');
    db2.close();
  });
});

// ===== 5. UPDATE + CRASH RECOVERY =====

describe('Crash Recovery: Update Operations', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('committed UPDATE persists after crash', () => {
    const db = openDb();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'before')");

    db.execute("UPDATE t SET val = 'after' WHERE id = 1");
    crashDb(db);

    const db2 = openDb();
    const r = rows(db2.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'after');
    db2.close();
  });
});

// ===== 6. REPEATED CRASH + RECOVERY =====

describe('Crash Recovery: Repeated Crashes', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('survives 5 sequential crash+recovery cycles', () => {
    for (let cycle = 0; cycle < 5; cycle++) {
      const db = openDb();
      if (cycle === 0) {
        db.execute('CREATE TABLE t (id INT, cycle INT)');
      }
      
      // Insert rows for this cycle
      for (let i = 0; i < 3; i++) {
        db.execute(`INSERT INTO t VALUES (${cycle * 3 + i}, ${cycle})`);
      }
      
      crashDb(db);
    }

    // Final reopen — all data from all cycles should be present
    const db = openDb();
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 15, 'All 15 rows from 5 crash cycles should survive');
    db.close();
  });
});

// ===== 7. BANK TRANSFER CORRECTNESS (preview for next task) =====

describe('Crash Recovery: Balance Invariant', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SUM of balances is preserved across crash during transfer', () => {
    const db = openDb();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 500)');
    db.execute('INSERT INTO accounts VALUES (2, 500)');

    // Complete a transfer (committed)
    const s = db.session();
    s.begin();
    s.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
    s.commit();
    s.close();

    crashDb(db);

    const db2 = openDb();
    const sum = rows(db2.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum[0].total, 1000, 'Balance sum invariant violated after crash');

    const r = rows(db2.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r[0].balance, 400, 'Account 1 balance wrong after recovery');
    assert.equal(r[1].balance, 600, 'Account 2 balance wrong after recovery');
    db2.close();
  });
});
