// concurrency-stress.test.js — Concurrent transaction stress tests
// Multiple interleaved sessions doing mixed operations, verify final consistency.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Concurrent Transfer Stress', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('100 transfers between 5 accounts: total balance preserved', () => {
    // Create 5 accounts with 1000 each = total 5000
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO accounts VALUES (${i}, 1000)`);
    }

    const initialTotal = 5000;
    let successfulTransfers = 0;
    let failedTransfers = 0;

    // Perform 100 transfers between random accounts
    for (let t = 0; t < 100; t++) {
      const from = Math.floor(Math.random() * 5) + 1;
      let to = Math.floor(Math.random() * 5) + 1;
      while (to === from) to = Math.floor(Math.random() * 5) + 1;
      const amount = Math.floor(Math.random() * 100) + 1;

      const s = db.session();
      s.begin();
      try {
        const fromBalance = rows(s.execute(`SELECT balance FROM accounts WHERE id = ${from}`))[0].balance;
        if (fromBalance >= amount) {
          s.execute(`UPDATE accounts SET balance = balance - ${amount} WHERE id = ${from}`);
          s.execute(`UPDATE accounts SET balance = balance + ${amount} WHERE id = ${to}`);
          s.commit();
          successfulTransfers++;
        } else {
          s.rollback();
          failedTransfers++;
        }
      } catch (e) {
        try { s.rollback(); } catch {}
        failedTransfers++;
      }
      s.close();
    }

    // Total balance must be preserved
    const finalTotal = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'))[0].total;
    assert.equal(finalTotal, initialTotal,
      `Total balance must be preserved: expected ${initialTotal}, got ${finalTotal} ` +
      `(${successfulTransfers} transfers, ${failedTransfers} failed)`);

    // No negative balances
    const negatives = rows(db.execute('SELECT id, balance FROM accounts WHERE balance < 0'));
    assert.equal(negatives.length, 0, 'No account should have negative balance');
  });
});

describe('Concurrent Insert Stress', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('200 interleaved inserts: all rows present', () => {
    db.execute('CREATE TABLE t (id INT, session_id INT, seq INT)');

    // Simulate 5 sessions each inserting 40 rows
    const sessions = [];
    for (let i = 0; i < 5; i++) {
      sessions.push(db.session());
      sessions[i].begin();
    }

    for (let seq = 0; seq < 40; seq++) {
      for (let sid = 0; sid < 5; sid++) {
        const id = sid * 40 + seq + 1;
        sessions[sid].execute(`INSERT INTO t VALUES (${id}, ${sid}, ${seq})`);
      }
    }

    // Commit all
    for (const s of sessions) {
      s.commit();
      s.close();
    }

    const count = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    assert.equal(count, 200, 'All 200 rows should be present');

    // Verify each session's rows
    for (let sid = 0; sid < 5; sid++) {
      const sRows = rows(db.execute(`SELECT COUNT(*) AS c FROM t WHERE session_id = ${sid}`));
      assert.equal(sRows[0].c, 40, `Session ${sid} should have 40 rows`);
    }
  });
});

describe('Read-Write Interleaving', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('readers see consistent snapshots during writes', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    }

    // Start a reader
    const reader = db.session();
    reader.begin();
    const initialSum = rows(reader.execute('SELECT SUM(val) AS s FROM t'))[0].s;
    assert.equal(initialSum, 5050); // 1+2+...+100

    // Perform 50 updates
    for (let i = 1; i <= 50; i++) {
      db.execute(`UPDATE t SET val = val * 2 WHERE id = ${i}`);
    }

    // Reader should still see original sum
    const snapshotSum = rows(reader.execute('SELECT SUM(val) AS s FROM t'))[0].s;
    assert.equal(snapshotSum, 5050, 'Reader snapshot should be consistent');

    reader.commit();
    reader.close();

    // New reader sees updated data
    const newSum = rows(db.execute('SELECT SUM(val) AS s FROM t'))[0].s;
    // New sum: 2*(1+2+...+50) + (51+52+...+100) = 2*1275 + 3775 = 2550 + 3775 = 6325
    assert.equal(newSum, 6325);
  });
});

describe('Rollback Stress', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('alternating commit/rollback preserves only committed data', () => {
    db.execute('CREATE TABLE t (id INT, committed INT)');
    
    for (let i = 1; i <= 100; i++) {
      const s = db.session();
      s.begin();
      s.execute(`INSERT INTO t VALUES (${i}, ${i % 2})`);
      if (i % 2 === 0) {
        s.commit(); // Even: commit
      } else {
        s.rollback(); // Odd: rollback
      }
      s.close();
    }

    // Only even rows (committed) should exist
    const count = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    assert.equal(count, 50, 'Only committed rows should exist');

    // All remaining rows should have committed=0 (even % 2)
    const committed = rows(db.execute('SELECT MIN(committed) AS mn, MAX(committed) AS mx FROM t'));
    assert.equal(committed[0].mn, 0);
    assert.equal(committed[0].mx, 0);
  });
});

describe('Mixed Operation Stress', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('mixed INSERT/UPDATE/DELETE across sessions maintains consistency', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 0)`);
    }

    // 100 random operations: insert, update, or delete
    let expectedCount = 50;
    let nextId = 51;

    for (let op = 0; op < 100; op++) {
      const r = Math.random();
      const s = db.session();
      s.begin();
      try {
        if (r < 0.3 && expectedCount < 100) {
          // Insert
          s.execute(`INSERT INTO t VALUES (${nextId}, ${op})`);
          s.commit();
          nextId++;
          expectedCount++;
        } else if (r < 0.6) {
          // Update random existing row
          const id = Math.floor(Math.random() * (nextId - 1)) + 1;
          s.execute(`UPDATE t SET val = ${op} WHERE id = ${id}`);
          s.commit();
        } else if (expectedCount > 10) {
          // Delete (but keep at least 10 rows)
          const delRows = rows(s.execute('SELECT id FROM t LIMIT 1'));
          if (delRows.length > 0) {
            s.execute(`DELETE FROM t WHERE id = ${delRows[0].id}`);
            s.commit();
            expectedCount--;
          } else {
            s.rollback();
          }
        } else {
          s.rollback();
        }
      } catch (e) {
        try { s.rollback(); } catch {}
      }
      s.close();
    }

    // Final count should match expected
    const finalCount = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    assert.equal(finalCount, expectedCount,
      `Expected ${expectedCount} rows, got ${finalCount}`);
  });
});
