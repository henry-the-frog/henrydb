// bank-transfer.test.js — Bank transfer invariant benchmark
// The gold standard test for transactional correctness:
// N accounts with known initial balances, concurrent transfers,
// verify that the total sum is always conserved.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, closeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setup(numAccounts = 10, initialBalance = 1000) {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-bank-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE accounts (id INT, balance INT)');
  for (let i = 1; i <= numAccounts; i++) {
    db.execute(`INSERT INTO accounts VALUES (${i}, ${initialBalance})`);
  }
  return numAccounts * initialBalance;
}

function teardown() {
  try { db.close(); } catch (e) { /* ignore */ }
  rmSync(dbDir, { recursive: true, force: true });
}

function checkInvariant(expectedSum, label) {
  const r = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'));
  assert.equal(r[0].total, expectedSum, `Balance invariant violated: ${label}`);
  
  // Also check no negative balances
  const neg = rows(db.execute('SELECT * FROM accounts WHERE balance < 0'));
  assert.equal(neg.length, 0, `Negative balance found: ${label}`);
}

// ===== 1. SEQUENTIAL TRANSFERS =====

describe('Bank Transfer: Sequential', () => {
  afterEach(teardown);

  it('100 sequential transfers preserve sum', () => {
    const expectedSum = setup(10, 1000);

    for (let i = 0; i < 100; i++) {
      const from = (i % 10) + 1;
      const to = ((i + 3) % 10) + 1;
      if (from === to) continue;
      const amount = 10;

      const s = db.session();
      s.begin();
      const fromRow = rows(s.execute(`SELECT balance FROM accounts WHERE id = ${from}`));
      if (fromRow[0].balance >= amount) {
        s.execute(`UPDATE accounts SET balance = balance - ${amount} WHERE id = ${from}`);
        s.execute(`UPDATE accounts SET balance = balance + ${amount} WHERE id = ${to}`);
      }
      s.commit();
      s.close();
    }

    checkInvariant(expectedSum, 'after 100 sequential transfers');
  });

  it('transfers that would overdraw are skipped', () => {
    const expectedSum = setup(3, 100);

    // Transfer more than available — should be skipped by balance check
    for (let i = 0; i < 20; i++) {
      const s = db.session();
      s.begin();
      const fromRow = rows(s.execute('SELECT balance FROM accounts WHERE id = 1'));
      if (fromRow[0].balance >= 60) {
        s.execute('UPDATE accounts SET balance = balance - 60 WHERE id = 1');
        s.execute('UPDATE accounts SET balance = balance + 60 WHERE id = 2');
      }
      s.commit();
      s.close();
    }

    checkInvariant(expectedSum, 'after overdraw-protected transfers');
  });
});

// ===== 2. INTERLEAVED CONCURRENT TRANSFERS =====

describe('Bank Transfer: Concurrent Interleaving', () => {
  afterEach(teardown);

  it('two concurrent transfers on disjoint accounts both succeed', () => {
    const expectedSum = setup(4, 1000);

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    // s1: transfer 100 from account 1 → 2
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s1.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');

    // s2: transfer 200 from account 3 → 4
    s2.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 3');
    s2.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 4');

    s1.commit();
    s2.commit();

    s1.close();
    s2.close();

    checkInvariant(expectedSum, 'disjoint concurrent transfers');

    const balances = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(balances[0].balance, 900);  // account 1
    assert.equal(balances[1].balance, 1100); // account 2
    assert.equal(balances[2].balance, 800);  // account 3
    assert.equal(balances[3].balance, 1200); // account 4
  });

  it('interleaved reads and writes maintain snapshot isolation', () => {
    const expectedSum = setup(4, 1000);

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    // s1 reads account 1 balance
    const s1Read = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(s1Read[0].balance, 1000);

    // s2 does a transfer from account 1 → 2 and commits
    s2.begin();
    s2.execute('UPDATE accounts SET balance = balance - 300 WHERE id = 1');
    s2.execute('UPDATE accounts SET balance = balance + 300 WHERE id = 2');
    s2.commit();

    // s1 still sees old balance (snapshot isolation)
    const s1Read2 = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(s1Read2[0].balance, 1000, 'Snapshot isolation broken');

    // s1's SUM should still be original
    const s1Sum = rows(s1.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(s1Sum[0].total, expectedSum);

    s1.commit();
    s1.close();
    s2.close();

    checkInvariant(expectedSum, 'after interleaved read/write');
  });
});

// ===== 3. ROLLBACK PRESERVES INVARIANT =====

describe('Bank Transfer: Rollback Safety', () => {
  afterEach(teardown);

  it('half-completed transfer rolled back preserves sum', () => {
    const expectedSum = setup(2, 500);

    const s = db.session();
    s.begin();
    s.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    // Oops, something went wrong — rollback
    s.rollback();
    s.close();

    checkInvariant(expectedSum, 'after rollback of half-transfer');

    // Account 1 should still have 500
    const r = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r[0].balance, 500);
  });

  it('alternating commit and rollback transfers', () => {
    const expectedSum = setup(4, 1000);
    
    let committedTransfers = 0;
    for (let i = 0; i < 30; i++) {
      const from = (i % 4) + 1;
      const to = ((i + 1) % 4) + 1;
      if (from === to) continue;

      const s = db.session();
      s.begin();
      const fromRow = rows(s.execute(`SELECT balance FROM accounts WHERE id = ${from}`));
      if (fromRow[0].balance >= 50) {
        s.execute(`UPDATE accounts SET balance = balance - 50 WHERE id = ${from}`);
        s.execute(`UPDATE accounts SET balance = balance + 50 WHERE id = ${to}`);
      }

      if (i % 3 === 0) {
        s.rollback(); // Every 3rd transfer is rolled back
      } else {
        s.commit();
        committedTransfers++;
      }
      s.close();
    }

    checkInvariant(expectedSum, `after ${committedTransfers} committed + rollbacks`);
  });
});

// ===== 4. CRASH RECOVERY PRESERVES INVARIANT =====

describe('Bank Transfer: Crash Recovery', () => {
  afterEach(teardown);

  it('committed transfers survive crash', () => {
    const expectedSum = setup(4, 1000);

    // Do several transfers
    for (let i = 0; i < 10; i++) {
      const from = (i % 4) + 1;
      const to = ((i + 2) % 4) + 1;
      if (from === to) continue;

      const s = db.session();
      s.begin();
      s.execute(`UPDATE accounts SET balance = balance - 25 WHERE id = ${from}`);
      s.execute(`UPDATE accounts SET balance = balance + 25 WHERE id = ${to}`);
      s.commit();
      s.close();
    }

    checkInvariant(expectedSum, 'before crash');

    // Crash
    try { db._wal.flush(); } catch(e) {}
    if (db._wal._fd >= 0) { closeSync(db._wal._fd); db._wal._fd = -1; }
    for (const dm of db._diskManagers.values()) {
      if (dm._fd >= 0) { closeSync(dm._fd); dm._fd = -1; }
    }

    // Recovery
    db = TransactionalDatabase.open(dbDir);
    checkInvariant(expectedSum, 'after crash recovery');
  });

  it('in-flight transfer at crash time does not corrupt sum', () => {
    const expectedSum = setup(2, 500);

    // Committed transfer
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s1.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
    s1.commit();
    s1.close();

    // In-flight transfer (not committed)
    const s2 = db.session();
    s2.begin();
    s2.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    // Don't commit — crash!

    try { db._wal.flush(); } catch(e) {}
    if (db._wal._fd >= 0) { closeSync(db._wal._fd); db._wal._fd = -1; }
    for (const dm of db._diskManagers.values()) {
      if (dm._fd >= 0) { closeSync(dm._fd); dm._fd = -1; }
    }

    // Recovery — in-flight transfer should be lost
    db = TransactionalDatabase.open(dbDir);
    checkInvariant(expectedSum, 'after crash with in-flight transfer');

    // Account 1 should be 400 (500 - 100 from committed), not 200
    const r = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r[0].balance, 400, 'Committed transfer should survive');
  });
});

// ===== 5. STRESS: MANY SMALL TRANSFERS =====

describe('Bank Transfer: Stress', () => {
  afterEach(teardown);

  it('200 random transfers across 20 accounts', () => {
    const expectedSum = setup(20, 500);

    for (let i = 0; i < 200; i++) {
      const from = (i % 20) + 1;
      let to = ((i * 7 + 3) % 20) + 1;
      if (to === from) to = (to % 20) + 1;
      const amount = (i % 50) + 1;

      const s = db.session();
      s.begin();
      const fromRow = rows(s.execute(`SELECT balance FROM accounts WHERE id = ${from}`));
      if (fromRow[0].balance >= amount) {
        s.execute(`UPDATE accounts SET balance = balance - ${amount} WHERE id = ${from}`);
        s.execute(`UPDATE accounts SET balance = balance + ${amount} WHERE id = ${to}`);
      }
      s.commit();
      s.close();
    }

    checkInvariant(expectedSum, 'after 200 random transfers');
  });
});
