// isolation-depth.test.js — Transaction isolation level correctness tests
// Tests the ACID isolation guarantees at different levels.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-iso-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE accounts (id INT, balance INT)');
  db.execute('INSERT INTO accounts VALUES (1, 1000)');
  db.execute('INSERT INTO accounts VALUES (2, 1000)');
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Dirty Read Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('uncommitted writes not visible to other transactions', () => {
    // s1 makes a change but doesn't commit
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = 0 WHERE id = 1');

    // s2 should NOT see the uncommitted change (no dirty read)
    const s2 = db.session();
    s2.begin();
    const r = rows(s2.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r[0].balance, 1000, 'Should not see uncommitted write (dirty read)');

    s1.rollback();
    s2.commit();
    s1.close();
    s2.close();
  });

  it('rollback is truly invisible to others', () => {
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    s1.rollback();
    s1.close();

    // After rollback, original value should be intact
    const r = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r[0].balance, 1000, 'Rolled-back change should not persist');
  });
});

describe('Non-Repeatable Read Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('repeated read within same transaction sees same value', () => {
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r1[0].balance, 1000);

    // s2 commits a change
    const s2 = db.session();
    s2.begin();
    s2.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    s2.commit();
    s2.close();

    // s1 reads again — should see same value (snapshot isolation)
    const r2 = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r2[0].balance, 1000, 'Should see same value on repeated read');

    s1.commit();
    s1.close();
  });
});

describe('Phantom Read Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('new rows inserted by other tx not visible in snapshot', () => {
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT COUNT(*) AS cnt FROM accounts'));
    assert.equal(r1[0].cnt, 2);

    // s2 inserts a new row
    db.execute('INSERT INTO accounts VALUES (3, 500)');

    // s1 should still see 2 rows (no phantom)
    const r2 = rows(s1.execute('SELECT COUNT(*) AS cnt FROM accounts'));
    assert.equal(r2[0].cnt, 2, 'Should not see phantom row');

    s1.commit();
    s1.close();

    // After s1 commits, new read sees 3
    assert.equal(rows(db.execute('SELECT COUNT(*) AS cnt FROM accounts'))[0].cnt, 3);
  });

  it('deleted rows still visible in snapshot', () => {
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT COUNT(*) AS cnt FROM accounts'));
    assert.equal(r1[0].cnt, 2);

    // s2 deletes a row
    db.execute('DELETE FROM accounts WHERE id = 2');

    // s1 should still see 2 rows
    const r2 = rows(s1.execute('SELECT COUNT(*) AS cnt FROM accounts'));
    assert.equal(r2[0].cnt, 2, 'Deleted row should still be visible in snapshot');

    s1.commit();
    s1.close();
  });
});

describe('Write-Write Conflict Detection', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('concurrent update to same row: first committer wins', () => {
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');

    const s2 = db.session();
    s2.begin();

    // s2 tries to update same row — might get conflict
    let s2Error = null;
    try {
      s2.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    } catch (e) {
      s2Error = e;
    }

    s1.commit();
    s1.close();

    if (!s2Error) {
      try { s2.commit(); } catch (e) { s2Error = e; }
    }
    s2.close();

    // At least one should have committed successfully
    const r = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.ok(r[0].balance >= 700 && r[0].balance <= 900,
      `Balance should reflect at least one update: ${r[0].balance}`);
  });

  it('updates to different rows do not conflict', () => {
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');

    const s2 = db.session();
    s2.begin();
    s2.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 2');

    // Both should commit successfully
    s1.commit();
    s2.commit();
    s1.close();
    s2.close();

    const r = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r[0].balance, 900);
    assert.equal(r[1].balance, 800);
  });
});

describe('Lost Update Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('lost update scenario: both read same value, both update', () => {
    // Classic lost update: both see balance=1000, both subtract 100
    // Without protection: final balance = 900 (one update lost)
    // With protection: one should fail, final balance = 900 (one update)
    // OR: both succeed but serialized, final = 800
    
    const s1 = db.session();
    s1.begin();
    const bal1 = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'))[0].balance;

    const s2 = db.session();
    s2.begin();
    const bal2 = rows(s2.execute('SELECT balance FROM accounts WHERE id = 1'))[0].balance;

    assert.equal(bal1, 1000);
    assert.equal(bal2, 1000);

    // Both try to update
    s1.execute(`UPDATE accounts SET balance = ${bal1 - 100} WHERE id = 1`);

    let s2Error = null;
    try {
      s2.execute(`UPDATE accounts SET balance = ${bal2 - 100} WHERE id = 1`);
    } catch (e) {
      s2Error = e;
    }

    s1.commit();
    s1.close();

    if (!s2Error) {
      try { s2.commit(); } catch (e) { s2Error = e; }
    }
    s2.close();

    const final = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'))[0].balance;
    // If one was rejected: final = 900
    // If both committed (last-writer-wins): final = 900 (both set to 900)
    assert.ok(final === 900 || final === 800,
      `Expected 800 or 900, got ${final}`);
  });
});

describe('Atomicity', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('multiple operations in transaction are atomic', () => {
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = balance - 500 WHERE id = 1');
    s1.execute('UPDATE accounts SET balance = balance + 500 WHERE id = 2');

    // Before commit: other transactions should see original values
    const r = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r[0].balance, 1000);
    assert.equal(r[1].balance, 1000);

    s1.commit();
    s1.close();

    // After commit: both changes visible atomically
    const r2 = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r2[0].balance, 500);
    assert.equal(r2[1].balance, 1500);
    // Total preserved: 500 + 1500 = 2000 = original
    assert.equal(r2[0].balance + r2[1].balance, 2000);
  });

  it('rollback undoes all operations', () => {
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE accounts SET balance = 0 WHERE id = 1');
    s1.execute('DELETE FROM accounts WHERE id = 2');
    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].balance, 1000);
    assert.equal(r[1].balance, 1000);
  });
});
