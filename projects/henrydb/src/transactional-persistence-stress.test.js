// transactional-persistence-stress.test.js
// Tests the intersection of MVCC transactions and file-backed persistence
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-txpersist-${Date.now()}-${Math.random().toString(36).slice(2)}`);

describe('Transactional + Persistence Combined', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('committed transaction data survives close/reopen', () => {
    const d = testDir(); dirs.push(d);
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    
    const s = tdb.session();
    s.begin();
    s.execute('INSERT INTO accounts VALUES (1, 1000)');
    s.execute('INSERT INTO accounts VALUES (2, 2000)');
    s.commit();
    
    tdb.close();
    
    // Reopen — committed data should be there
    tdb = TransactionalDatabase.open(d);
    const r = tdb.execute('SELECT SUM(balance) as total FROM accounts');
    assert.strictEqual(r.rows[0].total, 3000);
    tdb.close();
  });

  it('rolled-back transaction data does NOT survive close/reopen', () => {
    const d = testDir(); dirs.push(d);
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE items (id INT PRIMARY KEY, val TEXT)');
    tdb.execute("INSERT INTO items VALUES (1, 'permanent')");
    
    // Start a transaction, insert, then rollback
    const s = tdb.session();
    s.begin();
    s.execute("INSERT INTO items VALUES (2, 'temporary')");
    s.execute("INSERT INTO items VALUES (3, 'temporary')");
    s.rollback();
    
    tdb.close();
    
    // Reopen — only the committed row should be there
    tdb = TransactionalDatabase.open(d);
    const r = tdb.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(r.rows[0].cnt, 1);
    assert.strictEqual(tdb.execute('SELECT val FROM items WHERE id = 1').rows[0].val, 'permanent');
    tdb.close();
  });

  it('multiple sessions: only committed data survives', () => {
    const d = testDir(); dirs.push(d);
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE multi (id INT PRIMARY KEY, source TEXT)');
    
    // Session 1: commit
    const s1 = tdb.session();
    s1.begin();
    s1.execute("INSERT INTO multi VALUES (1, 'session1')");
    s1.execute("INSERT INTO multi VALUES (2, 'session1')");
    s1.commit();
    
    // Session 2: rollback
    const s2 = tdb.session();
    s2.begin();
    s2.execute("INSERT INTO multi VALUES (3, 'session2')");
    s2.rollback();
    
    // Auto-commit insert (no explicit transaction)
    tdb.execute("INSERT INTO multi VALUES (4, 'autocommit')");
    
    tdb.close();
    
    tdb = TransactionalDatabase.open(d);
    const r = tdb.execute('SELECT id, source FROM multi ORDER BY id');
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].source, 'session1');
    assert.strictEqual(r.rows[1].source, 'session1');
    assert.strictEqual(r.rows[2].source, 'autocommit');
    tdb.close();
  });

  it('update within transaction survives close/reopen', () => {
    const d = testDir(); dirs.push(d);
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE balances (id INT PRIMARY KEY, amount INT)');
    tdb.execute('INSERT INTO balances VALUES (1, 1000)');
    
    const s = tdb.session();
    s.begin();
    s.execute('UPDATE balances SET amount = 500 WHERE id = 1');
    // Verify within transaction
    const inTx = s.execute('SELECT amount FROM balances WHERE id = 1');
    assert.strictEqual(inTx.rows[0].amount, 500);
    s.commit();
    
    tdb.close();
    
    tdb = TransactionalDatabase.open(d);
    const after = tdb.execute('SELECT amount FROM balances WHERE id = 1');
    assert.strictEqual(after.rows[0].amount, 500);
    tdb.close();
  });

  it('multi-cycle: interleaved commits and rollbacks', () => {
    const d = testDir(); dirs.push(d);
    
    let expectedCount = 0;
    
    for (let cycle = 0; cycle < 5; cycle++) {
      const tdb = TransactionalDatabase.open(d);
      if (cycle === 0) {
        tdb.execute('CREATE TABLE cycling (id INT PRIMARY KEY, cycle INT)');
      }
      
      // Commit some
      const s1 = tdb.session();
      s1.begin();
      for (let i = 0; i < 5; i++) {
        const id = cycle * 10 + i;
        s1.execute(`INSERT INTO cycling VALUES (${id}, ${cycle})`);
      }
      s1.commit();
      expectedCount += 5;
      
      // Rollback some
      const s2 = tdb.session();
      s2.begin();
      s2.execute(`INSERT INTO cycling VALUES (${cycle * 10 + 99}, ${cycle})`);
      s2.rollback();
      // expectedCount stays the same
      
      // Verify
      const count = tdb.execute('SELECT COUNT(*) as cnt FROM cycling');
      assert.strictEqual(count.rows[0].cnt, expectedCount, `Cycle ${cycle}: expected ${expectedCount}`);
      
      tdb.close();
    }
    
    // Final verification
    const tdb = TransactionalDatabase.open(d);
    const final = tdb.execute('SELECT COUNT(*) as cnt FROM cycling');
    assert.strictEqual(final.rows[0].cnt, expectedCount);
    tdb.close();
  });

  it('savepoint rollback within persistent session', () => {
    const d = testDir(); dirs.push(d);
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE sp_test (id INT PRIMARY KEY, val TEXT)');
    
    const s = tdb.session();
    s.begin();
    s.execute("INSERT INTO sp_test VALUES (1, 'first')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO sp_test VALUES (2, 'savepointed')");
    s.execute('ROLLBACK TO SAVEPOINT sp1');
    s.execute("INSERT INTO sp_test VALUES (3, 'after_rollback')");
    s.commit();
    
    tdb.close();
    
    tdb = TransactionalDatabase.open(d);
    const r = tdb.execute('SELECT * FROM sp_test ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].val, 'first');
    assert.strictEqual(r.rows[1].val, 'after_rollback');
    tdb.close();
  });

  it('bank transfer invariant across transactional persist cycles', () => {
    const d = testDir(); dirs.push(d);
    const numAccounts = 10;
    const initial = 1000;
    const totalExpected = numAccounts * initial;
    
    let tdb = TransactionalDatabase.open(d);
    tdb.execute('CREATE TABLE bank (id INT PRIMARY KEY, balance INT)');
    for (let i = 0; i < numAccounts; i++) {
      tdb.execute(`INSERT INTO bank VALUES (${i}, ${initial})`);
    }
    tdb.close();
    
    for (let cycle = 0; cycle < 5; cycle++) {
      tdb = TransactionalDatabase.open(d);
      
      const s = tdb.session();
      s.begin();
      
      // Transfer
      const from = cycle % numAccounts;
      const to = (from + 3) % numAccounts;
      const fromBal = s.execute(`SELECT balance FROM bank WHERE id = ${from}`);
      if (fromBal.rows[0].balance >= 100) {
        s.execute(`UPDATE bank SET balance = balance - 100 WHERE id = ${from}`);
        s.execute(`UPDATE bank SET balance = balance + 100 WHERE id = ${to}`);
      }
      s.commit();
      
      // Verify invariant
      const sum = tdb.execute('SELECT SUM(balance) as total FROM bank');
      assert.strictEqual(sum.rows[0].total, totalExpected, `Cycle ${cycle}`);
      
      tdb.close();
    }
    
    // Final verification
    tdb = TransactionalDatabase.open(d);
    const finalSum = tdb.execute('SELECT SUM(balance) as total FROM bank');
    assert.strictEqual(finalSum.rows[0].total, totalExpected);
    tdb.close();
  });
});
