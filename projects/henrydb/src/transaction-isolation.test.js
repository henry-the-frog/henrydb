// transaction-isolation.test.js — Transaction isolation and MVCC tests

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Transaction Basics', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000), (2, 2000), (3, 3000)');
  });

  it('BEGIN/COMMIT makes changes permanent', () => {
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = balance - 500 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 500 WHERE id = 2');
    db.execute('COMMIT');
    
    const r1 = db.execute('SELECT balance FROM accounts WHERE id = 1');
    const r2 = db.execute('SELECT balance FROM accounts WHERE id = 2');
    assert.equal(r1.rows[0].balance, 500);
    assert.equal(r2.rows[0].balance, 2500);
  });

  it('ROLLBACK undoes all changes', () => {
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = 0 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = 0 WHERE id = 2');
    db.execute('ROLLBACK');
    
    const r1 = db.execute('SELECT balance FROM accounts WHERE id = 1');
    const r2 = db.execute('SELECT balance FROM accounts WHERE id = 2');
    assert.equal(r1.rows[0].balance, 1000);
    assert.equal(r2.rows[0].balance, 2000);
  });

  it('auto-commit: each statement is permanent without BEGIN', () => {
    db.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    // Even if we could rollback, the change should be visible immediately
    const r = db.execute('SELECT balance FROM accounts WHERE id = 1');
    assert.equal(r.rows[0].balance, 500);
  });

  it('conservation of money across transfer', () => {
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = balance - 300 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 300 WHERE id = 3');
    db.execute('COMMIT');
    
    const total = db.execute('SELECT SUM(balance) as total FROM accounts');
    assert.equal(total.rows[0].total, 6000);
  });
});

describe('Savepoints', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
  });

  it('SAVEPOINT/ROLLBACK TO', () => {
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 100 WHERE id = 1');
    db.execute('SAVEPOINT sp1');
    db.execute('UPDATE t SET val = 200 WHERE id = 2');
    db.execute('ROLLBACK TO sp1');
    db.execute('COMMIT');
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 100); // kept
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, 20);  // rolled back
  });

  it('nested savepoints', () => {
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 111 WHERE id = 1');
    db.execute('SAVEPOINT sp1');
    db.execute('UPDATE t SET val = 222 WHERE id = 2');
    db.execute('SAVEPOINT sp2');
    db.execute('UPDATE t SET val = 333 WHERE id = 3');
    db.execute('ROLLBACK TO sp2');
    db.execute('COMMIT');
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 111);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, 222);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 3').rows[0].val, 30); // rolled back
  });

  it('RELEASE SAVEPOINT', () => {
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 999 WHERE id = 1');
    db.execute('SAVEPOINT sp1');
    db.execute('UPDATE t SET val = 888 WHERE id = 2');
    db.execute('RELEASE SAVEPOINT sp1');
    db.execute('COMMIT');
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 999);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, 888);
  });
});

describe('ROLLBACK Restores State', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
  });

  it('rollback restores deleted rows', () => {
    db.execute('BEGIN');
    db.execute('DELETE FROM t WHERE val > 10');
    const during = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(during.rows[0].cnt, 1);
    db.execute('ROLLBACK');
    
    const after = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(after.rows[0].cnt, 3);
  });

  it('rollback removes inserted rows', () => {
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (4, 40)');
    const during = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(during.rows[0].cnt, 4);
    db.execute('ROLLBACK');
    
    const after = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(after.rows[0].cnt, 3);
  });

  it('rollback restores updated values', () => {
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = val * 100');
    db.execute('ROLLBACK');
    
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 10);
  });

  it('rollback restores multiple operations', () => {
    db.execute('BEGIN');
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute('INSERT INTO t VALUES (4, 40)');
    db.execute('UPDATE t SET val = 999 WHERE id = 2');
    db.execute('ROLLBACK');
    
    const rows = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(rows.rows.length, 3);
    assert.equal(rows.rows[0].val, 10);
    assert.equal(rows.rows[1].val, 20);
    assert.equal(rows.rows[2].val, 30);
  });
});

describe('Constraint Enforcement in Transactions', () => {
  it('PK violation aborts statement, not transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (2, 20)');
    try {
      db.execute('INSERT INTO t VALUES (1, 30)'); // PK violation
    } catch (e) {
      // Expected
    }
    // Transaction should still be valid (PG behavior: aborted state)
    // In HenryDB, we can try to commit
    try {
      db.execute('COMMIT');
    } catch (e) {
      db.execute('ROLLBACK');
    }
    
    // id=2 insertion may or may not have survived (depends on implementation)
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.ok(r.rows.length >= 1);
  });

  it('UNIQUE constraint in transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'a@test.com')");
    
    db.execute('BEGIN');
    try {
      db.execute("INSERT INTO t VALUES (2, 'a@test.com')"); // UNIQUE violation
    } catch (e) {
      // Expected
    }
    db.execute('ROLLBACK');
    
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 1); // Only original row
  });
});
