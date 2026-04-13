// transaction-stress.test.js — Stress tests for HenryDB transactions
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Transaction stress tests', () => {
  
  it('basic BEGIN/COMMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('COMMIT');
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.strictEqual(r.rows[0].cnt, 2);
  });

  it('ROLLBACK discards changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute('ROLLBACK');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].val, 100);
  });

  it('SAVEPOINT and ROLLBACK TO', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('INSERT INTO t VALUES (3, 300)');
    db.execute('ROLLBACK TO sp1');
    db.execute('COMMIT');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
  });

  it('nested savepoints', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('SAVEPOINT sp2');
    db.execute('INSERT INTO t VALUES (3)');
    db.execute('ROLLBACK TO sp2');
    db.execute('COMMIT');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 2); // 1 and 2 (3 was rolled back)
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2]);
  });

  it('RELEASE savepoint', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('RELEASE sp1');
    db.execute('COMMIT');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
  });

  it('ROLLBACK after error preserves pre-transaction state', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 999 WHERE id = 1');
    try {
      db.execute('INSERT INTO nonexistent VALUES (1)'); // Should error
    } catch (e) {
      // Expected
    }
    db.execute('ROLLBACK');
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.strictEqual(r.rows[0].val, 100); // Should be original value
  });

  it('multiple transactions in sequence', () => {
    const db = new Database();
    db.execute('CREATE TABLE counter (id INT, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');
    
    for (let i = 0; i < 10; i++) {
      db.execute('BEGIN');
      db.execute(`UPDATE counter SET val = val + 1 WHERE id = 1`);
      db.execute('COMMIT');
    }
    
    const r = db.execute('SELECT val FROM counter WHERE id = 1');
    assert.strictEqual(r.rows[0].val, 10);
  });

  it('transaction with UPDATE and SELECT interleaved', () => {
    const db = new Database();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 500)');
    
    db.execute('BEGIN');
    // Transfer 200 from account 1 to account 2
    db.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 2');
    
    // Mid-transaction reads should see the updated values
    const mid = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(mid.rows[0].balance, 800);
    assert.strictEqual(mid.rows[1].balance, 700);
    
    db.execute('COMMIT');
    
    // Post-commit should be the same
    const post = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(post.rows[0].balance, 800);
    assert.strictEqual(post.rows[1].balance, 700);
    // Total balance preserved
    assert.strictEqual(post.rows[0].balance + post.rows[1].balance, 1500);
  });

  it('ROLLBACK with DELETE and INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    db.execute("INSERT INTO items VALUES (1, 'apple')");
    db.execute("INSERT INTO items VALUES (2, 'banana')");
    db.execute("INSERT INTO items VALUES (3, 'cherry')");
    
    db.execute('BEGIN');
    db.execute('DELETE FROM items WHERE id = 2');
    db.execute("INSERT INTO items VALUES (4, 'date')");
    db.execute("UPDATE items SET name = 'APPLE' WHERE id = 1");
    
    // Verify mid-transaction state
    const mid = db.execute('SELECT * FROM items ORDER BY id');
    assert.strictEqual(mid.rows.length, 3); // 1, 3, 4
    
    db.execute('ROLLBACK');
    
    const post = db.execute('SELECT * FROM items ORDER BY id');
    assert.strictEqual(post.rows.length, 3); // 1, 2, 3
    assert.strictEqual(post.rows[0].name, 'apple'); // Not APPLE
    assert.strictEqual(post.rows[1].name, 'banana'); // Not deleted
  });

  it('transaction atomicity: partial failure', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (2, 200)');
    try {
      // This should fail if PRIMARY KEY constraint exists
      db.execute('INSERT INTO t VALUES (1, 300)'); // Duplicate PK
    } catch (e) {
      // Expected — constraint violation
    }
    db.execute('ROLLBACK');
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 100);
  });

  it('savepoint after partial work', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    
    db.execute('BEGIN');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute('SAVEPOINT mid');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute("INSERT INTO t VALUES (4, 'd')");
    db.execute("INSERT INTO t VALUES (5, 'e')");
    db.execute('ROLLBACK TO mid');
    // Only rows 1, 2 should remain
    db.execute("INSERT INTO t VALUES (6, 'f')");
    db.execute('COMMIT');
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows.length, 3); // 1, 2, 6
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 6]);
  });

  it('large transaction: 500 inserts then rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT, data TEXT)');
    
    db.execute('BEGIN');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, 'row ${i}')`);
    }
    // Verify all 500 exist
    const mid = db.execute('SELECT COUNT(*) as cnt FROM big');
    assert.strictEqual(mid.rows[0].cnt, 500);
    
    db.execute('ROLLBACK');
    
    const post = db.execute('SELECT COUNT(*) as cnt FROM big');
    assert.strictEqual(post.rows[0].cnt, 0);
  });

  it('DDL within transaction (CREATE TABLE)', () => {
    const db = new Database();
    db.execute('BEGIN');
    db.execute('CREATE TABLE temp (id INT)');
    db.execute('INSERT INTO temp VALUES (1)');
    db.execute('COMMIT');
    
    const r = db.execute('SELECT * FROM temp');
    assert.strictEqual(r.rows.length, 1);
  });

  it('rapid BEGIN/COMMIT cycles', () => {
    const db = new Database();
    db.execute('CREATE TABLE counter (val INT)');
    db.execute('INSERT INTO counter VALUES (0)');
    
    for (let i = 0; i < 100; i++) {
      db.execute('BEGIN');
      db.execute('UPDATE counter SET val = val + 1');
      db.execute('COMMIT');
    }
    
    const r = db.execute('SELECT val FROM counter');
    assert.strictEqual(r.rows[0].val, 100);
  });
});
