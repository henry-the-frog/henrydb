import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Transaction Tests (2026-04-19)', () => {
  it('BEGIN + COMMIT persists changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 20 WHERE id = 1');
    db.execute('COMMIT');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 20);
  });

  it('ROLLBACK reverts changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 999 WHERE id = 1');
    db.execute('ROLLBACK');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 10);
  });

  it('SAVEPOINT + ROLLBACK TO SAVEPOINT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 20 WHERE id = 1');
    db.execute('SAVEPOINT sp1');
    db.execute('UPDATE t SET val = 30 WHERE id = 1');
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    db.execute('COMMIT');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 20);
  });

  it('multiple statements in transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000), (2, 500)');
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 2');
    db.execute('COMMIT');
    const r = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.equal(r.rows[0].balance, 800);
    assert.equal(r.rows[1].balance, 700);
  });

  it('INSERT + DELETE in transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'keep'), (2, 'delete')");
    db.execute('BEGIN');
    db.execute("INSERT INTO t VALUES (3, 'new')");
    db.execute("DELETE FROM t WHERE val = 'delete'");
    db.execute('COMMIT');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 'keep');
    assert.equal(r.rows[1].val, 'new');
  });
});
