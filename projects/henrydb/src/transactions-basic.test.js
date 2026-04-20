// transactions.test.js — Transaction semantics tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Transaction Semantics', () => {
  it('BEGIN + COMMIT preserves changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute('BEGIN');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute('COMMIT');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1);
  });

  it('BEGIN + ROLLBACK discards changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'before')");
    db.execute('BEGIN');
    db.execute("INSERT INTO t VALUES (2, 'during')");
    db.execute('ROLLBACK');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1);
  });

  it('autocommit: changes persist without explicit transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1);
  });

  it('nested savepoints', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('SAVEPOINT sp2');
    db.execute('INSERT INTO t VALUES (3)');
    
    // Rollback sp2 only
    db.execute('ROLLBACK TO sp2');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2); // 1 and 2
    
    // Rollback sp1
    db.execute('ROLLBACK TO sp1');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1); // only 1
  });

  it('RELEASE SAVEPOINT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('RELEASE sp1');
    
    // Can't rollback to released savepoint
    assert.throws(() => db.execute('ROLLBACK TO sp1'));
    
    // But data is still there
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
  });
});
