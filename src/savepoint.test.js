// savepoint.test.js — Tests for SAVEPOINT/RELEASE/ROLLBACK TO
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SAVEPOINT', () => {
  it('basic savepoint and rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (2, 'b')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    
    db.execute('COMMIT');
  });

  it('rollback then continue', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    
    // Can continue inserting after rollback
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute('COMMIT');
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 3);
  });

  it('nested savepoints', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    
    db.execute('SAVEPOINT sp2');
    db.execute('INSERT INTO t VALUES (3)');
    
    // Rollback to sp1 — should undo sp2 changes too
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    
    // sp2 should be gone
    assert.throws(() => db.execute('ROLLBACK TO SAVEPOINT sp2'), /does not exist/);
    
    db.execute('COMMIT');
  });

  it('RELEASE SAVEPOINT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('RELEASE SAVEPOINT sp1');
    
    // Can't rollback to released savepoint
    assert.throws(() => db.execute('ROLLBACK TO SAVEPOINT sp1'), /does not exist/);
    
    // Data should still be there
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    db.execute('COMMIT');
  });

  it('savepoint outside transaction throws', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    assert.throws(() => db.execute('SAVEPOINT sp1'), /within a transaction/);
  });

  it('savepoint with index preservation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute('CREATE INDEX idx_age ON t (age)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 30)");
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (2, 'Bob', 25)");
    
    // Index should find Bob
    assert.equal(db.execute('SELECT * FROM t WHERE age = 25').rows.length, 1);
    
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    
    // Index should NOT find Bob anymore
    assert.equal(db.execute('SELECT * FROM t WHERE age = 25').rows.length, 0);
    
    // Index should still find Alice
    assert.equal(db.execute('SELECT * FROM t WHERE age = 30').rows.length, 1);
    
    db.execute('COMMIT');
  });

  it('savepoint with UPDATE rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute("UPDATE t SET val = 'modified' WHERE id = 1");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'modified');
    
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'original');
    
    db.execute('COMMIT');
  });

  it('savepoint with DELETE rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    
    db.execute('BEGIN');
    db.execute('SAVEPOINT sp1');
    db.execute('DELETE FROM t WHERE id = 2');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
    
    db.execute('COMMIT');
  });
});
