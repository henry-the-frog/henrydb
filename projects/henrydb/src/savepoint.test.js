// savepoint.test.js — Tests for SAVEPOINT, ROLLBACK TO, RELEASE
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Savepoints', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
  });

  it('SAVEPOINT and ROLLBACK TO restores state', () => {
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
    db.execute('ROLLBACK TO sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('ROLLBACK TO restores deleted rows', () => {
    db.execute('SAVEPOINT sp1');
    db.execute('DELETE FROM t WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    db.execute('ROLLBACK TO sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('ROLLBACK TO restores updated values', () => {
    db.execute('SAVEPOINT sp1');
    db.execute("UPDATE t SET val = 'modified' WHERE id = 1");
    db.execute('ROLLBACK TO sp1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'a');
  });

  it('nested savepoints', () => {
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute('SAVEPOINT sp2');
    db.execute("INSERT INTO t VALUES (4, 'd')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 4);
    
    db.execute('ROLLBACK TO sp2');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
    
    db.execute('ROLLBACK TO sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('RELEASE removes savepoint', () => {
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute('RELEASE sp1');
    assert.throws(() => db.execute('ROLLBACK TO sp1'), /not found/);
  });

  it('ROLLBACK TO nonexistent savepoint throws', () => {
    assert.throws(() => db.execute('ROLLBACK TO nope'), /not found/);
  });

  it('ROLLBACK TO SAVEPOINT syntax works', () => {
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });
});
