// select-for-update.test.js — Tests for SELECT FOR UPDATE / FOR SHARE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';

function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'henrydb-fu-'));
  const db = TransactionalDatabase.open(dir);
  db.execute("CREATE TABLE accounts (id INT, balance INT)");
  db.execute("INSERT INTO accounts VALUES (1, 1000)");
  db.execute("INSERT INTO accounts VALUES (2, 2000)");
  return { db, dir };
}

describe('SELECT FOR UPDATE', () => {

  it('basic SELECT FOR UPDATE returns rows', () => {
    const { db, dir } = freshDb();
    const s = db.session();
    s.begin();
    
    const result = s.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].balance, 1000);
    
    s.commit();
    s.close();
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('FOR UPDATE prevents concurrent modification', () => {
    const { db, dir } = freshDb();
    
    // Session 1: SELECT FOR UPDATE
    const s1 = db.session();
    s1.begin();
    s1.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE");
    
    // Session 2: try to UPDATE the same row
    const s2 = db.session();
    s2.begin();
    
    let conflictError = null;
    try {
      s2.execute("UPDATE accounts SET balance = 500 WHERE id = 1");
      s2.commit();
    } catch (e) {
      conflictError = e;
    }
    
    console.log('Conflict:', conflictError?.message);
    assert.ok(conflictError, 'Should get write-write conflict');
    assert.ok(conflictError.message.includes('locked') || conflictError.message.includes('conflict'), 
      'Error should mention lock conflict');
    
    s1.commit();
    s1.close();
    if (s2._tx) s2.rollback();
    s2.close();
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('FOR UPDATE then UPDATE in same transaction', () => {
    const { db, dir } = freshDb();
    const s = db.session();
    s.begin();
    
    // Lock then modify
    s.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE");
    s.execute("UPDATE accounts SET balance = 500 WHERE id = 1");
    s.commit();
    
    const result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert.equal(result.rows[0].balance, 500);
    
    s.close();
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('SELECT FOR SHARE returns rows', () => {
    const { db, dir } = freshDb();
    const s = db.session();
    s.begin();
    
    const result = s.execute("SELECT * FROM accounts FOR SHARE");
    assert.equal(result.rows.length, 2);
    
    s.commit();
    s.close();
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('FOR UPDATE lock released on commit', () => {
    const { db, dir } = freshDb();
    
    // S1: lock then commit
    const s1 = db.session();
    s1.begin();
    s1.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE");
    s1.commit();
    s1.close();
    
    // S2: should now be able to update
    const s2 = db.session();
    s2.begin();
    s2.execute("UPDATE accounts SET balance = 777 WHERE id = 1");
    s2.commit();
    s2.close();
    
    const result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert.equal(result.rows[0].balance, 777);
    
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('FOR UPDATE lock released on rollback', () => {
    const { db, dir } = freshDb();
    
    const s1 = db.session();
    s1.begin();
    s1.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE");
    s1.rollback();
    s1.close();
    
    const s2 = db.session();
    s2.begin();
    s2.execute("UPDATE accounts SET balance = 888 WHERE id = 1");
    s2.commit();
    s2.close();
    
    const result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert.equal(result.rows[0].balance, 888);
    
    db.close();
    rmSync(dir, { recursive: true });
  });
});
