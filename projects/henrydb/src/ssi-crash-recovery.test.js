// ssi-crash-recovery.test.js — SSI + crash recovery integration
// Verifies that serializable isolation works correctly across crash+recovery cycles

import { describe, it, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, closeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function crashDb(db) {
  try { db._wal.flush(); } catch(e) {}
  if (db._wal._fd >= 0) { closeSync(db._wal._fd); db._wal._fd = -1; }
  for (const dm of db._diskManagers.values()) {
    if (dm._fd >= 0) { closeSync(dm._fd); dm._fd = -1; }
  }
}

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ssi-crash-'));
}

function teardown() {
  rmSync(dbDir, { recursive: true, force: true });
}

afterEach(teardown);

describe('SSI + Crash Recovery', () => {
  it('committed data under SSI survives crash', () => {
    setup();
    const db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alpha')");
    db.execute("INSERT INTO t VALUES (2, 'beta')");
    
    crashDb(db);
    
    const db2 = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].val, 'alpha');
    assert.equal(r[1].val, 'beta');
    db2.close();
  });

  it('SSI prevents write skew in new session after recovery', () => {
    setup();
    let db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    
    db.execute('CREATE TABLE doctors (name TEXT, oncall INT)');
    db.execute("INSERT INTO doctors VALUES ('Alice', 1)");
    db.execute("INSERT INTO doctors VALUES ('Bob', 1)");
    
    // Crash and recover
    crashDb(db);
    db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    
    // Now try the write skew scenario on the recovered database
    const s1 = db.session();
    const s2 = db.session();
    
    s1.begin();
    s2.begin();
    
    // Both read: 2 doctors on call
    const s1Count = rows(s1.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    const s2Count = rows(s2.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(s1Count[0].c, 2);
    assert.equal(s2Count[0].c, 2);
    
    // s1 takes Alice off-call
    s1.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Alice'");
    s1.commit();
    
    // s2 takes Bob off-call — should be prevented by SSI
    s2.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Bob'");
    assert.throws(
      () => s2.commit(),
      /serialization/i,
      'SSI should prevent write skew after crash recovery'
    );
    
    // At least one doctor still on call
    const final = rows(db.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.ok(final[0].c >= 1, 'At least one doctor on call');
    
    s1.close();
    s2.close();
    db.close();
  });

  it('committed transfer survives crash under SSI', () => {
    setup();
    let db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 500)');
    db.execute('INSERT INTO accounts VALUES (2, 500)');
    
    // Do a transfer
    const s = db.session();
    s.begin();
    s.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
    s.commit();
    s.close();
    
    // Crash
    crashDb(db);
    
    // Recover with SSI
    db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    
    // Sum should be preserved
    const sum = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum[0].total, 1000, 'Balance sum preserved after crash');
    
    // Individual balances should be correct
    const r = rows(db.execute('SELECT * FROM accounts ORDER BY id'));
    assert.equal(r[0].balance, 400);
    assert.equal(r[1].balance, 600);
    
    db.close();
  });

  it('multiple crash+recovery cycles preserve data integrity', () => {
    setup();
    
    for (let cycle = 0; cycle < 3; cycle++) {
      let db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
      
      if (cycle === 0) {
        db.execute('CREATE TABLE counter (id INT, val INT)');
        db.execute('INSERT INTO counter VALUES (1, 0)');
      }
      
      // Increment counter
      const s = db.session();
      s.begin();
      const r = rows(s.execute('SELECT val FROM counter WHERE id = 1'));
      const newVal = r[0].val + 1;
      s.execute(`UPDATE counter SET val = ${newVal} WHERE id = 1`);
      s.commit();
      s.close();
      
      // Crash
      crashDb(db);
    }
    
    // Final recovery — counter should be 3
    const db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
    const result = rows(db.execute('SELECT val FROM counter WHERE id = 1'));
    assert.equal(result[0].val, 3, 'Counter should be 3 after 3 crash cycles');
    db.close();
  });
});
