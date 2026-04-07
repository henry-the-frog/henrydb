// ssi-integration.test.js — End-to-end SSI tests via TransactionalDatabase SQL
// Proves that SERIALIZABLE isolation prevents write skew at the SQL level

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setupSerializable() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ssi-'));
  db = TransactionalDatabase.open(dbDir, { isolationLevel: 'serializable' });
}

function setupSnapshot() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-si-'));
  db = TransactionalDatabase.open(dbDir, { isolationLevel: 'snapshot' });
}

function teardown() {
  try { db.close(); } catch (e) { /* ignore */ }
  rmSync(dbDir, { recursive: true, force: true });
}

// ===== THE CLASSIC WRITE SKEW: DOCTOR ON-CALL =====

describe('SSI SQL Integration: Write Skew Prevention', () => {
  afterEach(teardown);

  it('SNAPSHOT ISOLATION allows write skew (control test)', () => {
    setupSnapshot();
    
    db.execute('CREATE TABLE doctors (name TEXT, oncall INT)');
    db.execute("INSERT INTO doctors VALUES ('Alice', 1)");
    db.execute("INSERT INTO doctors VALUES ('Bob', 1)");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    // Both read: 2 doctors on call
    const s1Count = rows(s1.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    const s2Count = rows(s2.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(s1Count[0].c, 2);
    assert.equal(s2Count[0].c, 2);

    // Each takes one doctor off-call
    s1.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Alice'");
    s1.commit();

    s2.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Bob'");
    s2.commit();  // SI allows this!

    // Result: nobody on call — write skew anomaly!
    const final = rows(db.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(final[0].c, 0, 'SI allows write skew');

    s1.close();
    s2.close();
  });

  it('SERIALIZABLE isolation PREVENTS write skew', () => {
    setupSerializable();
    
    db.execute('CREATE TABLE doctors (name TEXT, oncall INT)');
    db.execute("INSERT INTO doctors VALUES ('Alice', 1)");
    db.execute("INSERT INTO doctors VALUES ('Bob', 1)");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    // Both read: 2 doctors on call
    const s1Count = rows(s1.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    const s2Count = rows(s2.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(s1Count[0].c, 2);
    assert.equal(s2Count[0].c, 2);

    // s1 takes Alice off-call and commits
    s1.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Alice'");
    s1.commit();

    // s2 takes Bob off-call
    s2.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Bob'");
    
    // s2's commit should FAIL — serialization anomaly detected!
    assert.throws(
      () => s2.commit(),
      /serialization/i,
      'SSI should prevent write skew'
    );

    // After rollback, at least one doctor is still on call
    const final = rows(db.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.ok(final[0].c >= 1, 'At least one doctor should be on call');

    s1.close();
    s2.close();
  });
});

// ===== SERIALIZABLE STILL ALLOWS NON-CONFLICTING =====

describe('SSI SQL Integration: Allowed Operations', () => {
  afterEach(teardown);

  it('disjoint transactions commit under SERIALIZABLE', () => {
    setupSerializable();
    
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute("INSERT INTO t VALUES (4, 'd')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    // s1 works on rows 1-2
    s1.execute("UPDATE t SET val = 'x' WHERE id = 1");
    
    // s2 works on rows 3-4
    s2.execute("UPDATE t SET val = 'y' WHERE id = 3");

    s1.commit();
    s2.commit(); // Should succeed — disjoint sets

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r[0].val, 'x');
    assert.equal(r[2].val, 'y');

    s1.close();
    s2.close();
  });

  it('sequential transactions always succeed under SERIALIZABLE', () => {
    setupSerializable();
    
    db.execute('CREATE TABLE counter (id INT, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');

    // 20 sequential increment transactions
    for (let i = 0; i < 20; i++) {
      const s = db.session();
      s.begin();
      const r = rows(s.execute('SELECT val FROM counter WHERE id = 1'));
      s.execute(`UPDATE counter SET val = ${r[0].val + 1} WHERE id = 1`);
      s.commit();
      s.close();
    }

    const result = rows(db.execute('SELECT val FROM counter WHERE id = 1'));
    assert.equal(result[0].val, 20);
  });

  it('read-only transactions never aborted under SERIALIZABLE', () => {
    setupSerializable();
    
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');

    const reader = db.session();
    const writer = db.session();

    reader.begin();
    rows(reader.execute('SELECT * FROM t')); // Read

    writer.begin();
    writer.execute('UPDATE t SET val = 200 WHERE id = 1');
    writer.commit();

    // Reader should still be able to commit (read-only)
    rows(reader.execute('SELECT * FROM t')); // Another read
    reader.commit(); // Should succeed

    reader.close();
    writer.close();
  });
});

// ===== BANK TRANSFER UNDER SERIALIZABLE =====

describe('SSI SQL Integration: Bank Transfer', () => {
  afterEach(teardown);

  it('concurrent bank transfers on disjoint accounts under SERIALIZABLE', () => {
    setupSerializable();
    
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 1000)');
    db.execute('INSERT INTO accounts VALUES (3, 1000)');
    db.execute('INSERT INTO accounts VALUES (4, 1000)');

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    // s1: transfer 100 from account 1 → 2
    s1.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    s1.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');

    // s2: transfer 200 from account 3 → 4
    s2.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 3');
    s2.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 4');

    s1.commit();
    s2.commit();

    const sum = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum[0].total, 4000, 'Sum preserved');

    s1.close();
    s2.close();
  });
});
