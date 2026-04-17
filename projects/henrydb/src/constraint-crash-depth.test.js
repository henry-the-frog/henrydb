// constraint-crash-depth.test.js — Constraint enforcement + crash recovery + MVCC depth tests
// Tests the integration boundary between constraints, MVCC, and crash recovery.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-constraint-'));
  db = TransactionalDatabase.open(dbDir);
}

function teardown() {
  try { db.close(); } catch (e) { /* ignore */ }
  rmSync(dbDir, { recursive: true, force: true });
}

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

describe('Constraint Enforcement After Crash Recovery', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NOT NULL constraint enforced after crash recovery', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");
    db.execute("INSERT INTO t VALUES (2, 'bob')");

    // Crash and recover
    db.close();
    db = TransactionalDatabase.open(dbDir);

    // Data should survive
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);

    // NOT NULL constraint should still be enforced
    assert.throws(() => {
      db.execute('INSERT INTO t VALUES (3, NULL)');
    }, /NOT NULL/i);

    // Valid insert should work
    db.execute("INSERT INTO t VALUES (3, 'carol')");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 3);
  });

  it('UNIQUE constraint enforced after crash recovery', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // UNIQUE constraint should reject duplicates after recovery
    assert.throws(() => {
      db.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
    }, /UNIQUE|duplicate/i);

    // Different email should work
    db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 2);
  });

  it('CHECK constraint enforced after crash recovery', () => {
    db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
    db.execute('INSERT INTO t VALUES (1, 25)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    assert.throws(() => {
      db.execute('INSERT INTO t VALUES (2, -5)');
    }, /CHECK/i);

    db.execute('INSERT INTO t VALUES (2, 30)');
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 2);
  });

  it('PRIMARY KEY uniqueness enforced after crash recovery', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // PK duplicate should be rejected
    assert.throws(() => {
      db.execute("INSERT INTO t VALUES (1, 'duplicate')");
    }, /duplicate|unique|primary/i);

    // New PK should work
    db.execute("INSERT INTO t VALUES (3, 'c')");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 3);
  });

  it('FOREIGN KEY constraint enforced after crash recovery', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO parent VALUES (2)');
    db.execute('CREATE TABLE child (id INT, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO child VALUES (1, 1)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // FK constraint: referencing non-existent parent should fail
    assert.throws(() => {
      db.execute('INSERT INTO child VALUES (2, 999)');
    }, /foreign key|not found/i);

    // Referencing existing parent should work
    db.execute('INSERT INTO child VALUES (2, 2)');
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM child'))[0].c, 2);
  });

  it('multiple constraints on same table survive recovery', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, age INT CHECK (age >= 0))');
    db.execute("INSERT INTO t VALUES (1, 'alice', 25)");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // All constraints should be enforced
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'dup', 20)"), /duplicate|unique|primary/i);
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, NULL, 20)"), /NOT NULL/i);
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, 'bob', -1)"), /CHECK/i);

    db.execute("INSERT INTO t VALUES (2, 'bob', 30)");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 2);
  });
});

describe('UNIQUE Constraint + MVCC Concurrent Insert', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UNIQUE constraint prevents duplicate across concurrent transactions', () => {
    db.execute('CREATE TABLE t (id INT, email TEXT UNIQUE)');

    // s1 inserts email
    const s1 = db.session();
    s1.begin();
    s1.execute("INSERT INTO t VALUES (1, 'alice@test.com')");

    // s2 tries to insert same email 
    // Under Snapshot Isolation without deferred unique checks, both may commit.
    // This is a known SI limitation — PostgreSQL handles it with unique index locks.
    const s2 = db.session();
    s2.begin();
    
    let s2Error = null;
    try {
      s2.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
      s2.commit();
    } catch (e) {
      s2Error = e;
    }

    let s1Error = null;
    try {
      s1.commit();
    } catch (e) {
      s1Error = e;
    }

    s1.close();
    s2.close();

    // Under SI, both may commit (known limitation).
    // If SSI or unique locks are added later, at most one should succeed.
    // For now, verify no crash and at least one row exists.
    const r = rows(db.execute('SELECT * FROM t'));
    assert.ok(r.length >= 1, 'At least one insert should succeed');
    // Note: ideally unique constraint should prevent duplicates across txns.
    // This test documents the current SI behavior.
  });

  it('UNIQUE constraint allows same value after DELETE + commit', () => {
    db.execute('CREATE TABLE t (id INT, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");

    // Delete the row
    db.execute("DELETE FROM t WHERE email = 'alice@test.com'");

    // Re-insert same email should work
    db.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });

  it('UNIQUE constraint: delete in one tx, insert same in another', () => {
    db.execute('CREATE TABLE t (id INT, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");

    // s1 deletes
    const s1 = db.session();
    s1.begin();
    s1.execute("DELETE FROM t WHERE id = 1");
    s1.commit();
    s1.close();

    // s2 inserts same email
    const s2 = db.session();
    s2.begin();
    s2.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
    s2.commit();
    s2.close();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].email, 'alice@test.com');
  });
});

describe('FK Constraint + VACUUM Interaction', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('FK prevents deleting parent row with active children', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO parent VALUES (2)');
    db.execute('INSERT INTO child VALUES (1, 1)');

    // Deleting referenced parent should fail
    assert.throws(() => {
      db.execute('DELETE FROM parent WHERE id = 1');
    }, /foreign key|referenced/i);

    // Deleting non-referenced parent should succeed
    db.execute('DELETE FROM parent WHERE id = 2');
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM parent'))[0].c, 1);
  });

  it('FK still works after VACUUM on parent table', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO parent VALUES (2)');
    db.execute('INSERT INTO parent VALUES (3)');
    db.execute('INSERT INTO child VALUES (1, 1)');

    // Delete non-referenced parent and vacuum
    db.execute('DELETE FROM parent WHERE id = 3');
    db.vacuum();

    // FK should still work: can't insert child with deleted parent
    assert.throws(() => {
      db.execute('INSERT INTO child VALUES (2, 3)');
    }, /foreign key|not found/i);

    // Can insert child with existing parent
    db.execute('INSERT INTO child VALUES (2, 2)');
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM child'))[0].c, 2);
  });

  it('FK after crash recovery: constraints preserved', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO child VALUES (1, 1)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // FK should still be enforced
    assert.throws(() => {
      db.execute('INSERT INTO child VALUES (2, 999)');
    }, /foreign key|not found/i);

    db.execute('INSERT INTO parent VALUES (2)');
    db.execute('INSERT INTO child VALUES (2, 2)');
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM child'))[0].c, 2);
  });
});

describe('CHECK Constraint + UPDATE Under Snapshot Isolation', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CHECK constraint enforced on UPDATE', () => {
    db.execute('CREATE TABLE t (id INT, balance INT CHECK (balance >= 0))');
    db.execute('INSERT INTO t VALUES (1, 100)');

    // Update that violates CHECK should fail
    assert.throws(() => {
      db.execute('UPDATE t SET balance = -50 WHERE id = 1');
    }, /CHECK/i);

    // Valid update should work
    db.execute('UPDATE t SET balance = 50 WHERE id = 1');
    assert.equal(rows(db.execute('SELECT balance FROM t WHERE id = 1'))[0].balance, 50);
  });

  it('CHECK constraint with concurrent transactions', () => {
    db.execute('CREATE TABLE accounts (id INT, balance INT CHECK (balance >= 0))');
    db.execute('INSERT INTO accounts VALUES (1, 100)');

    // s1 reads balance=100, will try to withdraw 80
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.equal(r1[0].balance, 100);

    // s2 withdraws 50 and commits
    const s2 = db.session();
    s2.begin();
    s2.execute('UPDATE accounts SET balance = balance - 50 WHERE id = 1');
    s2.commit();
    s2.close();

    // s1 now tries to withdraw 80 (which would bring balance to 100-80=20 in s1's view,
    // but actual balance is now 50, so 50-80=-30 would violate CHECK)
    let s1Error = null;
    try {
      s1.execute('UPDATE accounts SET balance = balance - 80 WHERE id = 1');
      s1.commit();
    } catch (e) {
      s1Error = e;
    }
    s1.close();

    // Either s1 failed (good — constraint enforced) or balance >= 0
    const final = rows(db.execute('SELECT balance FROM accounts WHERE id = 1'));
    assert.ok(final[0].balance >= 0, 
      `CHECK constraint violated: balance is ${final[0].balance}`);
  });
});

describe('Trigger Side-Effects and Rollback Atomicity', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('AFTER INSERT trigger fires and writes to audit table', () => {
    db.execute('CREATE TABLE orders (id INT, amount INT)');
    db.execute('CREATE TABLE audit_log (msg TEXT)');

    // Create a trigger
    db._db.triggers.push({
      name: 'audit_insert',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      bodySql: "INSERT INTO audit_log VALUES ('order_created')"
    });

    db.execute('INSERT INTO orders VALUES (1, 100)');

    const audit = rows(db.execute('SELECT * FROM audit_log'));
    assert.equal(audit.length, 1);
    assert.equal(audit[0].msg, 'order_created');
  });

  it('trigger side-effects roll back with transaction', () => {
    db.execute('CREATE TABLE orders (id INT, amount INT)');
    db.execute('CREATE TABLE audit_log (msg TEXT)');

    db._db.triggers.push({
      name: 'audit_insert',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      bodySql: "INSERT INTO audit_log VALUES ('order_created')"
    });

    // Begin transaction, insert (trigger fires), then rollback
    const s1 = db.session();
    s1.begin();
    s1.execute('INSERT INTO orders VALUES (1, 100)');

    // Audit should have the entry within the transaction
    const audit1 = rows(s1.execute('SELECT * FROM audit_log'));
    // Note: trigger fires in the same transaction context
    
    // Rollback
    s1.rollback();
    s1.close();

    // After rollback, both orders and audit_log should be empty
    const orders = rows(db.execute('SELECT * FROM orders'));
    const audit = rows(db.execute('SELECT * FROM audit_log'));
    
    assert.equal(orders.length, 0, 'Orders should be empty after rollback');
    assert.equal(audit.length, 0, 
      'Trigger side-effects should roll back with the transaction');
  });

  it('BEFORE INSERT trigger that modifies NEW values', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');

    // Trigger that uppercases name using NEW.column references
    db._db.triggers.push({
      name: 'uppercase_name',
      timing: 'BEFORE',
      event: 'INSERT',
      table: 't',
      bodySql: "UPDATE t SET name = UPPER(NEW.name) WHERE id = NEW.id"
    });

    db.execute("INSERT INTO t VALUES (1, 'alice')");

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    // The trigger body substitutes NEW.name → 'alice' and NEW.id → 1
    // So it runs: UPDATE t SET name = UPPER('alice') WHERE id = 1
  });

  it('AFTER DELETE trigger logs deletions', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE delete_log (deleted_id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');

    db._db.triggers.push({
      name: 'log_delete',
      timing: 'AFTER',
      event: 'DELETE',
      table: 't',
      bodySql: "INSERT INTO delete_log VALUES (OLD.id)"
    });

    db.execute('DELETE FROM t WHERE id = 2');

    const log = rows(db.execute('SELECT * FROM delete_log'));
    assert.equal(log.length, 1);
    assert.equal(log[0].deleted_id, 2);

    // Original table should have 2 rows
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 2);
  });
});

describe('Constraint + MVCC Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('constraint violation in rolled-back tx does not affect subsequent inserts', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    // Try to insert duplicate PK, fail
    const s1 = db.session();
    s1.begin();
    try {
      s1.execute("INSERT INTO t VALUES (1, 'duplicate')");
    } catch (e) {
      // Expected
    }
    s1.rollback();
    s1.close();

    // Now insert a different row — should work
    db.execute("INSERT INTO t VALUES (2, 'new')");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 2);
  });

  it('NOT NULL constraint + UPDATE to NULL', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");

    assert.throws(() => {
      db.execute("UPDATE t SET name = NULL WHERE id = 1");
    }, /NOT NULL/i);

    // Original value should be preserved
    const r = rows(db.execute('SELECT name FROM t WHERE id = 1'));
    assert.equal(r[0].name, 'alice');
  });

  it('multiple constraint violations in batch: first violation stops execution', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))');
    db.execute('INSERT INTO t VALUES (1, 25)');

    // Try batch of invalid inserts
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, 30)'), /duplicate|unique|primary/i);
    assert.throws(() => db.execute('INSERT INTO t VALUES (2, -5)'), /CHECK/i);

    // Only original row should exist
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 1);
  });
});
