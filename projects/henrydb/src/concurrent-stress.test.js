// concurrent-stress.test.js — Concurrent correctness stress tests for TransactionalDatabase
// Tests MVCC isolation under interleaved multi-session workloads
// These tests verify snapshot isolation properties with explicit session interleaving

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
let db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-concurrent-'));
  db = TransactionalDatabase.open(dbDir);
}

function teardown() {
  try { db.close(); } catch (e) { /* ignore */ }
  rmSync(dbDir, { recursive: true, force: true });
}

// Helper: extract rows from result (handles {type, rows} format)
function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

// ===== 1. NO DIRTY READS =====

describe('Concurrent Isolation: No Dirty Reads', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('uncommitted INSERT is invisible to other sessions', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s1.execute("INSERT INTO t VALUES (2, 'uncommitted')");

    s2.begin();
    const r = rows(s2.execute('SELECT * FROM t'));
    s2.commit();

    assert.equal(r.length, 1, `Expected 1 row, got ${r.length}: ${JSON.stringify(r)}`);
    assert.equal(r[0].id, 1);

    s1.rollback();
    s1.close();
    s2.close();
  });

  it('uncommitted UPDATE is invisible to other sessions', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s1.execute("UPDATE t SET val = 'modified' WHERE id = 1");

    s2.begin();
    const r = rows(s2.execute('SELECT val FROM t WHERE id = 1'));
    s2.commit();

    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'original');

    s1.rollback();
    s1.close();
    s2.close();
  });

  it('uncommitted DELETE is invisible to other sessions', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alive')");
    db.execute("INSERT INTO t VALUES (2, 'alive')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s1.execute('DELETE FROM t WHERE id = 1');

    s2.begin();
    const r = rows(s2.execute('SELECT * FROM t'));
    s2.commit();

    assert.equal(r.length, 2, `Expected 2 rows, got ${r.length}`);

    s1.rollback();
    s1.close();
    s2.close();
  });
});

// ===== 2. REPEATABLE READS =====

describe('Concurrent Isolation: Repeatable Reads', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('committed INSERT by another session is invisible within existing snapshot', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    const read1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read1.length, 1);

    s2.begin();
    s2.execute('INSERT INTO t VALUES (2)');
    s2.commit();

    const read2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read2.length, 1, 'Repeatable read violated: new row appeared in snapshot');

    s1.commit();
    s1.close();
    s2.close();
  });

  it('committed DELETE by another session is invisible within existing snapshot', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    const read1 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read1.length, 2);

    s2.begin();
    s2.execute('DELETE FROM t WHERE id = 2');
    s2.commit();

    const read2 = rows(s1.execute('SELECT * FROM t'));
    assert.equal(read2.length, 2, 'Repeatable read violated: row disappeared from snapshot');

    s1.commit();
    s1.close();
    s2.close();
  });
});

// ===== 3. LOST UPDATE PREVENTION =====

describe('Concurrent Isolation: Lost Update Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('concurrent DELETE on same row causes write-write conflict', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    s1.execute('DELETE FROM t WHERE id = 1');

    assert.throws(
      () => s2.execute('DELETE FROM t WHERE id = 1'),
      /conflict/i,
      'Should detect write-write conflict on concurrent delete'
    );

    s1.commit();
    s2.rollback();
    s1.close();
    s2.close();
  });
});

// ===== 4. PHANTOM PREVENTION =====

describe('Concurrent Isolation: Phantom Prevention', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('range query sees no phantoms from concurrent inserts', () => {
    db.execute('CREATE TABLE t (id INT, category TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'A')");
    db.execute("INSERT INTO t VALUES (3, 'B')");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    const read1 = rows(s1.execute("SELECT * FROM t WHERE category = 'A'"));
    assert.equal(read1.length, 2);

    s2.begin();
    s2.execute("INSERT INTO t VALUES (4, 'A')");
    s2.execute("INSERT INTO t VALUES (5, 'A')");
    s2.commit();

    const read2 = rows(s1.execute("SELECT * FROM t WHERE category = 'A'"));
    assert.equal(read2.length, 2, 'Phantom detected: new rows appeared in range query');

    s1.commit();
    s1.close();
    s2.close();
  });

  it('COUNT is stable across concurrent inserts', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    const count1 = rows(s1.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(count1[0].c, 10);

    s2.begin();
    for (let i = 11; i <= 15; i++) s2.execute(`INSERT INTO t VALUES (${i})`);
    s2.commit();

    const count2 = rows(s1.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(count2[0].c, 10, 'Phantom in aggregate: count changed');

    s1.commit();
    s1.close();
    s2.close();
  });
});

// ===== 5. WRITE SKEW =====

describe('Concurrent Isolation: Write Skew', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('write skew is possible under snapshot isolation (SI allows it)', () => {
    db.execute('CREATE TABLE doctors (name TEXT, oncall INT)');
    db.execute("INSERT INTO doctors VALUES ('Alice', 1)");
    db.execute("INSERT INTO doctors VALUES ('Bob', 1)");

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    s2.begin();

    const s1Read = rows(s1.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    const s2Read = rows(s2.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(s1Read[0].c, 2);
    assert.equal(s2Read[0].c, 2);

    s1.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Alice'");
    s1.commit();

    s2.execute("UPDATE doctors SET oncall = 0 WHERE name = 'Bob'");
    s2.commit();

    const final = rows(db.execute('SELECT COUNT(*) AS c FROM doctors WHERE oncall = 1'));
    assert.equal(final[0].c, 0, 'Write skew should be possible under SI');

    s1.close();
    s2.close();
  });
});

// ===== 6. MANY-SESSION STRESS =====

describe('Concurrent Stress: Many Sessions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('10 sessions each insert 10 rows, all visible after commit', () => {
    db.execute('CREATE TABLE t (session_id INT, row_num INT)');
    const sessions = [];
    
    for (let i = 0; i < 10; i++) {
      const s = db.session();
      s.begin();
      sessions.push(s);
    }

    for (let i = 0; i < 10; i++) {
      for (let j = 0; j < 10; j++) {
        sessions[i].execute(`INSERT INTO t VALUES (${i}, ${j})`);
      }
    }

    for (const s of sessions) {
      s.commit();
      s.close();
    }

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 100);
  });

  it('interleaved insert/commit: each session sees correct snapshot', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    const s1 = db.session();
    const s2 = db.session();
    const s3 = db.session();

    s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    s1.commit();

    s2.begin();
    const s2Read = rows(s2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(s2Read[0].c, 1);

    s3.begin();
    s3.execute('INSERT INTO t VALUES (2)');

    const s2Read2 = rows(s2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(s2Read2[0].c, 1, 's2 should not see s3 uncommitted');

    s3.commit();

    const s2Read3 = rows(s2.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(s2Read3[0].c, 1, 's2 snapshot should be stable');

    s2.commit();
    
    const final = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(final[0].c, 2);

    s1.close();
    s2.close();
    s3.close();
  });

  it('rapid sequential transactions maintain consistency', () => {
    db.execute('CREATE TABLE counter (id INT, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');

    for (let i = 0; i < 50; i++) {
      const s = db.session();
      s.begin();
      const r = rows(s.execute('SELECT val FROM counter WHERE id = 1'));
      const newVal = r[0].val + 1;
      s.execute(`UPDATE counter SET val = ${newVal} WHERE id = 1`);
      s.commit();
      s.close();
    }

    const result = rows(db.execute('SELECT val FROM counter WHERE id = 1'));
    assert.equal(result[0].val, 50, 'Counter should be exactly 50 after 50 increments');
  });
});

// ===== 7. ROLLBACK CORRECTNESS =====

describe('Concurrent Stress: Rollback Correctness', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('rolled back inserts are invisible to all subsequent transactions', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');

    const s1 = db.session();
    s1.begin();
    s1.execute('INSERT INTO t VALUES (2)');
    s1.execute('INSERT INTO t VALUES (3)');
    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
  });

  it('rolled back deletes leave rows intact', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');

    const s1 = db.session();
    s1.begin();
    s1.execute('DELETE FROM t WHERE id = 1');
    
    const s1Rows = rows(s1.execute('SELECT * FROM t'));
    assert.equal(s1Rows.length, 1);

    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 2);
  });

  it('alternating commit/rollback stress test', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    let expectedCount = 0;
    for (let i = 0; i < 20; i++) {
      const s = db.session();
      s.begin();
      s.execute(`INSERT INTO t VALUES (${i})`);
      
      if (i % 2 === 0) {
        s.commit();
        expectedCount++;
      } else {
        s.rollback();
      }
      s.close();
    }

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, expectedCount, `Expected ${expectedCount} committed rows`);
  });
});

// ===== 8. ISOLATION UNDER AGGREGATE QUERIES =====

describe('Concurrent Stress: Aggregate Isolation', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SUM is stable across concurrent modifications', () => {
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 100)');
    db.execute('INSERT INTO accounts VALUES (2, 200)');
    db.execute('INSERT INTO accounts VALUES (3, 300)');

    const s1 = db.session();
    const s2 = db.session();

    s1.begin();
    const sum1 = rows(s1.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum1[0].total, 600);

    s2.begin();
    s2.execute('UPDATE accounts SET balance = 150 WHERE id = 1');
    s2.execute('INSERT INTO accounts VALUES (4, 400)');
    s2.commit();

    const sum2 = rows(s1.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum2[0].total, 600, 'Aggregate SUM changed within snapshot');

    s1.commit();

    const sum3 = rows(db.execute('SELECT SUM(balance) AS total FROM accounts'));
    assert.equal(sum3[0].total, 1050); // 150 + 200 + 300 + 400

    s1.close();
    s2.close();
  });
});
