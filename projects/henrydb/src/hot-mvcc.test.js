// hot-mvcc.test.js — HOT chains + MVCC interaction tests
// Verifies that HOT (Heap-Only Tuple) updates work correctly under snapshot isolation.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-hot-mvcc-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('HOT Chains + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('HOT update of non-indexed column visible via index after commit', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, counter INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    
    // Update non-indexed column (should be HOT-eligible)
    db.execute('UPDATE t SET counter = 10 WHERE id = 1');
    
    // Index scan should find updated value
    const r = rows(db.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].counter, 10);
  });

  it('HOT update not visible to older snapshot', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, counter INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    
    // Start a session with a snapshot BEFORE the update
    const s1 = db.session();
    s1.begin();
    
    // Read the original value in s1's snapshot
    const r1 = rows(s1.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(r1.length, 1);
    assert.equal(r1[0].counter, 0);
    
    // Update outside s1 (non-indexed column = HOT eligible)
    db.execute('UPDATE t SET counter = 10 WHERE id = 1');
    
    // s1 should still see the old value (snapshot isolation)
    const r2 = rows(s1.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].counter, 0);
    
    s1.commit();
    
    // After commit, new session sees updated value
    const r3 = rows(db.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(r3.length, 1);
    assert.equal(r3[0].counter, 10);
  });

  it('multiple HOT updates: each snapshot sees correct version', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, version INT)');
    db.execute('CREATE INDEX idx_tag ON t (tag)');
    db.execute("INSERT INTO t VALUES (1, 'X', 0)");
    
    // Take snapshot at version 0
    const s1 = db.session();
    s1.begin();
    
    // HOT update to version 1
    db.execute('UPDATE t SET version = 1 WHERE id = 1');
    
    // Take snapshot at version 1
    const s2 = db.session();
    s2.begin();
    
    // HOT update to version 2
    db.execute('UPDATE t SET version = 2 WHERE id = 1');
    
    // s1 should see version 0
    const r1 = rows(s1.execute("SELECT version FROM t WHERE tag = 'X'"));
    assert.equal(r1.length, 1);
    assert.equal(r1[0].version, 0);
    
    // s2 should see version 1
    const r2 = rows(s2.execute("SELECT version FROM t WHERE tag = 'X'"));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].version, 1);
    
    // Current session should see version 2
    const r3 = rows(db.execute("SELECT version FROM t WHERE tag = 'X'"));
    assert.equal(r3.length, 1);
    assert.equal(r3[0].version, 2);
    
    s1.commit();
    s2.commit();
  });

  it('mixed HOT and non-HOT updates preserve index correctness', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, data INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    
    // HOT update (data only)
    db.execute('UPDATE t SET data = 1 WHERE id = 1');
    
    // Non-HOT update (changes indexed column)
    db.execute("UPDATE t SET name = 'Bob' WHERE id = 1");
    
    // Old name should not find anything
    const old = rows(db.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(old.length, 0);
    
    // New name should find the row
    const cur = rows(db.execute("SELECT * FROM t WHERE name = 'Bob'"));
    assert.equal(cur.length, 1);
    assert.equal(cur[0].data, 1);
  });

  it('HOT update on bulk rows: index scan returns correct results', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT, qty INT)');
    db.execute('CREATE INDEX idx_cat ON items (category)');
    
    for (let i = 1; i <= 20; i++) {
      const cat = i <= 10 ? 'A' : 'B';
      db.execute(`INSERT INTO items VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    // HOT update: change qty (non-indexed) for category A
    db.execute("UPDATE items SET qty = qty + 100 WHERE category = 'A'");
    
    // Index scan should find all 10 items with updated qty
    const catA = rows(db.execute("SELECT * FROM items WHERE category = 'A' ORDER BY id"));
    assert.equal(catA.length, 10);
    assert.equal(catA[0].qty, 110); // 10 + 100
    assert.equal(catA[9].qty, 200); // 100 + 100
  });

  it('concurrent HOT updates to different rows preserve both', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 0)");
    
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE t SET val = 10 WHERE id = 1');
    
    const s2 = db.session();
    s2.begin();
    s2.execute('UPDATE t SET val = 20 WHERE id = 2');
    
    s1.commit();
    s2.commit();
    
    // Both updates should be visible
    const alice = rows(db.execute("SELECT val FROM t WHERE name = 'Alice'"));
    assert.equal(alice.length, 1);
    assert.equal(alice[0].val, 10);
    
    const bob = rows(db.execute("SELECT val FROM t WHERE name = 'Bob'"));
    assert.equal(bob.length, 1);
    assert.equal(bob[0].val, 20);
  });

  it('rollback of HOT update: old value visible again via index', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, counter INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    
    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE t SET counter = 99 WHERE id = 1');
    
    // Within s1, should see updated value
    const r1 = rows(s1.execute("SELECT counter FROM t WHERE name = 'Alice'"));
    assert.equal(r1.length, 1);
    assert.equal(r1[0].counter, 99);
    
    s1.rollback();
    
    // After rollback, original value should be visible
    const r2 = rows(db.execute("SELECT counter FROM t WHERE name = 'Alice'"));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].counter, 0);
  });
});
