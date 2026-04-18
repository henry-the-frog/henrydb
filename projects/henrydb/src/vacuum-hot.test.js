// vacuum-hot.test.js — VACUUM + HOT chain interaction with MVCC
import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-vacuum-hot-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('VACUUM + HOT Chains (MVCC)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('index scan correct after HOT update + vacuum', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, counter INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 0)");
    
    // HOT update (non-indexed column)
    db.execute('UPDATE t SET counter = 10 WHERE id = 1');
    
    // VACUUM should clean up dead tuple and rebuild indexes
    db.vacuum();
    
    // Index scan should still work
    const r = rows(db.execute("SELECT * FROM t WHERE name = 'Alice'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].counter, 10);
  });

  it('multiple HOT updates + vacuum: chains cleared, index correct', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, version INT)');
    db.execute('CREATE INDEX idx_tag ON t (tag)');
    db.execute("INSERT INTO t VALUES (1, 'X', 0)");
    
    // Many HOT updates
    for (let i = 1; i <= 10; i++) {
      db.execute(`UPDATE t SET version = ${i} WHERE id = 1`);
    }
    
    // Vacuum
    db.vacuum();
    
    // Index scan should find latest version
    const r = rows(db.execute("SELECT version FROM t WHERE tag = 'X'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].version, 10);
  });

  it('vacuum after non-HOT update: index updated correctly', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'old', 0)");
    
    // Non-HOT update (changes indexed column)
    db.execute("UPDATE t SET name = 'new' WHERE id = 1");
    
    // Vacuum
    db.vacuum();
    
    // Old name should not find anything
    const old = rows(db.execute("SELECT * FROM t WHERE name = 'old'"));
    assert.equal(old.length, 0);
    
    // New name should find the row
    const cur = rows(db.execute("SELECT * FROM t WHERE name = 'new'"));
    assert.equal(cur.length, 1);
  });

  it('vacuum preserves visibility for active snapshots', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'A', 0)");
    
    // Start snapshot
    const s1 = db.session();
    s1.begin();
    
    // HOT update
    db.execute('UPDATE t SET val = 1 WHERE id = 1');
    
    // s1 should still see val=0
    const r1 = rows(s1.execute("SELECT val FROM t WHERE name = 'A'"));
    assert.equal(r1[0].val, 0);
    
    // Vacuum (should NOT remove the old version — s1 still needs it)
    db.vacuum();
    
    // s1 should STILL see val=0 after vacuum
    const r2 = rows(s1.execute("SELECT val FROM t WHERE name = 'A'"));
    assert.equal(r2[0].val, 0);
    
    s1.commit();
    
    // After s1 commits, vacuum can clean up
    db.vacuum();
    
    // Latest value visible
    const r3 = rows(db.execute("SELECT val FROM t WHERE name = 'A'"));
    assert.equal(r3[0].val, 1);
  });

  it('bulk HOT updates + vacuum: all rows accessible via index', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT, qty INT)');
    db.execute('CREATE INDEX idx_cat ON items (category)');
    
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, '${i <= 10 ? 'A' : 'B'}', ${i * 10})`);
    }
    
    // Bulk HOT update
    db.execute("UPDATE items SET qty = qty + 100 WHERE category = 'A'");
    
    // Vacuum
    db.vacuum();
    
    // All rows should be accessible via index
    const catA = rows(db.execute("SELECT * FROM items WHERE category = 'A' ORDER BY id"));
    assert.equal(catA.length, 10);
    assert.equal(catA[0].qty, 110); // 10 + 100
    
    const catB = rows(db.execute("SELECT * FROM items WHERE category = 'B' ORDER BY id"));
    assert.equal(catB.length, 10);
    assert.equal(catB[0].qty, 110); // unchanged
  });

  it('delete + vacuum + HOT: no ghost entries in index', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute('CREATE INDEX idx_name ON t (name)');
    db.execute("INSERT INTO t VALUES (1, 'A', 0)");
    db.execute("INSERT INTO t VALUES (2, 'B', 0)");
    
    // HOT update then delete
    db.execute('UPDATE t SET val = 1 WHERE id = 1');
    db.execute('DELETE FROM t WHERE id = 1');
    
    // Vacuum
    db.vacuum();
    
    // 'A' should be completely gone
    const r = rows(db.execute("SELECT * FROM t WHERE name = 'A'"));
    assert.equal(r.length, 0);
    
    // 'B' still accessible
    const r2 = rows(db.execute("SELECT * FROM t WHERE name = 'B'"));
    assert.equal(r2.length, 1);
  });
});
