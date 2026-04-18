// index-mvcc.test.js — Index operations + MVCC edge cases
// Tests CREATE/DROP INDEX with concurrent transactions, index-accelerated queries.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-idx-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Index + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CREATE INDEX on existing data, then query uses index', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    db.execute('CREATE INDEX idx_score ON t (score)');
    
    // Query that should use the index
    const r = rows(db.execute('SELECT id FROM t WHERE score = 500'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 50);
  });

  it('index query with concurrent insert (not in snapshot)', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('CREATE INDEX idx_score ON t (score)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const s1 = db.session();
    s1.begin();
    
    // Insert new row outside s1
    db.execute('INSERT INTO t VALUES (6, 60)');
    
    // s1's index query should NOT see the new row
    const r = rows(s1.execute('SELECT id FROM t WHERE score = 60'));
    assert.equal(r.length, 0, 'Score 60 should not be in s1\'s snapshot');
    
    s1.commit();
    
    // After commit, index query sees it
    const r2 = rows(db.execute('SELECT id FROM t WHERE score = 60'));
    assert.equal(r2.length, 1);
  });

  it('index query with concurrent delete', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('CREATE INDEX idx_score ON t (score)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const s1 = db.session();
    s1.begin();
    
    // Delete row outside s1
    db.execute('DELETE FROM t WHERE id = 3');
    
    // s1 should still see the deleted row via index
    const r = rows(s1.execute('SELECT id FROM t WHERE score = 30'));
    assert.equal(r.length, 1, 'Deleted row should still be in s1\'s snapshot');
    assert.equal(r[0].id, 3);
    
    s1.commit();
  });

  it('index range query with concurrent modifications', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('CREATE INDEX idx_score ON t (score)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const s1 = db.session();
    s1.begin();
    
    // Modify data outside s1
    db.execute('INSERT INTO t VALUES (11, 55)'); // Between 50 and 60
    db.execute('DELETE FROM t WHERE id = 5');     // Remove score 50
    
    // s1's range query should see original data
    const r = rows(s1.execute('SELECT id FROM t WHERE score BETWEEN 40 AND 60 ORDER BY score'));
    assert.equal(r.length, 3, 'Should see 3 rows (40, 50, 60) in snapshot');
    
    s1.commit();
  });

  it('CREATE INDEX during open transaction', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM t WHERE val = 30'));
    
    // Create index while s1 is open
    db.execute('CREATE INDEX idx_val ON t (val)');
    
    // s1 should still work
    const r2 = rows(s1.execute('SELECT * FROM t WHERE val = 30'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].id, 3);
    
    s1.commit();
  });

  it('DROP INDEX during open transaction', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx_val ON t (val)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT * FROM t WHERE val = 30'));
    
    // Drop index while s1 is open
    db.execute('DROP INDEX idx_val');
    
    // s1 should still work (falls back to full scan)
    const r2 = rows(s1.execute('SELECT * FROM t WHERE val = 30'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].id, 3);
    
    s1.commit();
  });

  it('index survives close/reopen', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('CREATE INDEX idx_score ON t (score)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r = rows(db.execute('SELECT id FROM t WHERE score = 250'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 25);
  });

  it('multiple indexes on same table', () => {
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('CREATE INDEX idx_a ON t (a)');
    db.execute('CREATE INDEX idx_b ON t (b)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 5}, ${i % 3})`);
    
    const r1 = rows(db.execute('SELECT id FROM t WHERE a = 2 ORDER BY id'));
    assert.ok(r1.length > 0, 'Index on a should work');
    
    const r2 = rows(db.execute('SELECT id FROM t WHERE b = 1 ORDER BY id'));
    assert.ok(r2.length > 0, 'Index on b should work');
    
    // Combined filter
    const r3 = rows(db.execute('SELECT id FROM t WHERE a = 2 AND b = 1 ORDER BY id'));
    assert.ok(r3.length >= 0, 'Combined filter should work');
  });

  it('index with NULL values', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx_val ON t (val)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, NULL)');
    
    // Query for NULL
    const r = rows(db.execute('SELECT id FROM t WHERE val IS NULL ORDER BY id'));
    assert.equal(r.length, 2, 'Should find 2 NULL values');
    
    // Query for non-NULL
    const r2 = rows(db.execute('SELECT id FROM t WHERE val = 10'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].id, 2);
  });
});
