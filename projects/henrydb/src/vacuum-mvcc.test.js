// vacuum-mvcc.test.js — Verify MVCC-aware vacuum cleans dead tuple versions
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('MVCC Vacuum', () => {
  let dir, db;

  afterEach(() => {
    if (db) try { db.close(); } catch {}
    if (dir && existsSync(dir)) rmSync(dir, { recursive: true, force: true });
  });

  it('removes dead versions after repeated UPDATEs', () => {
    dir = mkdtempSync(join(tmpdir(), 'vacuum-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    
    for (let i = 1; i <= 50; i++) {
      db.execute(`UPDATE t SET val = ${i} WHERE id = 1`);
    }

    const vmBefore = db._versionMaps.get('t');
    assert.equal(vmBefore.size, 51);

    const stats = db.vacuum();
    assert.equal(stats.t.deadTuplesRemoved, 50);
    
    const vmAfter = db._versionMaps.get('t');
    assert.equal(vmAfter.size, 1);
    
    // Data integrity
    const result = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(result.rows[0].val, 50);
  });

  it('SQL VACUUM command works through TransactionalDB', () => {
    dir = mkdtempSync(join(tmpdir(), 'vacuum-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, counter INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    
    for (let i = 1; i <= 20; i++) {
      db.execute(`UPDATE t SET counter = ${i} WHERE id = 1`);
    }

    const result = db.execute('VACUUM');
    assert.equal(result.type, 'OK');
    assert.ok(result.message.includes('20 dead tuples'));
    assert.equal(result.details.t.deadTuplesRemoved, 20);
  });

  it('preserves data across multiple tables', () => {
    dir = mkdtempSync(join(tmpdir(), 'vacuum-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO a VALUES (1, 100)');
    db.execute('INSERT INTO b VALUES (1, 200)');
    
    for (let i = 0; i < 10; i++) {
      db.execute(`UPDATE a SET val = val + 1 WHERE id = 1`);
      db.execute(`UPDATE b SET val = val + 1 WHERE id = 1`);
    }

    db.vacuum();

    const aResult = db.execute('SELECT val FROM a WHERE id = 1');
    const bResult = db.execute('SELECT val FROM b WHERE id = 1');
    assert.equal(aResult.rows[0].val, 110);
    assert.equal(bResult.rows[0].val, 210);
  });

  it('does not remove versions visible to active transactions', () => {
    dir = mkdtempSync(join(tmpdir(), 'vacuum-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');

    // Start a session that holds a snapshot
    const s = db.session();
    s.begin();
    s.execute('SELECT * FROM t WHERE id = 1'); // Take snapshot

    // Do updates after session started
    for (let i = 1; i <= 5; i++) {
      db.execute(`UPDATE t SET val = ${i} WHERE id = 1`);
    }

    // Vacuum should NOT remove versions visible to the active session
    db.vacuum();

    // Session should still see val=0 (its snapshot)
    const result = s.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(result.rows[0].val, 0);

    s.rollback();
  });

  it('vacuum is idempotent', () => {
    dir = mkdtempSync(join(tmpdir(), 'vacuum-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    
    for (let i = 1; i <= 10; i++) {
      db.execute(`UPDATE t SET val = ${i} WHERE id = 1`);
    }

    const stats1 = db.vacuum();
    assert.equal(stats1.t.deadTuplesRemoved, 10);
    
    const stats2 = db.vacuum();
    assert.equal(stats2.t?.deadTuplesRemoved || 0, 0);
    
    // Data still correct
    const result = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(result.rows[0].val, 10);
  });
});
