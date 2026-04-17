// isolation-edge-depth.test.js — Transaction isolation edge case tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-iso-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Snapshot Isolation', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('session sees consistent snapshot during read', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');

    const s1 = db.session();
    s1.begin();
    
    // Read initial value
    const r1 = rows(s1.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r1[0].val, 100);

    // Another session updates
    db.execute('UPDATE t SET val = 200 WHERE id = 1');

    // s1 should still see old value (snapshot)
    const r2 = rows(s1.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r2[0].val, 100);

    s1.commit();
    s1.close();

    // After commit, new session sees updated value
    const r3 = rows(db.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r3[0].val, 200);
  });

  it('committed writes are visible to new transactions', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    
    const s1 = db.session();
    s1.begin();
    s1.execute('INSERT INTO t VALUES (1, 100)');
    s1.commit();
    s1.close();

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 100);
  });
});

describe('Rollback', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('rollback undoes writes', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');

    const s1 = db.session();
    s1.begin();
    s1.execute('UPDATE t SET val = 999 WHERE id = 1');
    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r[0].val, 100); // Should be unchanged
  });

  it('rollback undoes inserts', () => {
    db.execute('CREATE TABLE t (id INT)');

    const s1 = db.session();
    s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    s1.execute('INSERT INTO t VALUES (2)');
    s1.rollback();
    s1.close();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0);
  });
});

describe('Auto-Commit', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('individual statements auto-commit', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    
    // Should be immediately visible
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
  });
});
