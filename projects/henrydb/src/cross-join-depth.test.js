// cross-join-depth.test.js — CROSS JOIN tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-cross-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('CROSS JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('produces cartesian product', () => {
    db.execute('CREATE TABLE t1 (a INT)');
    db.execute('CREATE TABLE t2 (b INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (10)');
    db.execute('INSERT INTO t2 VALUES (20)');
    db.execute('INSERT INTO t2 VALUES (30)');

    const r = rows(db.execute('SELECT a, b FROM t1 CROSS JOIN t2'));
    assert.equal(r.length, 6); // 2 × 3 = 6
  });

  it('empty table produces empty result', () => {
    db.execute('CREATE TABLE t1 (a INT)');
    db.execute('CREATE TABLE t2 (b INT)');
    db.execute('INSERT INTO t1 VALUES (1)');

    const r = rows(db.execute('SELECT * FROM t1 CROSS JOIN t2'));
    assert.equal(r.length, 0);
  });

  it('single row tables produce single row', () => {
    db.execute('CREATE TABLE t1 (a INT)');
    db.execute('CREATE TABLE t2 (b INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (2)');

    const r = rows(db.execute('SELECT a, b FROM t1 CROSS JOIN t2'));
    assert.equal(r.length, 1);
    assert.equal(r[0].a, 1);
    assert.equal(r[0].b, 2);
  });

  it('comma-separated FROM is implicit CROSS JOIN', () => {
    db.execute('CREATE TABLE t1 (a INT)');
    db.execute('CREATE TABLE t2 (b INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (10)');

    const r = rows(db.execute('SELECT a, b FROM t1, t2'));
    assert.equal(r.length, 2);
  });
});
