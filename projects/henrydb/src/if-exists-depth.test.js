// if-exists-depth.test.js — IF EXISTS/IF NOT EXISTS depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ife-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('CREATE TABLE IF NOT EXISTS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('creates table normally', () => {
    db.execute('CREATE TABLE IF NOT EXISTS t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
  });

  it('does not error on duplicate', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    // Should not throw
    db.execute('CREATE TABLE IF NOT EXISTS t (id INT)');
    // Data should still be there
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
  });
});

describe('DROP TABLE IF EXISTS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('drops existing table', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('DROP TABLE IF EXISTS t');
    assert.throws(() => db.execute('SELECT * FROM t'));
  });

  it('does not error on non-existent table', () => {
    // Should not throw
    db.execute('DROP TABLE IF EXISTS nonexistent');
  });
});

describe('CREATE INDEX IF NOT EXISTS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('creates index normally', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX IF NOT EXISTS idx_val ON t (val)');
  });

  it('does not error on duplicate index', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE INDEX idx_val ON t (val)');
    // Should not throw
    db.execute('CREATE INDEX IF NOT EXISTS idx_val ON t (val)');
  });
});
