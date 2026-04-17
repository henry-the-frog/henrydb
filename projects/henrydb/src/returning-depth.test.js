// returning-depth.test.js — RETURNING clause depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ret-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('INSERT RETURNING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('RETURNING * returns inserted row', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'Alice') RETURNING *");
    const retRows = rows(r);
    assert.equal(retRows.length, 1);
    assert.equal(retRows[0].id, 1);
    assert.equal(retRows[0].name, 'Alice');
  });

  it('RETURNING specific columns', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'Alice', 95) RETURNING id, name");
    const retRows = rows(r);
    assert.equal(retRows.length, 1);
    assert.equal(retRows[0].id, 1);
    assert.equal(retRows[0].name, 'Alice');
  });

  it('RETURNING with SERIAL', () => {
    db.execute('CREATE TABLE t (id SERIAL, name TEXT)');
    const r = db.execute("INSERT INTO t (name) VALUES ('Alice') RETURNING id");
    const retRows = rows(r);
    assert.equal(retRows.length, 1);
    assert.ok(retRows[0].id > 0, 'Should return auto-generated ID');
  });
});

describe('UPDATE RETURNING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('RETURNING after UPDATE', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');

    const r = db.execute('UPDATE t SET val = val + 50 WHERE id = 1 RETURNING *');
    const retRows = rows(r);
    assert.equal(retRows.length, 1);
    assert.equal(retRows[0].val, 150);
  });
});

describe('DELETE RETURNING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('RETURNING after DELETE', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");

    const r = db.execute('DELETE FROM t WHERE id = 1 RETURNING *');
    const retRows = rows(r);
    assert.equal(retRows.length, 1);
    assert.equal(retRows[0].name, 'Alice');

    // Verify row is actually deleted
    const remaining = rows(db.execute('SELECT * FROM t'));
    assert.equal(remaining.length, 1);
  });
});
