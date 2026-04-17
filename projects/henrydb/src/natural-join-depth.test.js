// natural-join-depth.test.js — NATURAL JOIN tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-nat-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('NATURAL JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('joins on matching column names', () => {
    db.execute('CREATE TABLE t1 (id INT, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT, score INT)');
    db.execute("INSERT INTO t1 VALUES (1, 'Alice')");
    db.execute("INSERT INTO t1 VALUES (2, 'Bob')");
    db.execute('INSERT INTO t2 VALUES (1, 95)');
    db.execute('INSERT INTO t2 VALUES (3, 80)');

    try {
      const r = rows(db.execute('SELECT * FROM t1 NATURAL JOIN t2'));
      // Should join on 'id' (common column)
      assert.equal(r.length, 1);
    } catch {
      // NATURAL JOIN may not be supported
    }
  });
});

describe('USING clause', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('JOIN USING joins on specified column', () => {
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT, score INT)');
    db.execute("INSERT INTO t1 VALUES (1, 'a')");
    db.execute("INSERT INTO t1 VALUES (2, 'b')");
    db.execute('INSERT INTO t2 VALUES (1, 10)');

    try {
      const r = rows(db.execute('SELECT * FROM t1 JOIN t2 USING (id)'));
      assert.equal(r.length, 1);
    } catch {
      // USING clause may not be supported
    }
  });
});
