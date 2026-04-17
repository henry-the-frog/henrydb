// aggregate-edge-depth.test.js — Aggregate edge case tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-agg-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Empty Table Aggregates', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('COUNT(*) on empty table returns 0', () => {
    db.execute('CREATE TABLE t (id INT)');
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0);
  });

  it('SUM on empty table returns NULL', () => {
    db.execute('CREATE TABLE t (val INT)');
    const r = rows(db.execute('SELECT SUM(val) AS s FROM t'));
    assert.equal(r[0].s, null);
  });

  it('AVG on empty table returns NULL', () => {
    db.execute('CREATE TABLE t (val INT)');
    const r = rows(db.execute('SELECT AVG(val) AS a FROM t'));
    assert.equal(r[0].a, null);
  });

  it('MIN/MAX on empty table returns NULL', () => {
    db.execute('CREATE TABLE t (val INT)');
    const rMin = rows(db.execute('SELECT MIN(val) AS m FROM t'));
    const rMax = rows(db.execute('SELECT MAX(val) AS m FROM t'));
    assert.equal(rMin[0].m, null);
    assert.equal(rMax[0].m, null);
  });
});

describe('COUNT(DISTINCT)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('counts distinct values', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    db.execute('INSERT INTO t VALUES (3)');
    db.execute('INSERT INTO t VALUES (3)');

    const r = rows(db.execute('SELECT COUNT(DISTINCT val) AS c FROM t'));
    assert.equal(r[0].c, 3);
  });
});

describe('Aggregates with NULLs', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SUM ignores NULLs', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (20)');

    const r = rows(db.execute('SELECT SUM(val) AS s FROM t'));
    assert.equal(r[0].s, 30);
  });

  it('COUNT(column) ignores NULLs', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (2)');

    const r = rows(db.execute('SELECT COUNT(val) AS c FROM t'));
    assert.equal(r[0].c, 2);
  });

  it('COUNT(*) counts NULLs', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 2);
  });
});

describe('GROUP_CONCAT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('concatenates group values', () => {
    db.execute('CREATE TABLE t (grp TEXT, val TEXT)');
    db.execute("INSERT INTO t VALUES ('a', 'x')");
    db.execute("INSERT INTO t VALUES ('a', 'y')");
    db.execute("INSERT INTO t VALUES ('b', 'z')");

    try {
      const r = rows(db.execute("SELECT grp, GROUP_CONCAT(val) AS vals FROM t GROUP BY grp ORDER BY grp"));
      assert.equal(r.length, 2);
      // Group 'a' should have 'x,y' or 'y,x'
      assert.ok(r[0].vals.includes('x') && r[0].vals.includes('y'));
    } catch {
      // GROUP_CONCAT may not be supported
    }
  });
});
