// null-semantics-depth.test.js — NULL semantics depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-null-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('NULL Comparisons (Three-Valued Logic)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULL = NULL is not true', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t WHERE val = NULL'));
    assert.equal(r[0].c, 0, 'NULL = NULL should not match');
  });

  it('IS NULL finds NULLs', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t WHERE val IS NULL'));
    assert.equal(r[0].c, 1);
  });

  it('IS NOT NULL excludes NULLs', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t WHERE val IS NOT NULL'));
    assert.equal(r[0].c, 1);
  });
});

describe('NULL in JOINs', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INNER JOIN excludes NULLs', () => {
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, data TEXT)');
    db.execute('INSERT INTO t1 VALUES (1, 10)');
    db.execute('INSERT INTO t1 VALUES (NULL, 20)');
    db.execute("INSERT INTO t2 VALUES (1, 'match')");
    db.execute("INSERT INTO t2 VALUES (NULL, 'null-match')");

    const r = rows(db.execute('SELECT t1.val, t2.data FROM t1 INNER JOIN t2 ON t1.id = t2.id'));
    // NULL = NULL is false in INNER JOIN, so only id=1 matches
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 10);
  });

  it('LEFT JOIN preserves NULL-keyed rows from left', () => {
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, data TEXT)');
    db.execute('INSERT INTO t1 VALUES (1, 10)');
    db.execute('INSERT INTO t1 VALUES (NULL, 20)');
    db.execute("INSERT INTO t2 VALUES (1, 'match')");

    const r = rows(db.execute('SELECT t1.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id'));
    // Both t1 rows should appear
    assert.equal(r.length, 2);
  });
});

describe('NULL in GROUP BY', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULLs form their own group', () => {
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a', 1)");
    db.execute("INSERT INTO t VALUES ('a', 2)");
    db.execute('INSERT INTO t VALUES (NULL, 3)');
    db.execute('INSERT INTO t VALUES (NULL, 4)');

    const r = rows(db.execute('SELECT grp, SUM(val) AS total FROM t GROUP BY grp'));
    // Should have 2 groups: 'a' and NULL
    assert.equal(r.length, 2);
    const nullGroup = r.find(x => x.grp === null);
    assert.ok(nullGroup, 'NULL should form its own group');
    assert.equal(nullGroup.total, 7);
  });
});

describe('NULL Arithmetic', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULL + number = NULL', () => {
    const r = rows(db.execute('SELECT NULL + 5 AS val'));
    assert.equal(r[0].val, null);
  });

  it('NULL * number = NULL', () => {
    const r = rows(db.execute('SELECT NULL * 3 AS val'));
    assert.equal(r[0].val, null);
  });
});
