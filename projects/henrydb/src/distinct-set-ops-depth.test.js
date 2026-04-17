// distinct-set-ops-depth.test.js — DISTINCT + set operations depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-setops-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('DISTINCT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DISTINCT removes duplicate rows', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('a')");
    db.execute("INSERT INTO t VALUES ('b')");
    db.execute("INSERT INTO t VALUES ('a')");
    db.execute("INSERT INTO t VALUES ('c')");
    db.execute("INSERT INTO t VALUES ('b')");

    const r = rows(db.execute('SELECT DISTINCT val FROM t ORDER BY val'));
    assert.equal(r.length, 3);
    assert.deepEqual(r.map(x => x.val), ['a', 'b', 'c']);
  });

  it('DISTINCT with NULLs: NULLs treated as equal', () => {
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');

    const r = rows(db.execute('SELECT DISTINCT val FROM t ORDER BY val'));
    // Should have: NULL, 1
    assert.equal(r.length, 2);
  });

  it('DISTINCT on multiple columns', () => {
    db.execute('CREATE TABLE t (a INT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x')");
    db.execute("INSERT INTO t VALUES (1, 'y')");
    db.execute("INSERT INTO t VALUES (1, 'x')");
    db.execute("INSERT INTO t VALUES (2, 'x')");

    const r = rows(db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b'));
    assert.equal(r.length, 3); // (1,x), (1,y), (2,x)
  });
});

describe('UNION and UNION ALL', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UNION removes duplicates', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (4)');

    const r = rows(db.execute('SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 4);
    assert.deepEqual(r.map(x => x.id), [1, 2, 3, 4]);
  });

  it('UNION ALL keeps duplicates', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');

    const r = rows(db.execute('SELECT id FROM t1 UNION ALL SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 4); // 1, 2, 2, 3
    assert.deepEqual(r.map(x => x.id), [1, 2, 2, 3]);
  });

  it('UNION with NULL', () => {
    db.execute('CREATE TABLE t1 (val INT)');
    db.execute('CREATE TABLE t2 (val INT)');
    db.execute('INSERT INTO t1 VALUES (NULL)');
    db.execute('INSERT INTO t2 VALUES (NULL)');

    const r = rows(db.execute('SELECT val FROM t1 UNION SELECT val FROM t2'));
    // UNION removes dups; NULL = NULL for UNION purposes → 1 row
    assert.equal(r.length, 1);
  });

  it('UNION empty result', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');

    const r = rows(db.execute('SELECT id FROM t1 UNION SELECT id FROM t2'));
    assert.equal(r.length, 0);
  });
});

describe('INTERSECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INTERSECT returns common rows', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (4)');

    const r = rows(db.execute('SELECT id FROM t1 INTERSECT SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 2);
    assert.deepEqual(r.map(x => x.id), [2, 3]);
  });

  it('INTERSECT with empty set', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');

    const r = rows(db.execute('SELECT id FROM t1 INTERSECT SELECT id FROM t2'));
    assert.equal(r.length, 0);
  });
});

describe('EXCEPT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXCEPT returns rows in first but not second', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (4)');

    const r = rows(db.execute('SELECT id FROM t1 EXCEPT SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 2);
    assert.deepEqual(r.map(x => x.id), [1, 3]);
  });

  it('EXCEPT with identical sets returns empty', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (2)');

    const r = rows(db.execute('SELECT id FROM t1 EXCEPT SELECT id FROM t2'));
    assert.equal(r.length, 0);
  });
});
