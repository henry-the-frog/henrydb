// set-ops-deep-depth.test.js — Set operations deep dive tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-sop-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t1 (id INT, val TEXT)');
  db.execute('CREATE TABLE t2 (id INT, val TEXT)');
  db.execute("INSERT INTO t1 VALUES (1, 'a')");
  db.execute("INSERT INTO t1 VALUES (2, 'b')");
  db.execute("INSERT INTO t1 VALUES (3, 'c')");
  db.execute("INSERT INTO t2 VALUES (2, 'b')");
  db.execute("INSERT INTO t2 VALUES (3, 'c')");
  db.execute("INSERT INTO t2 VALUES (4, 'd')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('UNION ALL', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('includes duplicates', () => {
    const r = rows(db.execute('SELECT id FROM t1 UNION ALL SELECT id FROM t2'));
    assert.equal(r.length, 6); // 3 + 3, including duplicates
  });
});

describe('UNION', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('removes duplicates', () => {
    const r = rows(db.execute('SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 4); // 1, 2, 3, 4
    assert.equal(r[0].id, 1);
    assert.equal(r[3].id, 4);
  });
});

describe('INTERSECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('returns only common rows', () => {
    const r = rows(db.execute('SELECT id FROM t1 INTERSECT SELECT id FROM t2 ORDER BY id'));
    assert.equal(r.length, 2); // 2, 3
    assert.equal(r[0].id, 2);
    assert.equal(r[1].id, 3);
  });
});

describe('EXCEPT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('returns rows in first but not second', () => {
    const r = rows(db.execute('SELECT id FROM t1 EXCEPT SELECT id FROM t2'));
    assert.equal(r.length, 1); // 1
    assert.equal(r[0].id, 1);
  });

  it('reverse direction', () => {
    const r = rows(db.execute('SELECT id FROM t2 EXCEPT SELECT id FROM t1'));
    assert.equal(r.length, 1); // 4
    assert.equal(r[0].id, 4);
  });
});
