// boolean-depth.test.js — BOOLEAN type depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-bool-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('BOOLEAN Type', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('boolean column with TRUE/FALSE', () => {
    db.execute('CREATE TABLE t (id INT, active BOOLEAN)');
    db.execute('INSERT INTO t VALUES (1, TRUE)');
    db.execute('INSERT INTO t VALUES (2, FALSE)');

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.ok(r[0].active === true || r[0].active === 1);
    assert.ok(r[1].active === false || r[1].active === 0);
  });

  it('WHERE with boolean column', () => {
    db.execute('CREATE TABLE t (id INT, active BOOLEAN)');
    db.execute('INSERT INTO t VALUES (1, TRUE)');
    db.execute('INSERT INTO t VALUES (2, FALSE)');
    db.execute('INSERT INTO t VALUES (3, TRUE)');

    const r = rows(db.execute('SELECT id FROM t WHERE active = TRUE ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 3);
  });

  it('boolean expression in SELECT', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('INSERT INTO t VALUES (1, 90)');
    db.execute('INSERT INTO t VALUES (2, 50)');

    const r = rows(db.execute('SELECT id, score > 70 AS passing FROM t ORDER BY id'));
    assert.equal(r.length, 2);
  });
});
