// math-function-depth.test.js — Math function depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-math-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Math Functions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ABS of negative', () => {
    const r = rows(db.execute('SELECT ABS(-42) AS val'));
    assert.equal(r[0].val, 42);
  });

  it('ABS of positive', () => {
    const r = rows(db.execute('SELECT ABS(42) AS val'));
    assert.equal(r[0].val, 42);
  });

  it('ROUND with decimals', () => {
    const r = rows(db.execute('SELECT ROUND(3.7) AS val'));
    assert.equal(r[0].val, 4);
  });

  it('ROUND with precision', () => {
    const r = rows(db.execute('SELECT ROUND(3.14159, 2) AS val'));
    assert.ok(Math.abs(r[0].val - 3.14) < 0.01);
  });

  it('CEIL rounds up', () => {
    const r = rows(db.execute('SELECT CEIL(3.2) AS val'));
    assert.equal(r[0].val, 4);
  });

  it('FLOOR rounds down', () => {
    const r = rows(db.execute('SELECT FLOOR(3.8) AS val'));
    assert.equal(r[0].val, 3);
  });

  it('POWER', () => {
    const r = rows(db.execute('SELECT POWER(2, 10) AS val'));
    assert.equal(r[0].val, 1024);
  });

  it('SQRT', () => {
    const r = rows(db.execute('SELECT SQRT(144) AS val'));
    assert.equal(r[0].val, 12);
  });

  it('MOD', () => {
    const r = rows(db.execute('SELECT MOD(17, 5) AS val'));
    assert.equal(r[0].val, 2);
  });

  it('math functions with NULL return NULL', () => {
    const r = rows(db.execute('SELECT ABS(NULL) AS val'));
    assert.equal(r[0].val, null);
  });

  it('math in WHERE clause', () => {
    db.execute('CREATE TABLE t (id INT, val FLOAT)');
    db.execute('INSERT INTO t VALUES (1, -5.5)');
    db.execute('INSERT INTO t VALUES (2, 3.3)');
    db.execute('INSERT INTO t VALUES (3, -1.1)');

    const r = rows(db.execute('SELECT id FROM t WHERE ABS(val) > 3 ORDER BY id'));
    assert.equal(r.length, 2); // id 1 (5.5) and id 2 (3.3)
  });
});
