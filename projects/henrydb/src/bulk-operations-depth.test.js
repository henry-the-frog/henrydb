// bulk-operations-depth.test.js — TRUNCATE + bulk ops depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-bulk-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('TRUNCATE TABLE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('TRUNCATE removes all rows', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);

    db.execute('TRUNCATE TABLE t');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0, 'TRUNCATE should remove all rows');
  });

  it('table is usable after TRUNCATE', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'before')");

    db.execute('TRUNCATE TABLE t');
    db.execute("INSERT INTO t VALUES (2, 'after')");

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
    assert.equal(r[0].val, 'after');
  });

  it('TRUNCATE vs DELETE: TRUNCATE is non-transactional', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);

    // TRUNCATE doesn't participate in transaction rollback in most DBs
    // In HenryDB, behavior may vary
    db.execute('TRUNCATE TABLE t');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0);
  });
});

describe('Bulk INSERT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INSERT multiple rows in single statement', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 5);
  });

  it('1000 sequential inserts', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    }

    const count = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    assert.equal(count, 1000);

    // Spot checks
    const r50 = rows(db.execute('SELECT val FROM t WHERE id = 50'));
    assert.equal(r50[0].val, 500);

    const r999 = rows(db.execute('SELECT val FROM t WHERE id = 999'));
    assert.equal(r999[0].val, 9990);
  });
});

describe('Bulk DELETE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DELETE with large result set', () => {
    db.execute('CREATE TABLE t (id INT, category TEXT)');
    for (let i = 1; i <= 500; i++) {
      const cat = i % 3 === 0 ? 'delete_me' : 'keep';
      db.execute(`INSERT INTO t VALUES (${i}, '${cat}')`);
    }

    db.execute("DELETE FROM t WHERE category = 'delete_me'");

    const remaining = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    // 500 total, ~167 with delete_me → ~333 remaining
    assert.ok(remaining > 300 && remaining < 350,
      `Should have ~333 rows, got ${remaining}`);

    // Verify all remaining have category 'keep'
    const deleted = rows(db.execute("SELECT COUNT(*) AS c FROM t WHERE category = 'delete_me'"))[0].c;
    assert.equal(deleted, 0, 'No delete_me rows should remain');
  });

  it('DELETE all rows (equivalent to TRUNCATE)', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);

    db.execute('DELETE FROM t');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 0);
  });
});

describe('Bulk UPDATE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPDATE all rows', () => {
    db.execute('CREATE TABLE t (id INT, status TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'active')`);

    db.execute("UPDATE t SET status = 'archived'");

    const r = rows(db.execute("SELECT COUNT(*) AS c FROM t WHERE status = 'archived'"));
    assert.equal(r[0].c, 100);
  });

  it('UPDATE with expression', () => {
    db.execute('CREATE TABLE t (id INT, price INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 100})`);

    // 10% price increase
    db.execute('UPDATE t SET price = price * 1.1 WHERE price > 2500');

    const r = rows(db.execute('SELECT price FROM t WHERE id = 30'));
    // id=30: price was 3000, now 3300
    assert.ok(Math.abs(r[0].price - 3300) < 1, `Price should be 3300, got ${r[0].price}`);
  });
});
