// upsert-advanced-depth.test.js — Advanced UPSERT tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ups-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('UPSERT DO NOTHING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DO NOTHING silently skips duplicate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    db.execute("INSERT INTO t VALUES (1, 'duplicate') ON CONFLICT DO NOTHING");

    const r = rows(db.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r[0].val, 'original');
  });

  it('DO NOTHING allows non-duplicate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    db.execute("INSERT INTO t VALUES (2, 'second') ON CONFLICT DO NOTHING");

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 2);
  });
});

describe('UPSERT DO UPDATE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DO UPDATE updates on conflict', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT, count INT)');
    db.execute("INSERT INTO t VALUES (1, 'hello', 1)");
    db.execute("INSERT INTO t VALUES (1, 'hello', 1) ON CONFLICT (id) DO UPDATE SET count = t.count + 1");

    const r = rows(db.execute('SELECT count FROM t WHERE id = 1'));
    assert.equal(r[0].count, 2);
  });

  it('DO UPDATE with excluded values', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    db.execute("INSERT INTO t VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

    const r = rows(db.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r[0].val, 'updated');
  });

  it('multiple upserts increment correctly', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, hits INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET hits = t.hits + 1');
    db.execute('INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET hits = t.hits + 1');
    db.execute('INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET hits = t.hits + 1');

    const r = rows(db.execute('SELECT hits FROM t WHERE id = 1'));
    assert.equal(r[0].hits, 4);
  });
});

describe('UPSERT Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('batch upsert: mix of inserts and updates', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (3, 'c')");

    db.execute("INSERT INTO t VALUES (1, 'a2') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    db.execute("INSERT INTO t VALUES (2, 'b') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    db.execute("INSERT INTO t VALUES (3, 'c2') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].val, 'a2');
    assert.equal(r[1].val, 'b');
    assert.equal(r[2].val, 'c2');
  });
});
