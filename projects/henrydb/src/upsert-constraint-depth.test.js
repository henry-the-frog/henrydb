// upsert-constraint-depth.test.js — UPSERT constraint enforcement tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-upsert-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('UPSERT Constraint Enforcement', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPSERT DO UPDATE enforces CHECK constraint', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))');
    db.execute('INSERT INTO t VALUES (1, 25)');

    assert.throws(() => {
      db.execute('INSERT INTO t VALUES (1, -5) ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age');
    }, /CHECK/i);

    assert.equal(rows(db.execute('SELECT age FROM t'))[0].age, 25);
  });

  it('UPSERT DO UPDATE enforces NOT NULL constraint', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");

    assert.throws(() => {
      db.execute('INSERT INTO t VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name');
    }, /NOT NULL/i);

    assert.equal(rows(db.execute('SELECT name FROM t'))[0].name, 'alice');
  });

  it('UPSERT DO UPDATE enforces FK constraint', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO child VALUES (1, 1)');

    assert.throws(() => {
      db.execute('INSERT INTO child VALUES (1, 999) ON CONFLICT (id) DO UPDATE SET pid = EXCLUDED.pid');
    }, /foreign key|not found/i);

    assert.equal(rows(db.execute('SELECT pid FROM child'))[0].pid, 1);
  });

  it('valid UPSERT DO UPDATE works after constraint fix', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0), name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, 25, 'alice')");

    // Valid update
    db.execute("INSERT INTO t VALUES (1, 30, 'ALICE') ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age, name = EXCLUDED.name");
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].age, 30);
    assert.equal(r[0].name, 'ALICE');
  });

  it('UPSERT DO NOTHING with constraint violation on insert', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");

    // DO NOTHING should silently skip
    db.execute("INSERT INTO t VALUES (1, 'second') ON CONFLICT (id) DO NOTHING");
    const r = rows(db.execute('SELECT val FROM t'));
    assert.equal(r[0].val, 'first');
  });

  it('UPSERT rollback on constraint failure preserves original row', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT CHECK (score >= 0 AND score <= 100))');
    db.execute('INSERT INTO t VALUES (1, 85)');

    // Try to update score to invalid value
    assert.throws(() => {
      db.execute('INSERT INTO t VALUES (1, 150) ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score');
    }, /CHECK/i);

    // Original row should be intact
    const r = rows(db.execute('SELECT score FROM t'));
    assert.equal(r[0].score, 85, 'Original score should be preserved after failed UPSERT');
  });

  it('UPSERT DO UPDATE with UNIQUE constraint on non-PK column', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
    db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");

    // Try to UPSERT id=1 with bob's email — should fail UNIQUE
    assert.throws(() => {
      db.execute("INSERT INTO t VALUES (1, 'bob@test.com') ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email");
    }, /UNIQUE|duplicate/i);

    // Both rows should be unchanged
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r[0].email, 'alice@test.com');
    assert.equal(r[1].email, 'bob@test.com');
  });
});
