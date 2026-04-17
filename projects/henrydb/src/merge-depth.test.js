// merge-depth.test.js — MERGE statement depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-mrg-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('MERGE MATCHED UPDATE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('updates matching rows', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE source (id INT, val INT)');
    db.execute('INSERT INTO target VALUES (1, 100)');
    db.execute('INSERT INTO target VALUES (2, 200)');
    db.execute('INSERT INTO source VALUES (1, 150)');
    db.execute('INSERT INTO source VALUES (3, 300)');

    db.execute(
      'MERGE INTO target t USING source s ON t.id = s.id ' +
      'WHEN MATCHED THEN UPDATE SET val = s.val ' +
      'WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)'
    );

    const r = rows(db.execute('SELECT * FROM target ORDER BY id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].val, 150); // Updated from 100 to 150
    assert.equal(r[1].val, 200); // Unchanged
    assert.equal(r[2].val, 300); // Inserted
  });
});

describe('MERGE NOT MATCHED INSERT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('inserts non-matching rows', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE source (id INT, name TEXT)');
    db.execute("INSERT INTO target VALUES (1, 'Alice')");
    db.execute("INSERT INTO source VALUES (2, 'Bob')");
    db.execute("INSERT INTO source VALUES (3, 'Carol')");

    db.execute(
      'MERGE INTO target t USING source s ON t.id = s.id ' +
      'WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)'
    );

    const r = rows(db.execute('SELECT * FROM target ORDER BY id'));
    assert.equal(r.length, 3);
  });
});

describe('MERGE idempotent', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('running MERGE twice is idempotent', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE source (id INT, val INT)');
    db.execute('INSERT INTO target VALUES (1, 100)');
    db.execute('INSERT INTO source VALUES (1, 200)');
    db.execute('INSERT INTO source VALUES (2, 300)');

    const sql = 'MERGE INTO target t USING source s ON t.id = s.id ' +
      'WHEN MATCHED THEN UPDATE SET val = s.val ' +
      'WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)';

    db.execute(sql);
    db.execute(sql); // Second time: 1 is already 200, 2 already exists

    const r = rows(db.execute('SELECT * FROM target ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].val, 200);
    assert.equal(r[1].val, 300);
  });
});
