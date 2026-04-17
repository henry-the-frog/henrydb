// fk-depth.test.js — Foreign key constraint depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-fk-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('FK INSERT Enforcement', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('rejects INSERT with invalid FK reference', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id))');
    db.execute("INSERT INTO parents VALUES (1, 'Alice')");

    assert.throws(
      () => db.execute('INSERT INTO children VALUES (1, 999)'),
      /foreign|reference|constraint/i
    );
  });

  it('allows INSERT with valid FK reference', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id))');
    db.execute("INSERT INTO parents VALUES (1, 'Alice')");
    db.execute('INSERT INTO children VALUES (1, 1)');

    const r = rows(db.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
  });

  it('allows NULL FK value', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id))');
    db.execute('INSERT INTO children VALUES (1, NULL)');

    const r = rows(db.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
    assert.equal(r[0].parent_id, null);
  });
});

describe('FK DELETE CASCADE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CASCADE deletes child rows', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute("INSERT INTO parents VALUES (1, 'Alice')");
    db.execute("INSERT INTO parents VALUES (2, 'Bob')");
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');
    db.execute('INSERT INTO children VALUES (3, 2)');

    db.execute('DELETE FROM parents WHERE id = 1');

    const children = rows(db.execute('SELECT * FROM children'));
    assert.equal(children.length, 1);
    assert.equal(children[0].parent_id, 2);
  });
});

describe('FK DELETE SET NULL', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SET NULL nullifies FK on parent delete', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id) ON DELETE SET NULL)');
    db.execute("INSERT INTO parents VALUES (1, 'Alice')");
    db.execute('INSERT INTO children VALUES (1, 1)');

    db.execute('DELETE FROM parents WHERE id = 1');

    const r = rows(db.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
    assert.equal(r[0].parent_id, null);
  });
});

describe('FK DELETE RESTRICT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('RESTRICT prevents parent deletion with children', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id) ON DELETE RESTRICT)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');

    assert.throws(
      () => db.execute('DELETE FROM parents WHERE id = 1'),
      /restrict|foreign|constraint|referenced/i
    );
  });

  it('RESTRICT allows deletion when no children exist', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id) ON DELETE RESTRICT)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO parents VALUES (2)');
    db.execute('INSERT INTO children VALUES (1, 1)');

    // Delete parent 2 (no children) should work
    db.execute('DELETE FROM parents WHERE id = 2');
    const r = rows(db.execute('SELECT * FROM parents'));
    assert.equal(r.length, 1);
  });
});
