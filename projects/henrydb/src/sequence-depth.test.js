// sequence-depth.test.js — SEQUENCE/AUTOINCREMENT depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-seq-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('AUTOINCREMENT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('auto-generates sequential IDs', () => {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");
    db.execute("INSERT INTO t (name) VALUES ('Carol')");

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 2);
    assert.equal(r[2].id, 3);
  });

  it('auto-increment continues after DELETE', () => {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute("INSERT INTO t (name) VALUES ('Carol')");

    const r = rows(db.execute('SELECT id FROM t ORDER BY id'));
    // Carol should get id=3, not reuse id=2
    assert.equal(r[1].id, 3, 'Auto-increment should not reuse deleted IDs');
  });

  it('auto-increment with explicit ID', () => {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
    db.execute("INSERT INTO t VALUES (10, 'Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");

    const r = rows(db.execute('SELECT id FROM t ORDER BY id'));
    assert.equal(r[0].id, 10);
    // Bob should get id > 10
    assert.ok(r[1].id > 10, `Auto-increment should continue after explicit ID: got ${r[1].id}`);
  });

  it('auto-increment survives crash', () => {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    db.execute("INSERT INTO t (name) VALUES ('Carol')");
    const r = rows(db.execute('SELECT id FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    // Carol should get id=3 or higher (not reuse 1 or 2)
    assert.ok(r[2].id >= 3, `Auto-increment after recovery should continue: got ${r[2].id}`);
  });
});

describe('SERIAL type', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SERIAL generates sequential IDs', () => {
    db.execute('CREATE TABLE t (id SERIAL, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.ok(r[0].id < r[1].id, 'IDs should be sequential');
  });
});

describe('CREATE SEQUENCE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic sequence usage', () => {
    db.execute('CREATE SEQUENCE myseq');
    
    const r1 = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));
    const r2 = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));
    const r3 = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));

    assert.equal(r1[0].v, 1);
    assert.equal(r2[0].v, 2);
    assert.equal(r3[0].v, 3);
  });

  it('sequence with custom START', () => {
    db.execute('CREATE SEQUENCE myseq START WITH 100');
    
    const r = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));
    assert.equal(r[0].v, 100);
  });

  it('sequence with INCREMENT', () => {
    db.execute('CREATE SEQUENCE myseq START WITH 0 INCREMENT BY 5');
    
    const r1 = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));
    const r2 = rows(db.execute("SELECT NEXTVAL('myseq') AS v"));
    
    assert.equal(r1[0].v, 0);
    assert.equal(r2[0].v, 5);
  });
});
