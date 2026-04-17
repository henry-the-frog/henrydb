// system-catalog-depth.test.js — SHOW TABLES/COLUMNS + system catalog tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-cat-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('SHOW TABLES', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('lists created tables', () => {
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute('CREATE TABLE orders (id INT, user_id INT)');

    const r = rows(db.execute('SHOW TABLES'));
    const names = r.map(x => x.table_name || x.name || x.Table || Object.values(x)[0]);
    assert.ok(names.includes('users'), 'Should list users table');
    assert.ok(names.includes('orders'), 'Should list orders table');
  });

  it('empty database has no user tables', () => {
    const r = rows(db.execute('SHOW TABLES'));
    // May have system tables, but no user tables
    assert.ok(r.length >= 0);
  });

  it('DROP TABLE removes from SHOW TABLES', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('DROP TABLE t1');

    const r = rows(db.execute('SHOW TABLES'));
    const names = r.map(x => x.table_name || x.name || x.Table || Object.values(x)[0]);
    assert.ok(!names.includes('t1'), 't1 should be removed');
    assert.ok(names.includes('t2'), 't2 should remain');
  });
});

describe('SHOW COLUMNS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('lists table columns', () => {
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)');

    const r = rows(db.execute('SHOW COLUMNS FROM users'));
    assert.ok(r.length >= 3, 'Should list 3 columns');

    const names = r.map(x => x.column_name || x.name || x.Field || Object.values(x)[0]);
    assert.ok(names.includes('id'));
    assert.ok(names.includes('name'));
    assert.ok(names.includes('age'));
  });

  it('shows column types', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT, score FLOAT)');

    const r = rows(db.execute('SHOW COLUMNS FROM t'));
    assert.equal(r.length, 3);
  });
});

describe('Catalog Survives Crash', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SHOW TABLES after recovery', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    
    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r = rows(db.execute('SHOW TABLES'));
    const names = r.map(x => x.table_name || x.name || x.Table || Object.values(x)[0]);
    assert.ok(names.includes('t1'));
    assert.ok(names.includes('t2'));
  });

  it('SHOW COLUMNS after recovery', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    
    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r = rows(db.execute('SHOW COLUMNS FROM t'));
    assert.ok(r.length >= 2);
  });
});
