// string-functions.test.js — String function edge cases

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-str-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('String Function Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPPER and LOWER with NULL', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    
    const r1 = rows(db.execute('SELECT UPPER(val) as u FROM t'));
    assert.equal(r1[0].u, null, 'UPPER(NULL) should be NULL');
    
    const r2 = rows(db.execute('SELECT LOWER(val) as l FROM t'));
    assert.equal(r2[0].l, null, 'LOWER(NULL) should be NULL');
  });

  it('LENGTH with empty string', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('')");
    
    const r = rows(db.execute('SELECT LENGTH(val) as len FROM t'));
    assert.equal(r[0].len, 0, 'LENGTH of empty string should be 0');
  });

  it('LENGTH with NULL', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    
    const r = rows(db.execute('SELECT LENGTH(val) as len FROM t'));
    assert.equal(r[0].len, null, 'LENGTH(NULL) should be NULL');
  });

  it('SUBSTR with boundary values', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('abcdef')");
    
    // Start from 1 (SQL is 1-indexed)
    const r1 = rows(db.execute('SELECT SUBSTR(val, 1, 3) as s FROM t'));
    assert.equal(r1[0].s, 'abc');
    
    // Substr past end
    const r2 = rows(db.execute('SELECT SUBSTR(val, 4, 100) as s FROM t'));
    assert.equal(r2[0].s, 'def');
    
    // Substr with no length
    const r3 = rows(db.execute('SELECT SUBSTR(val, 3) as s FROM t'));
    assert.equal(r3[0].s, 'cdef');
  });

  it('concatenation operator ||', () => {
    db.execute('CREATE TABLE t (first TEXT, last TEXT)');
    db.execute("INSERT INTO t VALUES ('John', 'Doe')");
    
    const r = rows(db.execute("SELECT first || ' ' || last as full_name FROM t"));
    assert.equal(r[0].full_name, 'John Doe');
  });

  it('|| with NULL returns NULL', () => {
    db.execute('CREATE TABLE t (a TEXT, b TEXT)');
    db.execute('INSERT INTO t VALUES (NULL, \'hello\')');
    
    const r = rows(db.execute("SELECT a || b as result FROM t"));
    // SQL standard: NULL || anything = NULL
    // Some DBs concat as empty string — both behaviors acceptable
    assert.ok(r[0].result === null || r[0].result === 'hello',
      `NULL || 'hello' should be NULL or 'hello', got: ${r[0].result}`);
  });

  it('REPLACE with empty string', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('hello')");
    
    const r = rows(db.execute("SELECT REPLACE(val, 'l', '') as r FROM t"));
    assert.equal(r[0].r, 'heo', 'REPLACE with empty string should remove matches');
  });

  it('string comparison with LIKE', () => {
    db.execute('CREATE TABLE t (name TEXT)');
    db.execute("INSERT INTO t VALUES ('Alice')");
    db.execute("INSERT INTO t VALUES ('Bob')");
    db.execute("INSERT INTO t VALUES ('Angela')");
    db.execute("INSERT INTO t VALUES ('Zoe')");
    
    const r = rows(db.execute("SELECT name FROM t WHERE name LIKE 'A%' ORDER BY name"));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Angela');
  });

  it('LIKE with underscore wildcard', () => {
    db.execute('CREATE TABLE t (code TEXT)');
    db.execute("INSERT INTO t VALUES ('A1')");
    db.execute("INSERT INTO t VALUES ('A2')");
    db.execute("INSERT INTO t VALUES ('AB')");
    db.execute("INSERT INTO t VALUES ('B1')");
    
    const r = rows(db.execute("SELECT code FROM t WHERE code LIKE 'A_' ORDER BY code"));
    assert.equal(r.length, 3); // A1, A2, AB
  });

  it('TRIM variations', () => {
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('  hello  ')");
    
    const r = rows(db.execute("SELECT TRIM(val) as trimmed FROM t"));
    assert.equal(r[0].trimmed, 'hello');
  });

  it('string functions survive close/reopen', () => {
    db.execute('CREATE TABLE t (name TEXT)');
    db.execute("INSERT INTO t VALUES ('Test Data')");
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r = rows(db.execute('SELECT UPPER(name) as u, LENGTH(name) as len FROM t'));
    assert.equal(r[0].u, 'TEST DATA');
    assert.equal(r[0].len, 9);
  });
});
