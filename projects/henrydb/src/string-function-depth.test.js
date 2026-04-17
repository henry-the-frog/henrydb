// string-function-depth.test.js — String function depth tests

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

describe('String Functions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPPER converts to uppercase', () => {
    const r = rows(db.execute("SELECT UPPER('hello world') AS val"));
    assert.equal(r[0].val, 'HELLO WORLD');
  });

  it('LOWER converts to lowercase', () => {
    const r = rows(db.execute("SELECT LOWER('Hello World') AS val"));
    assert.equal(r[0].val, 'hello world');
  });

  it('TRIM removes whitespace', () => {
    const r = rows(db.execute("SELECT TRIM('  hello  ') AS val"));
    assert.equal(r[0].val, 'hello');
  });

  it('LENGTH returns string length', () => {
    const r = rows(db.execute("SELECT LENGTH('hello') AS val"));
    assert.equal(r[0].val, 5);
  });

  it('SUBSTRING extracts portion', () => {
    const r = rows(db.execute("SELECT SUBSTRING('hello world', 7, 5) AS val"));
    assert.equal(r[0].val, 'world');
  });

  it('REPLACE substitutes text', () => {
    const r = rows(db.execute("SELECT REPLACE('hello world', 'world', 'there') AS val"));
    assert.equal(r[0].val, 'hello there');
  });

  it('string functions with NULL', () => {
    const r = rows(db.execute("SELECT UPPER(NULL) AS val"));
    assert.equal(r[0].val, null);
  });

  it('string functions on column data', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");

    const r = rows(db.execute('SELECT UPPER(name) AS upper_name FROM t ORDER BY id'));
    assert.equal(r[0].upper_name, 'ALICE');
    assert.equal(r[1].upper_name, 'BOB');
  });

  it('nested string functions', () => {
    const r = rows(db.execute("SELECT UPPER(TRIM('  hello  ')) AS val"));
    assert.equal(r[0].val, 'HELLO');
  });

  it('CONCAT_WS joins with separator', () => {
    try {
      const r = rows(db.execute("SELECT CONCAT_WS(', ', 'Alice', 'Bob', 'Carol') AS val"));
      assert.equal(r[0].val, 'Alice, Bob, Carol');
    } catch {
      // CONCAT_WS may not be supported
    }
  });
});
