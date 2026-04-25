// string-truncation.test.js — Tests for string truncation fix (PAGE_SIZE 4096→32768)
// Verifies that large strings persist correctly and oversized rows throw errors.

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { Database } from './db.js';
import { PAGE_SIZE } from './disk-manager.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

let dirs = [];
function tmpDir() {
  const d = mkdtempSync(join(tmpdir(), 'henrydb-strtest-'));
  dirs.push(d);
  return d;
}

afterEach(() => {
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch {}
  }
  dirs = [];
});

describe('String truncation fix', () => {
  it('PAGE_SIZE is 32KB', () => {
    assert.equal(PAGE_SIZE, 32768, 'disk-manager PAGE_SIZE should be 32768');
  });

  it('in-memory DB handles large strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    const big = 'x'.repeat(20000);
    db.execute(`INSERT INTO t VALUES ('${big}')`);
    const r = db.execute('SELECT LENGTH(val) as len FROM t');
    assert.equal(r.rows[0].len, 20000);
  });

  it('persistent DB stores strings up to ~30KB', () => {
    const dir = tmpDir();
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE t (val TEXT)');

    for (const size of [100, 1000, 4076, 5000, 10000, 20000, 30000]) {
      db.execute('DELETE FROM t');
      db.execute(`INSERT INTO t VALUES ('${'a'.repeat(size)}')`);
      const r = db.execute('SELECT LENGTH(val) as len FROM t');
      assert.equal(r.rows.length, 1, `Row should exist for size ${size}`);
      assert.equal(r.rows[0].len, size, `String of size ${size} should roundtrip`);
    }
    db.close();
  });

  it('persistent DB roundtrips large strings across close/reopen', () => {
    const dir = tmpDir();
    const sizes = [5000, 10000, 20000];

    // Write
    const db1 = PersistentDatabase.open(dir);
    db1.execute('CREATE TABLE t (id INT, val TEXT)');
    for (const size of sizes) {
      db1.execute(`INSERT INTO t VALUES (${size}, '${'z'.repeat(size)}')`);
    }
    db1.close();

    // Reopen and verify
    const db2 = PersistentDatabase.open(dir);
    for (const size of sizes) {
      const r = db2.execute(`SELECT LENGTH(val) as len FROM t WHERE id = ${size}`);
      assert.equal(r.rows.length, 1, `Row with id=${size} should persist`);
      assert.equal(r.rows[0].len, size, `String of size ${size} should survive close/reopen`);
    }
    db2.close();
  });

  it('persistent DB throws on rows exceeding page capacity', () => {
    const dir = tmpDir();
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE t (val TEXT)');

    // A string of 40000 chars encodes to >32768 bytes
    assert.throws(
      () => db.execute(`INSERT INTO t VALUES ('${'x'.repeat(40000)}')`),
      /Row too large/,
      'Should throw for rows exceeding 32KB page'
    );
    db.close();
  });

  it('boundary: strings near old 4076-byte limit all persist', () => {
    const dir = tmpDir();
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT, val TEXT)');

    // These sizes used to silently fail with the old 4096 PAGE_SIZE
    for (const size of [4076, 4077, 4078, 4079, 4080, 4100, 4500, 5000]) {
      db.execute(`INSERT INTO t VALUES (${size}, '${'b'.repeat(size)}')`);
    }

    const r = db.execute('SELECT id, LENGTH(val) as len FROM t ORDER BY id');
    assert.equal(r.rows.length, 8, 'All 8 rows should be stored');
    for (const row of r.rows) {
      assert.equal(row.len, row.id, `String of size ${row.id} should be intact`);
    }
    db.close();
  });

  it('multiple large strings in same table', () => {
    const dir = tmpDir();
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE docs (id INT, body TEXT)');

    // Insert 10 strings of 10KB each (100KB total, needs multiple 32KB pages)
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO docs VALUES (${i}, '${'d'.repeat(10000)}')`);
    }

    const r = db.execute('SELECT COUNT(*) as cnt FROM docs');
    assert.equal(r.rows[0].cnt, 10);

    const r2 = db.execute('SELECT MIN(LENGTH(body)) as minl, MAX(LENGTH(body)) as maxl FROM docs');
    assert.equal(r2.rows[0].minl, 10000);
    assert.equal(r2.rows[0].maxl, 10000);

    db.close();
  });
});
