// drop-table-persistence.test.js — Verify DROP TABLE survives crash-and-recover

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-drop-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true }); } catch {}
}

describe('DROP TABLE Persistence', () => {
  it('dropped table stays dropped after WAL recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.execute('DROP TABLE t');
      db.close();

      const db2 = Database.recover(dir);
      assert.throws(() => db2.execute('SELECT * FROM t'), /not found/i,
        'Dropped table should not exist after recovery');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('DROP TABLE IF EXISTS does not break recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('DROP TABLE IF EXISTS t');
      db.execute('DROP TABLE IF EXISTS nonexistent');
      db.close();

      const db2 = Database.recover(dir);
      assert.throws(() => db2.execute('SELECT * FROM t'), /not found/i);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('other tables survive when one is dropped', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE keep_me (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE drop_me (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO keep_me VALUES (1, 'stay')");
      db.execute("INSERT INTO drop_me VALUES (1, 'go')");
      db.execute('DROP TABLE drop_me');
      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM keep_me').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].val, 'stay');
      assert.throws(() => db2.execute('SELECT * FROM drop_me'), /not found/i);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('create → drop → recreate cycle persists correctly', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      // Create and populate
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'old')");
      // Drop
      db.execute('DROP TABLE t');
      // Recreate with different schema
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT, extra INT)');
      db.execute("INSERT INTO t VALUES (10, 'new', 42)");
      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].id, 10);
      assert.strictEqual(rows[0].val, 'new');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('DROP + TRUNCATE combo persists correctly', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t1 VALUES (1)');
      db.execute('INSERT INTO t1 VALUES (2)');
      db.execute('INSERT INTO t2 VALUES (1)');
      db.execute('INSERT INTO t2 VALUES (2)');

      db.execute('DROP TABLE t1');
      db.execute('TRUNCATE TABLE t2');
      db.execute('INSERT INTO t2 VALUES (3)');
      db.close();

      const db2 = Database.recover(dir);
      assert.throws(() => db2.execute('SELECT * FROM t1'), /not found/i);
      const rows = db2.execute('SELECT * FROM t2 ORDER BY id').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].id, 3);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('in-memory DROP TABLE still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('DROP TABLE t');
    assert.throws(() => db.execute('SELECT * FROM t'), /not found/i);
  });
});
