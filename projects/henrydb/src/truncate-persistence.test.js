// truncate-persistence.test.js — Verify TRUNCATE TABLE survives crash-and-recover
// This tests the fix for: WAL recovery restoring truncated rows after close/reopen.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-truncate-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true }); } catch {}
}

describe('TRUNCATE TABLE Persistence', () => {
  it('truncated table stays empty after WAL recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.execute("INSERT INTO t VALUES (3, 'Carol')");
      assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);

      db.execute('TRUNCATE TABLE t');
      assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 0);
      db.close();

      // Recover — table should still be empty
      const db2 = Database.recover(dir);
      assert.strictEqual(db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 0,
        'TRUNCATE should persist through WAL recovery');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('rows inserted after TRUNCATE survive recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      db.execute('INSERT INTO t VALUES (2, 200)');
      db.execute('INSERT INTO t VALUES (3, 300)');

      db.execute('TRUNCATE TABLE t');
      db.execute('INSERT INTO t VALUES (4, 400)');
      db.execute('INSERT INTO t VALUES (5, 500)');
      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 2, 'Only post-truncate rows should survive');
      assert.strictEqual(rows[0].id, 4);
      assert.strictEqual(rows[1].id, 5);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('multiple truncates are handled correctly', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');

      // First batch + truncate
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("INSERT INTO t VALUES (2, 'b')");
      db.execute('TRUNCATE TABLE t');

      // Second batch + truncate
      db.execute("INSERT INTO t VALUES (3, 'c')");
      db.execute("INSERT INTO t VALUES (4, 'd')");
      db.execute('TRUNCATE TABLE t');

      // Third batch (survives)
      db.execute("INSERT INTO t VALUES (5, 'e')");
      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].id, 5);
      assert.strictEqual(rows[0].name, 'e');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('TRUNCATE on one table does not affect another', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t1 VALUES (1, 'a')");
      db.execute("INSERT INTO t1 VALUES (2, 'b')");
      db.execute("INSERT INTO t2 VALUES (1, 'x')");
      db.execute("INSERT INTO t2 VALUES (2, 'y')");

      db.execute('TRUNCATE TABLE t1');
      db.close();

      const db2 = Database.recover(dir);
      assert.strictEqual(db2.execute('SELECT COUNT(*) as cnt FROM t1').rows[0].cnt, 0,
        't1 should be empty after truncate + recovery');
      assert.strictEqual(db2.execute('SELECT COUNT(*) as cnt FROM t2').rows[0].cnt, 2,
        't2 should be unaffected by t1 truncate');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('TRUNCATE with TRUNCATE TABLE syntax (alternate AST path)', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
      db.execute("INSERT INTO items VALUES (1, 'Widget', 10)");
      db.execute("INSERT INTO items VALUES (2, 'Gadget', 20)");

      // Use TRUNCATE TABLE (the other AST path)
      db.execute('TRUNCATE TABLE items');
      db.execute("INSERT INTO items VALUES (3, 'Doohickey', 30)");
      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM items ORDER BY id').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].name, 'Doohickey');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('in-memory truncate still works (no persistence)', () => {
    const db = new Database(); // No dataDir
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('TRUNCATE TABLE t');
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 0);
    db.execute('INSERT INTO t VALUES (3)');
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
  });
});
