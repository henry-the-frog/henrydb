// index-persistence.test.js — Verify CREATE/DROP INDEX persist through WAL recovery

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-idx-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true }); } catch {}
}

describe('Index Persistence', () => {
  it('CREATE INDEX survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
      db.execute("INSERT INTO t VALUES (1, 'a', 10)");
      db.execute("INSERT INTO t VALUES (2, 'b', 20)");
      db.execute('CREATE INDEX idx_val ON t (val)');
      db.close();

      const db2 = Database.recover(dir);
      // Index should exist after recovery
      const table = db2.tables.get('t');
      assert.ok(table.indexes.has('val'), 'Index on val should exist');
      // Data should be accessible
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 2);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('DROP INDEX survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 100)');
      db.execute('CREATE INDEX idx_val ON t (val)');
      db.execute('DROP INDEX idx_val');
      db.close();

      const db2 = Database.recover(dir);
      // Index should NOT exist after recovery
      const table = db2.tables.get('t');
      assert.ok(!table.indexes.has('val') || table.indexes.get('val') === undefined, 'Dropped index should not exist');
      // Data intact
      assert.strictEqual(db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('CREATE UNIQUE INDEX survives recovery with constraint', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a@test.com')");
      db.execute("INSERT INTO t VALUES (2, 'b@test.com')");
      db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
      db.close();

      const db2 = Database.recover(dir);
      const table = db2.tables.get('t');
      assert.ok(table.indexes.has('email'), 'Unique index should exist');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('index + data modifications + recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE INDEX idx_val ON t (val)');
      for (let i = 0; i < 20; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      }
      db.execute('DELETE FROM t WHERE id >= 15');
      db.close();

      const db2 = Database.recover(dir);
      const cnt = db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
      assert.strictEqual(cnt, 15);
      db2.close();
    } finally { cleanup(dir); }
  });
});
