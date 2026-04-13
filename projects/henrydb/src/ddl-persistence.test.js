// ddl-persistence.test.js — Verify ALTER TABLE, RENAME TABLE persist through WAL recovery

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-ddl-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true }); } catch {}
}

describe('DDL Persistence: ALTER TABLE', () => {
  it('ADD COLUMN survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('ALTER TABLE t ADD COLUMN age INT');
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.close();

      const db2 = Database.recover(dir);
      const schema = db2.tables.get('t').schema.map(c => c.name);
      assert.deepStrictEqual(schema, ['id', 'name', 'age']);
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].age, null); // Pre-ALTER row gets NULL
      db2.close();
    } finally { cleanup(dir); }
  });

  it('DROP COLUMN survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, extra TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice', 'x')");
      db.execute('ALTER TABLE t DROP COLUMN extra');
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.close();

      const db2 = Database.recover(dir);
      const schema = db2.tables.get('t').schema.map(c => c.name);
      assert.deepStrictEqual(schema, ['id', 'name']);
      const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
      assert.strictEqual(rows.length, 2);
      assert.ok(!('extra' in rows[0]), 'extra column should not exist');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('RENAME COLUMN survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('ALTER TABLE t RENAME COLUMN name TO full_name');
      db.close();

      const db2 = Database.recover(dir);
      const schema = db2.tables.get('t').schema.map(c => c.name);
      assert.ok(schema.includes('full_name'), 'Column should be renamed');
      assert.ok(!schema.includes('name'), 'Old column name should not exist');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('RENAME TABLE survives recovery', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE old_name (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO old_name VALUES (1, 'test')");
      db.execute('ALTER TABLE old_name RENAME TO new_name');
      db.close();

      const db2 = Database.recover(dir);
      assert.throws(() => db2.execute('SELECT * FROM old_name'), /not found/i);
      const rows = db2.execute('SELECT * FROM new_name').rows;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].val, 'test');
      db2.close();
    } finally { cleanup(dir); }
  });

  it('multiple ALTER TABLE operations in sequence', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a1', 'b1', 'c1')");
      db.execute('ALTER TABLE t DROP COLUMN b');
      db.execute('ALTER TABLE t ADD COLUMN d INT');
      db.execute('ALTER TABLE t RENAME COLUMN a TO alpha');
      db.close();

      const db2 = Database.recover(dir);
      const schema = db2.tables.get('t').schema.map(c => c.name);
      assert.ok(schema.includes('alpha'));
      assert.ok(schema.includes('c'));
      assert.ok(schema.includes('d'));
      assert.ok(!schema.includes('a'));
      assert.ok(!schema.includes('b'));
      db2.close();
    } finally { cleanup(dir); }
  });

  it('ALTER TABLE + TRUNCATE + DROP combo', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
      db.execute("INSERT INTO t1 VALUES (1, 'x')");
      db.execute('INSERT INTO t2 VALUES (1, 100)');

      db.execute('ALTER TABLE t1 ADD COLUMN email TEXT');
      db.execute('TRUNCATE TABLE t2');
      db.execute('INSERT INTO t2 VALUES (2, 200)');
      db.execute('ALTER TABLE t1 RENAME TO contacts');
      db.close();

      const db2 = Database.recover(dir);
      // t1 renamed to contacts with email column
      assert.throws(() => db2.execute('SELECT * FROM t1'), /not found/i);
      const contacts = db2.execute('SELECT * FROM contacts').rows;
      assert.strictEqual(contacts.length, 1);
      assert.ok('email' in contacts[0]);
      // t2 truncated with new data
      const t2rows = db2.execute('SELECT * FROM t2').rows;
      assert.strictEqual(t2rows.length, 1);
      assert.strictEqual(t2rows[0].val, 200);
      db2.close();
    } finally { cleanup(dir); }
  });
});
