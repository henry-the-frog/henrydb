// alter-table.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ALTER TABLE', () => {
  describe('ADD COLUMN', () => {
    it('adds column with default value', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute('ALTER TABLE t ADD COLUMN age INT DEFAULT 25');
      const r = db.execute('SELECT * FROM t');
      assert.equal(r.rows[0].age, 25);
    });

    it('adds column with NULL default', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('ALTER TABLE t ADD email TEXT');
      const r = db.execute('SELECT * FROM t');
      assert.equal(r.rows[0].email, null);
    });

    it('rejects duplicate column name', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      assert.throws(() => db.execute('ALTER TABLE t ADD name TEXT'), /already exists/);
    });
  });

  describe('DROP COLUMN', () => {
    it('removes column and data', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
      db.execute("INSERT INTO t VALUES (1, 'test', 42)");
      db.execute('ALTER TABLE t DROP COLUMN val');
      const r = db.execute('SELECT * FROM t');
      assert.equal(Object.keys(r.rows[0]).length, 2);
      assert.equal(r.rows[0].val, undefined);
    });

    it('rejects dropping primary key', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      assert.throws(() => db.execute('ALTER TABLE t DROP COLUMN id'), /primary key/i);
    });
  });

  describe('RENAME COLUMN', () => {
    it('renames column', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, old_name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'test')");
      db.execute('ALTER TABLE t RENAME COLUMN old_name TO new_name');
      const r = db.execute('SELECT new_name FROM t');
      assert.equal(r.rows[0].new_name, 'test');
    });

    it('rejects renaming non-existent column', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      assert.throws(() => db.execute('ALTER TABLE t RENAME COLUMN fake TO real'), /not found/);
    });
  });
});
