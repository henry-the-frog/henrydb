// db-dump.test.js — Tests for database dump/restore
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { dump, restore, clone } from './db-dump.js';
import { Database } from './db.js';

describe('Database Dump/Restore', () => {
  function setupDb() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)');
    db.execute("INSERT INTO posts VALUES (1, 1, 'Hello World')");
    db.execute("INSERT INTO posts VALUES (2, 2, 'My Post')");
    return db;
  }

  describe('dump', () => {
    it('generates SQL script with schema and data', () => {
      const db = setupDb();
      const script = dump(db);
      assert.ok(script.includes('CREATE TABLE users'));
      assert.ok(script.includes('CREATE TABLE posts'));
      assert.ok(script.includes("INSERT INTO users"));
      assert.ok(script.includes("'Alice'"));
      assert.ok(script.includes("'Hello World'"));
    });

    it('schema-only mode', () => {
      const db = setupDb();
      const script = dump(db, { schemaOnly: true });
      assert.ok(script.includes('CREATE TABLE'));
      assert.ok(!script.includes('INSERT INTO'));
    });

    it('data-only mode', () => {
      const db = setupDb();
      const script = dump(db, { dataOnly: true });
      assert.ok(!script.includes('CREATE TABLE'));
      assert.ok(script.includes('INSERT INTO'));
    });

    it('filters specific tables', () => {
      const db = setupDb();
      const script = dump(db, { tables: ['users'] });
      assert.ok(script.includes('users'));
      assert.ok(!script.includes('CREATE TABLE posts'));
    });

    it('includes DROP TABLE with dropExisting', () => {
      const db = setupDb();
      const script = dump(db, { dropExisting: true });
      assert.ok(script.includes('DROP TABLE IF EXISTS'));
    });

    it('handles NULL values', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute('INSERT INTO t VALUES (1, NULL)');
      const script = dump(db);
      assert.ok(script.includes('NULL'));
    });

    it('handles special characters in data', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello world')");
      const script = dump(db);
      assert.ok(script.includes('hello world'));
    });
  });

  describe('restore', () => {
    it('restores from dump script', () => {
      const source = setupDb();
      const script = dump(source);
      
      const target = new Database();
      const result = restore(target, script);
      assert.ok(result.statements > 0);
      
      const users = target.execute('SELECT * FROM users ORDER BY id');
      assert.equal(users.rows.length, 2);
      assert.equal(users.rows[0].name, 'Alice');
    });

    it('reports errors for invalid SQL', () => {
      const db = new Database();
      const result = restore(db, 'CREATE TABLE t (id INT);\nINVALID STUFF;\nSELECT 1;');
      assert.ok(result.errors.length > 0);
    });
  });

  describe('clone', () => {
    it('clones database completely', () => {
      const source = setupDb();
      const target = new Database();
      
      const result = clone(source, target);
      assert.equal(result.tables, 2);
      assert.equal(result.rows, 4); // 2 users + 2 posts
    });

    it('cloned data matches source', () => {
      const source = setupDb();
      const target = new Database();
      clone(source, target);
      
      const sourceUsers = source.execute('SELECT * FROM users ORDER BY id').rows;
      const targetUsers = target.execute('SELECT * FROM users ORDER BY id').rows;
      assert.deepEqual(sourceUsers, targetUsers);
    });
  });

  describe('round-trip', () => {
    it('dump → restore preserves all data', () => {
      const db = new Database();
      db.execute('CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, value REAL, active INTEGER)');
      for (let i = 0; i < 50; i++) {
        db.execute(`INSERT INTO metrics VALUES (${i}, 'metric_${i}', ${i * 1.5}, ${i % 2})`);
      }
      
      const script = dump(db);
      const restored = new Database();
      restore(restored, script);
      
      assert.equal(
        restored.execute('SELECT COUNT(*) as cnt FROM metrics').rows[0].cnt,
        50
      );
      assert.equal(
        restored.execute('SELECT SUM(value) as total FROM metrics').rows[0].total,
        db.execute('SELECT SUM(value) as total FROM metrics').rows[0].total
      );
    });
  });
});
