// schema-diff.test.js — Tests for schema diff tool
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { diffSchemas, extractSchemaMap, diffDatabases } from './schema-diff.js';
import { Database } from './db.js';

describe('Schema Diff', () => {
  describe('diffSchemas', () => {
    it('detects added table', () => {
      const d = diffSchemas({}, { users: { id: 'INTEGER', name: 'TEXT' } });
      assert.equal(d.addedTables.length, 1);
      assert.equal(d.addedTables[0], 'users');
      assert.ok(d.sql.some(s => s.includes('CREATE TABLE users')));
    });

    it('detects removed table', () => {
      const d = diffSchemas({ old: { id: 'INTEGER' } }, {});
      assert.equal(d.removedTables.length, 1);
      assert.ok(d.sql.some(s => s.includes('DROP TABLE old')));
    });

    it('detects added column', () => {
      const d = diffSchemas(
        { users: { id: 'INTEGER', name: 'TEXT' } },
        { users: { id: 'INTEGER', name: 'TEXT', email: 'TEXT' } }
      );
      assert.equal(d.addedColumns.length, 1);
      assert.equal(d.addedColumns[0].column, 'email');
      assert.ok(d.sql.some(s => s.includes('ADD COLUMN email')));
    });

    it('detects removed column', () => {
      const d = diffSchemas(
        { users: { id: 'INTEGER', name: 'TEXT', old_col: 'TEXT' } },
        { users: { id: 'INTEGER', name: 'TEXT' } }
      );
      assert.equal(d.removedColumns.length, 1);
      assert.equal(d.removedColumns[0].column, 'old_col');
    });

    it('detects type change', () => {
      const d = diffSchemas(
        { t: { val: 'INTEGER' } },
        { t: { val: 'REAL' } }
      );
      assert.equal(d.modifiedColumns.length, 1);
      assert.equal(d.modifiedColumns[0].from, 'INTEGER');
      assert.equal(d.modifiedColumns[0].to, 'REAL');
    });

    it('no diff for identical schemas', () => {
      const schema = { t: { id: 'INTEGER', name: 'TEXT' } };
      const d = diffSchemas(schema, schema);
      assert.equal(d.addedTables.length, 0);
      assert.equal(d.removedTables.length, 0);
      assert.equal(d.addedColumns.length, 0);
      assert.equal(d.removedColumns.length, 0);
      assert.equal(d.modifiedColumns.length, 0);
      assert.equal(d.sql.length, 0);
    });

    it('handles complex diff', () => {
      const source = {
        users: { id: 'INTEGER', name: 'TEXT' },
        old_table: { x: 'INTEGER' },
      };
      const target = {
        users: { id: 'INTEGER', name: 'TEXT', email: 'TEXT' },
        new_table: { id: 'INTEGER', val: 'REAL' },
      };
      const d = diffSchemas(source, target);
      assert.equal(d.addedTables.length, 1);
      assert.equal(d.removedTables.length, 1);
      assert.equal(d.addedColumns.length, 1);
    });
  });

  describe('extractSchemaMap', () => {
    it('extracts schema from Database', () => {
      const db = new Database();
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)');
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      
      const schema = extractSchemaMap(db);
      assert.ok(schema.users);
      assert.equal(schema.users.id, 'INTEGER');
      assert.equal(schema.users.name, 'TEXT');
      assert.equal(schema.users.score, 'REAL');
    });
  });

  describe('diffDatabases', () => {
    it('diffs two Database instances', () => {
      const db1 = new Database();
      db1.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
      db1.execute("INSERT INTO users VALUES (1, 'Alice')");
      
      const db2 = new Database();
      db2.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
      db2.execute("INSERT INTO users VALUES (1, 'Alice')");
      db2.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)');
      db2.execute("INSERT INTO posts VALUES (1, 'Hello')");
      
      const d = diffDatabases(db1, db2);
      assert.equal(d.addedTables.length, 1);
      assert.equal(d.addedTables[0], 'posts');
    });
  });
});
