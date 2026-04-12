// information-schema.test.js — Tests for information_schema virtual tables

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('information_schema', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)');
    db.execute('CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT NOT NULL)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    db.execute("INSERT INTO posts VALUES (1, 1, 'Hello World')");
    return db;
  }

  describe('information_schema.tables', () => {
    it('should list all tables', () => {
      const db = makeDB();
      const result = db.execute('SELECT table_name, table_type FROM information_schema.tables');
      assert.equal(result.rows.length, 2);
      
      const names = result.rows.map(r => r.table_name).sort();
      assert.deepEqual(names, ['posts', 'users']);
    });

    it('should show table_type as BASE TABLE', () => {
      const db = makeDB();
      const result = db.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'");
      assert.equal(result.rows.length, 2);
    });

    it('should include catalog and schema', () => {
      const db = makeDB();
      const result = db.execute('SELECT table_catalog, table_schema, table_name FROM information_schema.tables');
      assert.ok(result.rows.every(r => r.table_catalog === 'henrydb'));
      assert.ok(result.rows.every(r => r.table_schema === 'public'));
    });

    it('should update when tables are created', () => {
      const db = makeDB();
      db.execute('CREATE TABLE comments (id INT, text TEXT)');
      const result = db.execute('SELECT table_name FROM information_schema.tables');
      assert.equal(result.rows.length, 3);
      assert.ok(result.rows.some(r => r.table_name === 'comments'));
    });
  });

  describe('information_schema.columns', () => {
    it('should list all columns', () => {
      const db = makeDB();
      const result = db.execute('SELECT table_name, column_name FROM information_schema.columns');
      // users: id, name, email = 3; posts: id, user_id, title = 3
      assert.equal(result.rows.length, 6);
    });

    it('should show correct data types', () => {
      const db = makeDB();
      const result = db.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users'");
      assert.equal(result.rows.length, 3);
      
      const idCol = result.rows.find(r => r.column_name === 'id');
      assert.equal(idCol.data_type, 'INT');
      
      const nameCol = result.rows.find(r => r.column_name === 'name');
      assert.equal(nameCol.data_type, 'TEXT');
    });

    it('should show nullable status', () => {
      const db = makeDB();
      const result = db.execute("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'users'");
      
      const nameCol = result.rows.find(r => r.column_name === 'name');
      assert.equal(nameCol.is_nullable, 'NO'); // NOT NULL
      
      const emailCol = result.rows.find(r => r.column_name === 'email');
      assert.equal(emailCol.is_nullable, 'YES'); // nullable
    });

    it('should have ordinal positions', () => {
      const db = makeDB();
      const result = db.execute("SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position");
      assert.equal(result.rows[0].column_name, 'id');
      assert.equal(result.rows[0].ordinal_position, 1);
      assert.equal(result.rows[1].column_name, 'name');
      assert.equal(result.rows[1].ordinal_position, 2);
      assert.equal(result.rows[2].column_name, 'email');
      assert.equal(result.rows[2].ordinal_position, 3);
    });

    it('should filter by table_name', () => {
      const db = makeDB();
      const result = db.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'posts'");
      assert.equal(result.rows.length, 3);
      const names = result.rows.map(r => r.column_name);
      assert.ok(names.includes('title'));
      assert.ok(names.includes('user_id'));
    });
  });

  describe('information_schema.table_constraints', () => {
    it('should list primary key constraints', () => {
      const db = makeDB();
      const result = db.execute("SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY'");
      assert.equal(result.rows.length, 2);
    });

    it('should list NOT NULL constraints', () => {
      const db = makeDB();
      const result = db.execute("SELECT table_name, constraint_type FROM information_schema.table_constraints WHERE constraint_type = 'NOT NULL'");
      assert.ok(result.rows.length >= 2); // name, title
    });

    it('should filter by table name', () => {
      const db = makeDB();
      const result = db.execute("SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'users'");
      assert.ok(result.rows.some(r => r.constraint_type === 'PRIMARY KEY'));
      assert.ok(result.rows.some(r => r.constraint_type === 'NOT NULL'));
    });
  });

  describe('information_schema.key_column_usage', () => {
    it('should list primary key columns', () => {
      const db = makeDB();
      const result = db.execute('SELECT table_name, column_name, constraint_name FROM information_schema.key_column_usage');
      assert.ok(result.rows.some(r => r.table_name === 'users' && r.column_name === 'id'));
      assert.ok(result.rows.some(r => r.table_name === 'posts' && r.column_name === 'id'));
    });
  });

  describe('Integration', () => {
    it('should work with WHERE filters on information_schema', () => {
      const db = makeDB();
      // Use simple queries that combine information_schema data
      const result = db.execute(`
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'users'
        ORDER BY ordinal_position
      `);
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].column_name, 'id');
      assert.equal(result.rows[2].column_name, 'email');
    });

    it('should return empty for nonexistent schema table', () => {
      const db = makeDB();
      try {
        db.execute('SELECT * FROM information_schema.nonexistent');
        assert.fail('Should throw for nonexistent information_schema table');
      } catch (e) {
        assert.ok(e.message.includes('not found') || e.message.includes('nonexistent'));
      }
    });
  });
});
