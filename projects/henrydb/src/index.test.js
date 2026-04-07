// index.test.js — Secondary index tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Secondary Indexes', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, email TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'alice@test.com')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'bob@test.com')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@test.com')");
    db.execute("INSERT INTO users VALUES (4, 'Diana', 28, 'diana@test.com')");
    db.execute("INSERT INTO users VALUES (5, 'Eve', 30, 'eve@test.com')");
  });

  describe('CREATE INDEX', () => {
    it('creates index on non-PK column', () => {
      const result = db.execute('CREATE INDEX idx_name ON users (name)');
      assert.equal(result.type, 'OK');
      assert.match(result.message, /idx_name/);
    });

    it('creates unique index', () => {
      const result = db.execute('CREATE UNIQUE INDEX idx_email ON users (email)');
      assert.equal(result.type, 'OK');
    });

    it('unique index rejects duplicates', () => {
      db.execute("INSERT INTO users VALUES (6, 'Frank', 30, 'alice@test.com')");
      assert.throws(() => {
        db.execute('CREATE UNIQUE INDEX idx_email ON users (email)');
      }, /Duplicate key/);
    });

    it('errors on non-existent column', () => {
      assert.throws(() => {
        db.execute('CREATE INDEX idx_bad ON users (nonexistent)');
      }, /not found/);
    });

    it('errors on non-existent table', () => {
      assert.throws(() => {
        db.execute('CREATE INDEX idx_bad ON ghost (col)');
      }, /not found/);
    });
  });

  describe('DROP INDEX', () => {
    it('drops existing index', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      const result = db.execute('DROP INDEX idx_name');
      assert.equal(result.type, 'OK');
    });

    it('errors on non-existent index', () => {
      assert.throws(() => {
        db.execute('DROP INDEX idx_ghost');
      }, /not found/);
    });

    it('dropped index no longer used', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      db.execute('DROP INDEX idx_name');
      // Query should still work (full scan fallback)
      const result = db.execute("SELECT * FROM users WHERE name = 'Alice'");
      assert.equal(result.rows.length, 1);
    });
  });

  describe('Index-accelerated queries', () => {
    it('equality lookup uses index', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      const result = db.execute("SELECT * FROM users WHERE name = 'Alice'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('equality on PK uses index', () => {
      const result = db.execute('SELECT * FROM users WHERE id = 3');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Charlie');
    });

    it('AND with indexed column', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      const result = db.execute("SELECT * FROM users WHERE age = 30 AND name = 'Alice'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('no false matches on index lookup', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      const result = db.execute("SELECT * FROM users WHERE name = 'Zara'");
      assert.equal(result.rows.length, 0);
    });

    it('returns correct results after insert', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      db.execute("INSERT INTO users VALUES (6, 'Frank', 22, 'frank@test.com')");
      const result = db.execute("SELECT * FROM users WHERE name = 'Frank'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].age, 22);
    });
  });

  describe('Index maintenance', () => {
    it('new inserts are indexed', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      db.execute("INSERT INTO users VALUES (6, 'Frank', 22, 'frank@test.com')");
      const result = db.execute("SELECT * FROM users WHERE name = 'Frank'");
      assert.equal(result.rows.length, 1);
    });

    it('multiple indexes maintained together', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      db.execute('CREATE INDEX idx_age ON users (age)');
      db.execute("INSERT INTO users VALUES (6, 'Frank', 40, 'frank@test.com')");

      const byName = db.execute("SELECT * FROM users WHERE name = 'Frank'");
      assert.equal(byName.rows.length, 1);

      const byAge = db.execute('SELECT * FROM users WHERE age = 40');
      assert.equal(byAge.rows.length, 1);
      assert.equal(byAge.rows[0].name, 'Frank');
    });

    it('drop table removes indexes', () => {
      db.execute('CREATE INDEX idx_name ON users (name)');
      db.execute('DROP TABLE users');
      assert.equal(db.indexCatalog.size, 0);
    });
  });

  describe('Query planner integration', () => {
    it('full scan without index works', () => {
      const result = db.execute("SELECT * FROM users WHERE name = 'Bob'");
      assert.equal(result.rows.length, 1);
    });

    it('index scan and full scan return same results', () => {
      // Before index
      const before = db.execute('SELECT * FROM users WHERE age = 30 ORDER BY id');

      // After index
      db.execute('CREATE INDEX idx_age ON users (age)');
      const after = db.execute('SELECT * FROM users WHERE age = 30 ORDER BY id');

      assert.deepEqual(before.rows, after.rows);
    });

    it('ORDER BY works with index scan', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      const result = db.execute('SELECT * FROM users WHERE age = 30 ORDER BY name');
      assert.equal(result.rows.length, 2);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(result.rows[1].name, 'Eve');
    });

    it('LIMIT works with index scan', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      const result = db.execute('SELECT * FROM users WHERE age = 30 LIMIT 1');
      assert.equal(result.rows.length, 1);
    });

    it('aggregates work with index scan', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      const result = db.execute('SELECT COUNT(*) AS cnt FROM users WHERE age = 30');
      assert.equal(result.rows[0].cnt, 2);
    });
  });

  describe('Edge cases', () => {
    it('index on column with nulls', () => {
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT)');
      db.execute("INSERT INTO items VALUES (1, 'A')");
      db.execute('INSERT INTO items VALUES (2, NULL)');
      db.execute("INSERT INTO items VALUES (3, 'A')");
      db.execute('CREATE INDEX idx_cat ON items (category)');

      const result = db.execute("SELECT * FROM items WHERE category = 'A'");
      assert.equal(result.rows.length, 2);
    });

    it('empty table index creation', () => {
      db.execute('CREATE TABLE empty_t (id INT PRIMARY KEY, val TEXT)');
      const result = db.execute('CREATE INDEX idx_val ON empty_t (val)');
      assert.equal(result.type, 'OK');
    });

    it('index survives many inserts', () => {
      db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE INDEX idx_val ON nums (val)');
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO nums VALUES (${i}, ${i % 10})`);
      }
      const result = db.execute('SELECT * FROM nums WHERE val = 5');
      assert.equal(result.rows.length, 10);
    });
  });
});
