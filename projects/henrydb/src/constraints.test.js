// constraints.test.js — Tests for table constraints
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Table Constraints', () => {
  let db;

  beforeEach(() => {
    db = new Database();
  });

  describe('NOT NULL', () => {
    it('rejects NULL for NOT NULL column', () => {
      db.execute('CREATE TABLE t (id INT NOT NULL, name TEXT)');
      assert.throws(
        () => db.execute('INSERT INTO t VALUES (NULL, \'Alice\')'),
        /NOT NULL/
      );
    });

    it('accepts non-NULL values', () => {
      db.execute('CREATE TABLE t (id INT NOT NULL, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows.length, 1);
    });

    it('allows NULL for nullable columns', () => {
      db.execute('CREATE TABLE t (id INT NOT NULL, name TEXT)');
      db.execute('INSERT INTO t VALUES (1, NULL)');
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows[0].name, null);
    });
  });

  describe('PRIMARY KEY', () => {
    it('creates index on primary key column', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      const result = db.execute('SELECT * FROM t ORDER BY id');
      assert.equal(result.rows.length, 2);
    });
  });

  describe('CHECK constraint', () => {
    it('rejects values violating CHECK', () => {
      db.execute('CREATE TABLE t (age INT CHECK(age >= 0))');
      assert.throws(
        () => db.execute('INSERT INTO t VALUES (-1)'),
        /CHECK/
      );
    });

    it('accepts values satisfying CHECK', () => {
      db.execute('CREATE TABLE t (age INT CHECK(age >= 0))');
      db.execute('INSERT INTO t VALUES (25)');
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows[0].age, 25);
    });

    it('CHECK with upper bound', () => {
      db.execute('CREATE TABLE t (score INT CHECK(score >= 0 AND score <= 100))');
      db.execute('INSERT INTO t VALUES (75)');
      assert.throws(() => db.execute('INSERT INTO t VALUES (101)'), /CHECK/);
      assert.throws(() => db.execute('INSERT INTO t VALUES (-5)'), /CHECK/);
    });
  });

  describe('UNIQUE index', () => {
    it('allows different values', () => {
      db.execute('CREATE TABLE t (id INT, email TEXT)');
      db.execute('CREATE UNIQUE INDEX idx_email ON t(email)');
      db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
      db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows.length, 2);
    });
  });

  describe('FOREIGN KEY', () => {
    it('rejects invalid foreign key references', () => {
      db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id))');
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      assert.throws(
        () => db.execute("INSERT INTO employees VALUES (1, 'Alice', 99)"),
        /[Ff]oreign key/
      );
    });

    it('accepts valid foreign key references', () => {
      db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id))');
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      const result = db.execute('SELECT * FROM employees');
      assert.equal(result.rows.length, 1);
    });
  });

  describe('DEFAULT values', () => {
    it('uses DEFAULT when column not specified', () => {
      db.execute('CREATE TABLE t (id INT, status TEXT DEFAULT \'active\', count INT DEFAULT 0)');
      db.execute('INSERT INTO t (id) VALUES (1)');
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows[0].status, 'active');
      assert.equal(result.rows[0].count, 0);
    });

    it('overrides DEFAULT when value provided', () => {
      db.execute('CREATE TABLE t (id INT, status TEXT DEFAULT \'active\')');
      db.execute("INSERT INTO t VALUES (1, 'inactive')");
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows[0].status, 'inactive');
    });
  });

  describe('Multiple constraints', () => {
    it('combined NOT NULL + CHECK', () => {
      db.execute('CREATE TABLE t (age INT NOT NULL CHECK(age >= 0 AND age <= 150))');
      assert.throws(() => db.execute('INSERT INTO t VALUES (NULL)'), /NOT NULL/);
      assert.throws(() => db.execute('INSERT INTO t VALUES (-1)'), /CHECK/);
      assert.throws(() => db.execute('INSERT INTO t VALUES (200)'), /CHECK/);
      db.execute('INSERT INTO t VALUES (25)');
      assert.equal(db.execute('SELECT * FROM t').rows[0].age, 25);
    });
  });
});
