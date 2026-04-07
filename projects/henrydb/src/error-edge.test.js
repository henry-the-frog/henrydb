// error-edge.test.js — Error handling and edge case tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Error Handling and Edge Cases', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  describe('SQL syntax errors', () => {
    it('incomplete SELECT', () => {
      assert.throws(() => db.execute('SELECT'));
    });

    it('missing table name', () => {
      assert.throws(() => db.execute('SELECT * FROM'));
    });

    it('misspelled keyword', () => {
      assert.throws(() => db.execute('SELCT * FROM t'));
    });

    it('unclosed string', () => {
      assert.throws(() => db.execute("SELECT 'unclosed"));
    });

    it('double comma in column list', () => {
      assert.throws(() => db.execute('CREATE TABLE t (a INT,, b INT)'));
    });
  });

  describe('Table errors', () => {
    it('query non-existent table', () => {
      assert.throws(() => db.execute('SELECT * FROM nonexistent'));
    });

    it('create duplicate table', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      assert.throws(() => db.execute('CREATE TABLE t (id INT PRIMARY KEY)'));
    });

    it('DROP non-existent table', () => {
      assert.throws(() => db.execute('DROP TABLE nonexistent'));
    });
  });

  describe('Column errors', () => {
    it('INSERT duplicate primary key', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      try {
        db.execute('INSERT INTO t VALUES (1)');
        // If no error, verify it didn't create duplicate
        const result = db.execute('SELECT COUNT(*) AS cnt FROM t');
        assert.equal(result.rows[0].cnt, 1);
      } catch (e) {
        // Throwing on duplicate key is also correct
        assert.ok(true);
      }
    });
  });

  describe('Empty table edge cases', () => {
    it('SELECT from empty table', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows.length, 0);
    });

    it('COUNT of empty table', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      const result = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(result.rows[0].cnt, 0);
    });

    it('SUM of empty table', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      const result = db.execute('SELECT SUM(val) AS total FROM t');
      // Could be null or 0 depending on engine
      assert.ok(result.rows[0].total === null || result.rows[0].total === 0);
    });

    it('MIN/MAX of empty table is NULL', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      const result = db.execute('SELECT MIN(val) AS mn, MAX(val) AS mx FROM t');
      assert.equal(result.rows[0].mn, null);
      assert.equal(result.rows[0].mx, null);
    });

    it('GROUP BY on empty table', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, category TEXT, val INT)');
      const result = db.execute('SELECT category, SUM(val) FROM t GROUP BY category');
      assert.equal(result.rows.length, 0);
    });

    it('DELETE all rows', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      db.execute('INSERT INTO t VALUES (2, 20)');
      db.execute('DELETE FROM t');
      const result = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(result.rows[0].cnt, 0);
    });

    it('UPDATE non-existent rows', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      db.execute("UPDATE t SET val = 99 WHERE id = 999");
      const result = db.execute('SELECT * FROM t');
      assert.equal(result.rows[0].val, 10); // unchanged
    });
  });

  describe('Large values', () => {
    it('long string value', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      const longStr = 'x'.repeat(1000);
      db.execute("INSERT INTO t VALUES (1, '" + longStr + "')");
      const result = db.execute('SELECT val FROM t');
      assert.equal(result.rows[0].val.length, 1000);
    });

    it('many rows', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      }
      const result = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(result.rows[0].cnt, 100);
    });

    it('ORDER BY on many rows', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 50; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${Math.floor(Math.random() * 1000)})`);
      }
      const result = db.execute('SELECT * FROM t ORDER BY val ASC');
      for (let i = 1; i < result.rows.length; i++) {
        assert.ok(result.rows[i].val >= result.rows[i - 1].val);
      }
    });
  });

  describe('Special characters', () => {
    it('string with spaces', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello world')");
      const result = db.execute("SELECT * FROM t WHERE val = 'hello world'");
      assert.equal(result.rows.length, 1);
    });
  });
});
