// null-handling.test.js — Tests for NULL handling in SQL operations

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function makeDB() {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, name TEXT, val REAL)');
  db.execute("INSERT INTO t VALUES (1, 'Alice', 100)");
  db.execute("INSERT INTO t VALUES (2, 'Bob', NULL)");
  db.execute("INSERT INTO t VALUES (3, NULL, 200)");
  db.execute("INSERT INTO t VALUES (4, NULL, NULL)");
  return db;
}

describe('NULL Handling', () => {
  describe('COALESCE', () => {
    it('should return first non-null value', () => {
      const db = makeDB();
      const r = db.execute("SELECT COALESCE(name, 'Unknown') AS result FROM t WHERE id = 3");
      assert.equal(r.rows[0].result, 'Unknown'); // id=3 has name=NULL
    });

    it('should return first arg if not null', () => {
      const db = makeDB();
      const r = db.execute("SELECT COALESCE(name, 'Unknown') AS result FROM t WHERE id = 1");
      assert.equal(r.rows[0].result, 'Alice');
    });

    it('should handle multiple arguments', () => {
      const db = makeDB();
      const r = db.execute("SELECT COALESCE(name, val, 0) AS result FROM t WHERE id = 4");
      assert.equal(r.rows[0].result, 0);
    });
  });

  describe('NULLIF', () => {
    it('should return null when equal', () => {
      const db = makeDB();
      const r = db.execute("SELECT NULLIF(val, 100) AS result FROM t WHERE id = 1");
      assert.equal(r.rows[0].result, null);
    });

    it('should return first arg when not equal', () => {
      const db = makeDB();
      const r = db.execute("SELECT NULLIF(val, 999) AS result FROM t WHERE id = 1");
      assert.equal(r.rows[0].result, 100);
    });
  });

  describe('IFNULL', () => {
    it('should return replacement when null', () => {
      const db = makeDB();
      const r = db.execute("SELECT IFNULL(val, -1) AS result FROM t WHERE id = 2");
      assert.equal(r.rows[0].result, -1);
    });

    it('should return original when not null', () => {
      const db = makeDB();
      const r = db.execute("SELECT IFNULL(val, -1) AS result FROM t WHERE id = 1");
      assert.equal(r.rows[0].result, 100);
    });
  });

  describe('NULL arithmetic', () => {
    it('NULL + number should be NULL', () => {
      const db = makeDB();
      const r = db.execute('SELECT val + 10 AS result FROM t WHERE id = 2');
      assert.equal(r.rows[0].result, null);
    });

    it('NULL * number should be NULL', () => {
      const db = makeDB();
      const r = db.execute('SELECT val * 2 AS result FROM t WHERE id = 2');
      assert.equal(r.rows[0].result, null);
    });

    it('number + NULL should be NULL', () => {
      const db = makeDB();
      const r = db.execute('SELECT 10 + val AS result FROM t WHERE id = 2');
      assert.equal(r.rows[0].result, null);
    });
  });

  describe('NULL comparisons', () => {
    it('NULL = NULL should be falsy (not found)', () => {
      const db = makeDB();
      const r = db.execute('SELECT id FROM t WHERE val = NULL');
      assert.equal(r.rows.length, 0, 'val = NULL should match nothing');
    });

    it('IS NULL should find null rows', () => {
      const db = makeDB();
      const r = db.execute('SELECT id FROM t WHERE val IS NULL');
      assert.equal(r.rows.length, 2); // id 2 and 4
    });

    it('IS NOT NULL should find non-null rows', () => {
      const db = makeDB();
      const r = db.execute('SELECT id FROM t WHERE val IS NOT NULL');
      assert.equal(r.rows.length, 2); // id 1 and 3
    });
  });

  describe('NULL in aggregates', () => {
    it('COUNT(*) should count all rows including nulls', () => {
      const db = makeDB();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 4);
    });

    it('COUNT(column) should exclude nulls', () => {
      const db = makeDB();
      const r = db.execute('SELECT COUNT(val) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 2);
    });

    it('SUM should ignore nulls', () => {
      const db = makeDB();
      const r = db.execute('SELECT SUM(val) AS total FROM t');
      assert.equal(r.rows[0].total, 300); // 100 + 200
    });

    it('AVG should ignore nulls', () => {
      const db = makeDB();
      const r = db.execute('SELECT AVG(val) AS avg FROM t');
      assert.equal(r.rows[0].avg, 150); // (100 + 200) / 2
    });

    it('MIN/MAX should ignore nulls', () => {
      const db = makeDB();
      const r = db.execute('SELECT MIN(val) AS min_val, MAX(val) AS max_val FROM t');
      assert.equal(r.rows[0].min_val, 100);
      assert.equal(r.rows[0].max_val, 200);
    });
  });

  describe('NULL in CASE', () => {
    it('should handle NULL in CASE WHEN', () => {
      const db = makeDB();
      const r = db.execute("SELECT CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END AS status FROM t ORDER BY id");
      assert.equal(r.rows[0].status, 'present');  // id 1: val=100
      assert.equal(r.rows[1].status, 'missing');   // id 2: val=NULL
    });
  });

  describe('NULL in ORDER BY', () => {
    it('should handle NULL values in sort', () => {
      const db = makeDB();
      const r = db.execute('SELECT id, val FROM t ORDER BY val');
      // NULLs should appear somewhere (typically first or last)
      assert.equal(r.rows.length, 4);
    });
  });

  describe('NULL in DISTINCT', () => {
    it('should treat NULLs as equal for DISTINCT', () => {
      const db = new Database();
      db.execute('CREATE TABLE d (v TEXT)');
      db.execute("INSERT INTO d VALUES ('a')");
      db.execute("INSERT INTO d VALUES (NULL)");
      db.execute("INSERT INTO d VALUES ('a')");
      db.execute("INSERT INTO d VALUES (NULL)");
      
      const r = db.execute('SELECT DISTINCT v FROM d');
      assert.equal(r.rows.length, 2); // 'a' and NULL
    });
  });
});
