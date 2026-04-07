// datetime-cast.test.js — Type handling, NULL behavior, and aggregate edge cases
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Type and NULL Handling', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  describe('NULL in queries', () => {
    it('NULL values inserted and retrieved', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t1 VALUES (1, NULL)');
      db.execute('INSERT INTO t1 VALUES (2, 42)');
      const result = db.execute('SELECT * FROM t1 ORDER BY id');
      assert.equal(result.rows[0].val, null);
      assert.equal(result.rows[1].val, 42);
    });

    it('IS NULL in WHERE', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t1 VALUES (1, NULL)');
      db.execute('INSERT INTO t1 VALUES (2, 42)');
      const result = db.execute('SELECT * FROM t1 WHERE val IS NULL');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].id, 1);
    });

    it('IS NOT NULL in WHERE', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t1 VALUES (1, NULL)');
      db.execute('INSERT INTO t1 VALUES (2, 42)');
      db.execute('INSERT INTO t1 VALUES (3, NULL)');
      const result = db.execute('SELECT * FROM t1 WHERE val IS NOT NULL');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].val, 42);
    });

    it('COUNT(*) counts NULLs, COUNT(col) does not', () => {
      db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
      db.execute('INSERT INTO scores VALUES (1, 100)');
      db.execute('INSERT INTO scores VALUES (2, NULL)');
      db.execute('INSERT INTO scores VALUES (3, 80)');
      const result = db.execute('SELECT COUNT(*) AS total, COUNT(score) AS scored FROM scores');
      assert.equal(result.rows[0].total, 3);
      assert.equal(result.rows[0].scored, 2);
    });

    it('SUM ignores NULL', () => {
      db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
      db.execute('INSERT INTO scores VALUES (1, 100)');
      db.execute('INSERT INTO scores VALUES (2, NULL)');
      db.execute('INSERT INTO scores VALUES (3, 80)');
      const result = db.execute('SELECT SUM(score) AS total FROM scores');
      assert.equal(result.rows[0].total, 180);
    });

    it('AVG ignores NULL', () => {
      db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
      db.execute('INSERT INTO scores VALUES (1, 100)');
      db.execute('INSERT INTO scores VALUES (2, NULL)');
      db.execute('INSERT INTO scores VALUES (3, 80)');
      const result = db.execute('SELECT AVG(score) AS avg_score FROM scores');
      assert.equal(result.rows[0].avg_score, 90); // (100+80)/2, not /3
    });

    it('MIN/MAX ignore NULL', () => {
      db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
      db.execute('INSERT INTO scores VALUES (1, 100)');
      db.execute('INSERT INTO scores VALUES (2, NULL)');
      db.execute('INSERT INTO scores VALUES (3, 80)');
      const result = db.execute('SELECT MIN(score) AS min_s, MAX(score) AS max_s FROM scores');
      assert.equal(result.rows[0].min_s, 80);
      assert.equal(result.rows[0].max_s, 100);
    });

    it('NULL in GROUP BY', () => {
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT, val INT)');
      db.execute("INSERT INTO items VALUES (1, 'A', 10)");
      db.execute("INSERT INTO items VALUES (2, 'A', 20)");
      db.execute('INSERT INTO items VALUES (3, NULL, 30)');
      db.execute('INSERT INTO items VALUES (4, NULL, 40)');
      const result = db.execute('SELECT category, SUM(val) AS total FROM items GROUP BY category ORDER BY total');
      // Should have 2 groups: A(30) and NULL(70)
      assert.equal(result.rows.length, 2);
    });
  });

  describe('Type handling', () => {
    it('REAL/FLOAT values', () => {
      db.execute('CREATE TABLE measurements (id INT PRIMARY KEY, val REAL)');
      db.execute('INSERT INTO measurements VALUES (1, 3.14)');
      db.execute('INSERT INTO measurements VALUES (2, 2.71)');
      const result = db.execute('SELECT SUM(val) AS total FROM measurements');
      assert.ok(Math.abs(result.rows[0].total - 5.85) < 0.01);
    });

    it('TEXT comparison in WHERE', () => {
      db.execute('CREATE TABLE names (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO names VALUES (1, 'Alice')");
      db.execute("INSERT INTO names VALUES (2, 'Bob')");
      db.execute("INSERT INTO names VALUES (3, 'Charlie')");
      const result = db.execute("SELECT * FROM names WHERE name > 'B' ORDER BY name");
      assert.ok(result.rows.length >= 2); // Bob, Charlie
    });

    it('INTEGER overflow (large values)', () => {
      db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO big VALUES (1, 2000000000)');
      db.execute('INSERT INTO big VALUES (2, 2000000000)');
      const result = db.execute('SELECT SUM(val) AS total FROM big');
      assert.equal(result.rows[0].total, 4000000000);
    });

    it('negative numbers', () => {
      db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO nums VALUES (1, -10)');
      db.execute('INSERT INTO nums VALUES (2, 20)');
      db.execute('INSERT INTO nums VALUES (3, -5)');
      const result = db.execute('SELECT SUM(val) AS total, MIN(val) AS mn, MAX(val) AS mx FROM nums');
      assert.equal(result.rows[0].total, 5);
      assert.equal(result.rows[0].mn, -10);
      assert.equal(result.rows[0].mx, 20);
    });

    it('empty string vs NULL', () => {
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t1 VALUES (1, '')");
      db.execute('INSERT INTO t1 VALUES (2, NULL)');
      const empty = db.execute("SELECT * FROM t1 WHERE val = ''");
      assert.equal(empty.rows.length, 1);
      const nulls = db.execute('SELECT * FROM t1 WHERE val IS NULL');
      assert.equal(nulls.rows.length, 1);
    });
  });

  describe('CASE with type casting', () => {
    it('CASE returns different types', () => {
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO items VALUES (1, 10)');
      db.execute('INSERT INTO items VALUES (2, 20)');
      db.execute('INSERT INTO items VALUES (3, 30)');
      const result = db.execute("SELECT id, CASE WHEN val > 20 THEN 'high' WHEN val > 10 THEN 'mid' ELSE 'low' END AS level FROM items ORDER BY id");
      assert.equal(result.rows[0].level, 'low');
      assert.equal(result.rows[1].level, 'mid');
      assert.equal(result.rows[2].level, 'high');
    });
  });
});
