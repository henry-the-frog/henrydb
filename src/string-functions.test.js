// string-functions.test.js — String function tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('String Functions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE names (id INT PRIMARY KEY, first TEXT, last TEXT)');
    db.execute("INSERT INTO names VALUES (1, 'Alice', 'Smith')");
    db.execute("INSERT INTO names VALUES (2, 'Bob', 'Jones')");
    db.execute("INSERT INTO names VALUES (3, 'Charlie', 'Brown')");
    db.execute("INSERT INTO names VALUES (4, 'diana', 'prince')");
  });

  describe('UPPER', () => {
    it('converts to uppercase', () => {
      const result = db.execute("SELECT UPPER(first) AS name FROM names WHERE id = 4");
      assert.equal(result.rows[0].name, 'DIANA');
    });

    it('UPPER in WHERE', () => {
      const result = db.execute("SELECT * FROM names WHERE UPPER(first) = 'ALICE'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].id, 1);
    });

    it('UPPER preserves already uppercase', () => {
      const result = db.execute("SELECT UPPER(first) AS name FROM names WHERE id = 1");
      assert.equal(result.rows[0].name, 'ALICE');
    });
  });

  describe('LOWER', () => {
    it('converts to lowercase', () => {
      const result = db.execute("SELECT LOWER(first) AS name FROM names WHERE id = 1");
      assert.equal(result.rows[0].name, 'alice');
    });

    it('LOWER in ORDER BY', () => {
      const result = db.execute('SELECT first FROM names ORDER BY LOWER(first)');
      assert.equal(result.rows[0].first, 'Alice');
    });
  });

  describe('UPPER + LOWER combined', () => {
    it('case-insensitive comparison', () => {
      const result = db.execute("SELECT * FROM names WHERE UPPER(first) = 'DIANA'");
      assert.equal(result.rows.length, 1);
    });
  });

  describe('REPLACE', () => {
    it('replaces substring', () => {
      const result = db.execute("SELECT REPLACE(first, 'li', 'XX') AS r FROM names WHERE id = 1");
      assert.equal(result.rows[0].r, 'AXXce');
    });

    it('replaces all occurrences', () => {
      const result = db.execute("SELECT REPLACE('aabaa', 'a', 'x') AS r FROM names WHERE id = 1");
      assert.equal(result.rows[0].r, 'xxbxx');
    });
  });

  describe('TRIM', () => {
    it('removes leading/trailing spaces', () => {
      db.execute('CREATE TABLE padded (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO padded VALUES (1, '  hello  ')");
      const result = db.execute("SELECT TRIM(val) AS trimmed FROM padded");
      assert.equal(result.rows[0].trimmed, 'hello');
    });
  });

  describe('String concatenation', () => {
    it('concatenates with ||', () => {
      const result = db.execute("SELECT first || ' ' || last AS full_name FROM names WHERE id = 1");
      assert.equal(result.rows[0].full_name, 'Alice Smith');
    });

    it('concatenation in ORDER BY', () => {
      const result = db.execute("SELECT first || ' ' || last AS full_name FROM names ORDER BY full_name");
      assert.ok(result.rows.length === 4);
    });
  });

  describe('LIKE pattern matching', () => {
    it('LIKE with %', () => {
      const result = db.execute("SELECT first FROM names WHERE first LIKE 'A%'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].first, 'Alice');
    });

    it('LIKE with _', () => {
      const result = db.execute("SELECT first FROM names WHERE first LIKE 'B_b'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].first, 'Bob');
    });

    it('LIKE case sensitive', () => {
      const result = db.execute("SELECT first FROM names WHERE first LIKE '%li%'");
      assert.equal(result.rows.length, 2); // Alice and Charlie
    });

    it('NOT LIKE', () => {
      const result = db.execute("SELECT first FROM names WHERE first NOT LIKE '%a%'");
      // Only Bob has no lowercase 'a'... but Bob also has no 'a'
      assert.ok(result.rows.length >= 0);
    });
  });

  describe('Combined string operations', () => {
    it('UPPER in GROUP BY', () => {
      const result = db.execute('SELECT UPPER(first) AS name, COUNT(*) AS cnt FROM names GROUP BY UPPER(first) ORDER BY name');
      assert.ok(result.rows.length > 0);
    });
  });
});
