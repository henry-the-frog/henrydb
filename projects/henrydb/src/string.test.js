// string.test.js — String functions and LIKE/BETWEEN tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('String Functions & Patterns', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, city TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice Smith', 'alice@example.com', 'New York')");
    db.execute("INSERT INTO users VALUES (2, 'Bob Jones', 'bob@test.org', 'Los Angeles')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie Brown', 'charlie@example.com', 'New York')");
    db.execute("INSERT INTO users VALUES (4, 'Diana Prince', 'diana@test.org', 'Chicago')");
    db.execute("INSERT INTO users VALUES (5, 'Eve Adams', 'eve@example.com', 'Boston')");
  });

  describe('LIKE', () => {
    it('% wildcard at end', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE 'Alice%'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice Smith');
    });

    it('% wildcard at start', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE '%Jones'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Bob Jones');
    });

    it('% wildcard on both sides', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE '%Brown%'");
      assert.equal(result.rows.length, 1);
    });

    it('_ single character wildcard', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE 'Bo_ Jones'");
      assert.equal(result.rows.length, 1);
    });

    it('LIKE with email domain', () => {
      const result = db.execute("SELECT * FROM users WHERE email LIKE '%@example.com'");
      assert.equal(result.rows.length, 3);
    });

    it('LIKE with no match', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE '%Zorro%'");
      assert.equal(result.rows.length, 0);
    });

    it('LIKE is case sensitive', () => {
      const result = db.execute("SELECT * FROM users WHERE name LIKE '%Alice%'");
      assert.equal(result.rows.length, 1);
    });

    it('ILIKE case insensitive', () => {
      const result = db.execute("SELECT * FROM users WHERE name ILIKE '%alice%'");
      assert.equal(result.rows.length, 1);
    });
  });

  describe('BETWEEN', () => {
    it('numeric BETWEEN', () => {
      const result = db.execute('SELECT * FROM users WHERE id BETWEEN 2 AND 4');
      assert.equal(result.rows.length, 3);
    });

    it('BETWEEN inclusive', () => {
      const result = db.execute('SELECT * FROM users WHERE id BETWEEN 1 AND 1');
      assert.equal(result.rows.length, 1);
    });

    it('string BETWEEN', () => {
      const result = db.execute("SELECT * FROM users WHERE name BETWEEN 'A' AND 'C'");
      assert.ok(result.rows.length >= 2); // Alice, Bob (before C)
    });
  });

  describe('UPPER', () => {
    it('UPPER in SELECT', () => {
      const result = db.execute('SELECT UPPER(name) AS upper_name FROM users WHERE id = 1');
      assert.equal(result.rows[0].upper_name, 'ALICE SMITH');
    });

    it('UPPER in WHERE', () => {
      const result = db.execute("SELECT * FROM users WHERE UPPER(name) = 'BOB JONES'");
      assert.equal(result.rows.length, 1);
    });
  });

  describe('LOWER', () => {
    it('LOWER in SELECT', () => {
      const result = db.execute('SELECT LOWER(name) AS lower_name FROM users WHERE id = 1');
      assert.equal(result.rows[0].lower_name, 'alice smith');
    });

    it('LOWER in WHERE', () => {
      const result = db.execute("SELECT * FROM users WHERE LOWER(city) = 'new york'");
      assert.equal(result.rows.length, 2);
    });
  });

  describe('LENGTH', () => {
    it('LENGTH in SELECT', () => {
      const result = db.execute('SELECT LENGTH(name) AS len FROM users WHERE id = 1');
      assert.equal(result.rows[0].len, 11); // 'Alice Smith'
    });

    it('LENGTH in WHERE', () => {
      const result = db.execute('SELECT * FROM users WHERE LENGTH(name) > 11');
      assert.ok(result.rows.length > 0);
    });
  });

  describe('CONCAT', () => {
    it('CONCAT function', () => {
      const result = db.execute("SELECT CONCAT(name, ' - ', city) AS full FROM users WHERE id = 1");
      assert.equal(result.rows[0].full, 'Alice Smith - New York');
    });

    it('|| operator', () => {
      const result = db.execute("SELECT name || ' (' || city || ')' AS display FROM users WHERE id = 2");
      assert.equal(result.rows[0].display, 'Bob Jones (Los Angeles)');
    });
  });

  describe('Combined', () => {
    it('LIKE with AND', () => {
      const result = db.execute("SELECT * FROM users WHERE email LIKE '%@example.com' AND city = 'New York'");
      assert.equal(result.rows.length, 2);
    });

    it('functions in ORDER BY projection', () => {
      const result = db.execute('SELECT UPPER(name) AS uname FROM users ORDER BY uname LIMIT 2');
      assert.equal(result.rows.length, 2);
    });

    it('BETWEEN with other conditions', () => {
      const result = db.execute("SELECT * FROM users WHERE id BETWEEN 1 AND 3 AND city = 'New York'");
      assert.equal(result.rows.length, 2);
    });
  });
});
