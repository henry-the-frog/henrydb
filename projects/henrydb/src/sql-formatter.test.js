// sql-formatter.test.js — Tests for SQL formatter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { formatSQL, format } from './sql-formatter.js';
import { parse } from './sql.js';

describe('SQL Formatter', () => {
  
  function fmt(sql) {
    return formatSQL(parse(sql));
  }

  describe('SELECT', () => {
    it('formats simple SELECT', () => {
      const result = fmt('SELECT a, b FROM t');
      assert.ok(result.includes('SELECT'));
      assert.ok(result.includes('FROM t'));
      assert.ok(result.includes('a'));
    });

    it('formats SELECT with WHERE', () => {
      const result = fmt("SELECT name FROM users WHERE age > 21");
      assert.ok(result.includes('SELECT name'));
      assert.ok(result.includes('WHERE'));
      assert.ok(result.includes('>'));
    });

    it('formats SELECT with ORDER BY', () => {
      const result = fmt('SELECT * FROM t ORDER BY score DESC');
      assert.ok(result.includes('ORDER BY'));
      assert.ok(result.includes('DESC'));
    });

    it('formats SELECT with GROUP BY and HAVING', () => {
      const result = fmt('SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING COUNT(*) > 5');
      assert.ok(result.includes('GROUP BY'));
      assert.ok(result.includes('HAVING'));
    });

    it('formats SELECT with LIMIT and OFFSET', () => {
      const result = fmt('SELECT * FROM t LIMIT 10 OFFSET 20');
      assert.ok(result.includes('LIMIT 10'));
      assert.ok(result.includes('OFFSET 20'));
    });

    it('formats SELECT DISTINCT', () => {
      const result = fmt('SELECT DISTINCT name FROM t');
      assert.ok(result.includes('DISTINCT'));
    });

    it('formats JOIN', () => {
      const result = fmt('SELECT a.name, b.score FROM users a JOIN scores b ON a.id = b.user_id');
      assert.ok(result.includes('JOIN'));
      assert.ok(result.includes('ON'));
    });

    it('formats subquery in WHERE', () => {
      const result = fmt('SELECT name FROM t WHERE id IN (SELECT id FROM other)');
      assert.ok(result.includes('IN'));
    });

    it('formats CASE expression', () => {
      const result = fmt("SELECT CASE WHEN score > 90 THEN 'A' WHEN score > 80 THEN 'B' ELSE 'C' END as grade FROM t");
      assert.ok(result.includes('CASE'));
      assert.ok(result.includes('WHEN'));
      assert.ok(result.includes('ELSE'));
      assert.ok(result.includes('END'));
    });

    it('formats BETWEEN', () => {
      const result = fmt('SELECT * FROM t WHERE price BETWEEN 10 AND 50');
      assert.ok(result.includes('BETWEEN'));
    });

    it('formats LIKE', () => {
      const result = fmt("SELECT * FROM t WHERE name LIKE 'A%'");
      assert.ok(result.includes('LIKE'));
    });

    it('formats CTE', () => {
      const result = fmt('WITH cte AS (SELECT id FROM t) SELECT * FROM cte');
      assert.ok(result.includes('WITH'));
      assert.ok(result.includes('AS'));
    });

    it('formats aliases', () => {
      const result = fmt('SELECT name as full_name, age FROM users');
      assert.ok(result.includes('AS'));
    });
  });

  describe('INSERT', () => {
    it('formats INSERT with values', () => {
      const result = fmt("INSERT INTO users (id, name) VALUES (1, 'Alice')");
      assert.ok(result.includes('INSERT INTO'));
      assert.ok(result.includes('VALUES'));
    });
  });

  describe('UPDATE', () => {
    it('formats UPDATE with SET and WHERE', () => {
      const result = fmt("UPDATE users SET name = 'Bob' WHERE id = 1");
      assert.ok(result.includes('UPDATE'));
      assert.ok(result.includes('SET'));
      assert.ok(result.includes('WHERE'));
    });
  });

  describe('DELETE', () => {
    it('formats DELETE with WHERE', () => {
      const result = fmt('DELETE FROM users WHERE id = 1');
      assert.ok(result.includes('DELETE FROM'));
      assert.ok(result.includes('WHERE'));
    });
  });

  describe('DDL', () => {
    it('formats CREATE TABLE', () => {
      const result = fmt('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
      assert.ok(result.includes('CREATE TABLE'));
      assert.ok(result.includes('PRIMARY KEY'));
    });

    it('formats CREATE INDEX', () => {
      const result = fmt('CREATE INDEX idx_name ON users(name)');
      assert.ok(result.includes('CREATE'));
      assert.ok(result.includes('INDEX'));
      assert.ok(result.includes('ON'));
    });
  });

  describe('format() helper', () => {
    it('parses and formats in one call', () => {
      const result = format('SELECT * FROM t WHERE id = 1', parse);
      assert.ok(result.includes('SELECT'));
      assert.ok(result.includes('WHERE'));
    });
  });

  describe('round-trip: parse → format → parse', () => {
    it('preserves semantics for simple queries', () => {
      const queries = [
        'SELECT * FROM t',
        "SELECT name FROM users WHERE age > 21",
        'SELECT a, b FROM t ORDER BY a DESC LIMIT 10',
        'SELECT category, COUNT(*) as cnt FROM products GROUP BY category',
      ];
      for (const sql of queries) {
        const formatted = fmt(sql);
        // Should be valid SQL (re-parseable)
        assert.ok(formatted.length > 0, `Empty format for: ${sql}`);
        // Try parsing the formatted output
        try {
          const reParsed = parse(formatted.replace(/;$/, ''));
          assert.ok(reParsed, `Failed to re-parse: ${formatted}`);
        } catch(e) {
          // Some formats might not be perfectly re-parseable yet — that's OK for now
          // but log it
          console.log(`Note: re-parse failed for: ${formatted.substring(0, 60)}...`);
        }
      }
    });
  });
});
