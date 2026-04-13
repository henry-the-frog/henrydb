// adversarial-queries.test.js — Tricky SQL edge cases
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Adversarial queries', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (a INT, b TEXT, c REAL)');
    db.execute("INSERT INTO t VALUES (1, 'hello', 3.14)");
    db.execute("INSERT INTO t VALUES (2, NULL, NULL)");
    db.execute("INSERT INTO t VALUES (NULL, 'world', 2.71)");
  });

  describe('Nested parentheses', () => {
    it('handles ((1 + 2) * 3)', () => {
      const r = db.execute('SELECT ((1 + 2) * 3) AS result');
      assert.equal(r.rows[0].result, 9);
    });

    it('handles deeply nested ((((((1+2)*3)-4)/5)+6)*7)', () => {
      const r = db.execute('SELECT ((((((1 + 2) * 3) - 4) / 5) + 6) * 7) AS deep');
      assert.equal(r.rows[0].deep, 49);
    });

    it('handles nested parens in WHERE', () => {
      const r = db.execute('SELECT * FROM t WHERE ((a + 1) * 2) > 4');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].a, 2);
    });

    it('handles nested parens with column references', () => {
      const r = db.execute('SELECT ((a * 10) + 5) AS computed FROM t WHERE a IS NOT NULL ORDER BY computed');
      assert.equal(r.rows[0].computed, 15);
      assert.equal(r.rows[1].computed, 25);
    });
  });

  describe('NULL handling', () => {
    it('IS NULL works correctly', () => {
      const r = db.execute('SELECT * FROM t WHERE a IS NULL');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].b, 'world');
    });

    it('IS NOT NULL works correctly', () => {
      const r = db.execute('SELECT * FROM t WHERE a IS NOT NULL');
      assert.equal(r.rows.length, 2);
    });

    it('NULL = NULL returns no rows', () => {
      const r = db.execute('SELECT * FROM t WHERE NULL = NULL');
      assert.equal(r.rows.length, 0);
    });

    it('NULL comparisons return no rows', () => {
      const r = db.execute('SELECT * FROM t WHERE a > NULL');
      assert.equal(r.rows.length, 0);
    });

    it('NULL arithmetic propagates', () => {
      const r = db.execute('SELECT a + 1 AS val FROM t ORDER BY a');
      const nullRow = r.rows.find(row => row.val === null);
      assert.ok(nullRow !== undefined, 'Should have a NULL result');
    });

    it('COALESCE handles NULLs', () => {
      const r = db.execute('SELECT COALESCE(a, 0) AS val FROM t ORDER BY val');
      assert.equal(r.rows[0].val, 0);
    });

    it('COALESCE with all NULLs', () => {
      const r = db.execute('SELECT COALESCE(NULL, NULL, 42) AS val');
      assert.equal(r.rows[0].val, 42);
    });

    it('CASE with NULL', () => {
      const r = db.execute("SELECT CASE WHEN a IS NULL THEN 'nil' ELSE CAST(a AS TEXT) END AS label FROM t ORDER BY a");
      const nilRow = r.rows.find(row => row.label === 'nil');
      assert.ok(nilRow, 'Should have a nil label');
    });
  });

  describe('Aggregate edge cases', () => {
    it('aggregates on empty result set', () => {
      const r = db.execute('SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t WHERE a > 100');
      assert.equal(r.rows[0]['COUNT(*)'], 0);
      assert.equal(r.rows[0]['SUM(a)'], null);
      assert.equal(r.rows[0]['AVG(a)'], null);
    });

    it('COUNT(*) counts NULL rows', () => {
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 3);
    });

    it('COUNT(column) excludes NULLs', () => {
      const r = db.execute('SELECT COUNT(a) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 2);
    });

    it('HAVING with expression not in SELECT', () => {
      const r = db.execute("SELECT b FROM t GROUP BY b HAVING COUNT(*) >= 1 AND b IS NOT NULL");
      assert.ok(r.rows.length >= 1);
    });
  });

  describe('Subqueries', () => {
    it('scalar subquery in WHERE', () => {
      const r = db.execute('SELECT * FROM t WHERE a = (SELECT MAX(a) FROM t)');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].a, 2);
    });

    it('correlated subquery', () => {
      db.execute('CREATE TABLE orders (id INT, customer TEXT, amount REAL)');
      db.execute("INSERT INTO orders VALUES (1, 'alice', 100)");
      db.execute("INSERT INTO orders VALUES (2, 'alice', 200)");
      db.execute("INSERT INTO orders VALUES (3, 'bob', 50)");
      const r = db.execute('SELECT customer, amount FROM orders o1 WHERE amount > (SELECT AVG(amount) FROM orders o2 WHERE o2.customer = o1.customer)');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].customer, 'alice');
      assert.equal(r.rows[0].amount, 200);
    });
  });

  describe('Set operations', () => {
    it('UNION removes duplicates', () => {
      db.execute('CREATE TABLE t2 (x INT)');
      db.execute('INSERT INTO t2 VALUES (1), (2), (3)');
      db.execute('CREATE TABLE t3 (x INT)');
      db.execute('INSERT INTO t3 VALUES (2), (3), (4)');
      const r = db.execute('SELECT x FROM t2 UNION SELECT x FROM t3');
      assert.equal(r.rows.length, 4);
    });

    it('UNION ALL keeps duplicates', () => {
      db.execute('CREATE TABLE t2 (x INT)');
      db.execute('INSERT INTO t2 VALUES (1), (2)');
      db.execute('CREATE TABLE t3 (x INT)');
      db.execute('INSERT INTO t3 VALUES (2), (3)');
      const r = db.execute('SELECT x FROM t2 UNION ALL SELECT x FROM t3');
      assert.equal(r.rows.length, 4);
    });
  });

  describe('CTE edge cases', () => {
    it('CTE with complex query', () => {
      const r = db.execute('WITH nums AS (SELECT a FROM t WHERE a IS NOT NULL) SELECT SUM(a) AS total FROM nums');
      assert.equal(r.rows[0].total, 3);
    });
  });

  describe('UPDATE and DELETE edge cases', () => {
    it('UPDATE with self-reference', () => {
      db.execute('UPDATE t SET a = a * 2 WHERE a IS NOT NULL');
      const r = db.execute('SELECT a FROM t WHERE a IS NOT NULL ORDER BY a');
      assert.equal(r.rows[0].a, 2);
      assert.equal(r.rows[1].a, 4);
    });

    it('DELETE with subquery', () => {
      db.execute('DELETE FROM t WHERE a = (SELECT MIN(a) FROM t)');
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 2);
    });
  });

  describe('Expression edge cases', () => {
    it('BETWEEN inclusive', () => {
      const r = db.execute('SELECT * FROM t WHERE a BETWEEN 1 AND 2');
      assert.equal(r.rows.length, 2);
    });

    it('LIMIT 0 returns empty', () => {
      const r = db.execute('SELECT * FROM t LIMIT 0');
      assert.equal(r.rows.length, 0);
    });

    it('SELECT without FROM', () => {
      const r = db.execute('SELECT 1 + 2 AS result');
      assert.equal(r.rows[0].result, 3);
    });

    it('CAST INTEGER truncates', () => {
      const r = db.execute('SELECT CAST(3.7 AS INTEGER) AS val');
      assert.equal(r.rows[0].val, 3);
    });

    it('CAST NULL returns NULL', () => {
      const r = db.execute('SELECT CAST(NULL AS INTEGER) AS val');
      assert.equal(r.rows[0].val, null);
    });

    it('DISTINCT with ORDER BY', () => {
      db.execute('CREATE TABLE t2 (x INT)');
      db.execute('INSERT INTO t2 VALUES (3), (1), (2), (1), (3)');
      const r = db.execute('SELECT DISTINCT x FROM t2 ORDER BY x');
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0].x, 1);
      assert.equal(r.rows[2].x, 3);
    });

    it('ORDER BY expression', () => {
      const r = db.execute('SELECT * FROM t WHERE a IS NOT NULL ORDER BY a * -1');
      assert.equal(r.rows[0].a, 2);
    });

    it('CASE in ORDER BY', () => {
      db.execute('CREATE TABLE items (name TEXT, priority INT)');
      db.execute("INSERT INTO items VALUES ('low', 3)");
      db.execute("INSERT INTO items VALUES ('high', 1)");
      db.execute("INSERT INTO items VALUES ('med', 2)");
      const r = db.execute('SELECT * FROM items ORDER BY CASE priority WHEN 1 THEN 0 WHEN 2 THEN 1 ELSE 2 END');
      assert.equal(r.rows[0].name, 'high');
    });

    it('large IN list', () => {
      const values = Array.from({length: 50}, (_, i) => i).join(',');
      const r = db.execute(`SELECT * FROM t WHERE a IN (${values})`);
      assert.equal(r.rows.length, 2);
    });
  });

  describe('Complex mixed queries', () => {
    it('multiple aggregates with GROUP BY and ORDER BY', () => {
      db.execute('CREATE TABLE sales (product TEXT, amount REAL)');
      db.execute("INSERT INTO sales VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('c', 5)");
      const r = db.execute('SELECT product, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amount FROM sales GROUP BY product ORDER BY total DESC');
      assert.equal(r.rows[0].product, 'b');
      assert.equal(r.rows[0].total, 70);
    });

    it('multiple LIKE with OR', () => {
      const r = db.execute("SELECT * FROM t WHERE b LIKE 'h%' OR b LIKE 'w%'");
      assert.equal(r.rows.length, 2);
    });

    it('complex AND/OR with parens', () => {
      db.execute('CREATE TABLE orders (status TEXT, amount REAL, customer TEXT)');
      db.execute("INSERT INTO orders VALUES ('paid', 200, 'alice')");
      db.execute("INSERT INTO orders VALUES ('pending', 75, 'bob')");
      db.execute("INSERT INTO orders VALUES ('paid', 50, 'carol')");
      const r = db.execute("SELECT * FROM orders WHERE (status = 'paid' AND amount > 100) OR (status = 'pending')");
      assert.equal(r.rows.length, 2);
    });
  });
});
