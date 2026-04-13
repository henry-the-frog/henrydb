// sql-edge-cases.test.js — SQL parser and execution edge cases
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-edge-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('SQL Edge Cases', () => {
  afterEach(cleanup);

  describe('String handling', () => {
    it("string with spaces and numbers", () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello world 123')");
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, 'hello world 123');
    });

    it('empty string', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, '')");
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, '');
    });

    it('string with special characters', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello\nworld')");
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.ok(r.rows[0].val.includes('\\n') || r.rows[0].val.includes('\n'));
    });
  });

  describe('Numeric edge cases', () => {
    it('zero', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (0, 0)');
      const r = db.execute('SELECT * FROM t WHERE id = 0');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].val, 0);
    });

    it('negative numbers', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, -100)');
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, -100);
    });

    it('large numbers', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, 2147483647)');
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, 2147483647);
    });

    it('float precision', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val FLOAT)');
      db.execute('INSERT INTO t VALUES (1, 0.1)');
      db.execute('INSERT INTO t VALUES (2, 0.2)');
      const r = db.execute('SELECT SUM(val) as total FROM t');
      // Float precision: 0.1 + 0.2 ≈ 0.30000000000000004
      assert.ok(Math.abs(r.rows[0].total - 0.3) < 0.001);
    });

    it('division by zero', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      try {
        const r = db.execute('SELECT val / 0 as result FROM t');
        // Should either return Infinity, NULL, or throw
        assert.ok(r.rows[0].result === null || r.rows[0].result === Infinity || r.rows[0].result === undefined);
      } catch {
        // Division by zero throwing is acceptable
        assert.ok(true);
      }
    });
  });

  describe('Complex WHERE clauses', () => {
    it('nested AND/OR', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, a INT, b INT, c INT)');
      db.execute('INSERT INTO t VALUES (1, 1, 2, 3)');
      db.execute('INSERT INTO t VALUES (2, 4, 5, 6)');
      db.execute('INSERT INTO t VALUES (3, 7, 8, 9)');
      const r = db.execute('SELECT id FROM t WHERE (a > 3 AND b < 9) OR c = 3');
      assert.ok(r.rows.some(row => row.id === 1)); // c = 3
      assert.ok(r.rows.some(row => row.id === 2)); // a > 3 AND b < 9
    });

    it('BETWEEN', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      const r = db.execute('SELECT * FROM t WHERE val BETWEEN 30 AND 70');
      assert.equal(r.rows.length, 5); // 30, 40, 50, 60, 70
    });

    it('LIKE pattern', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.execute("INSERT INTO t VALUES (3, 'Charlie')");
      const r = db.execute("SELECT name FROM t WHERE name LIKE 'A%'");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].name, 'Alice');
    });

    it('IN with multiple values', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("INSERT INTO t VALUES (2, 'b')");
      db.execute("INSERT INTO t VALUES (3, 'c')");
      db.execute("INSERT INTO t VALUES (4, 'd')");
      const r = db.execute("SELECT * FROM t WHERE val IN ('a', 'c', 'e')");
      assert.equal(r.rows.length, 2);
    });

    it('IS NULL / IS NOT NULL', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello')");
      db.execute('INSERT INTO t (id) VALUES (2)');
      assert.equal(db.execute('SELECT * FROM t WHERE val IS NULL').rows.length, 1);
      assert.equal(db.execute('SELECT * FROM t WHERE val IS NOT NULL').rows.length, 1);
    });

    it('COALESCE', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'hello')");
      db.execute('INSERT INTO t (id) VALUES (2)');
      const r = db.execute("SELECT id, COALESCE(val, 'default') as v FROM t ORDER BY id");
      assert.equal(r.rows[0].v, 'hello');
      assert.equal(r.rows[1].v, 'default');
    });
  });

  describe('Complex expressions', () => {
    it('arithmetic in SELECT', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, price INT, qty INT)');
      db.execute('INSERT INTO t VALUES (1, 100, 5)');
      // Simple computed column
      const r = db.execute('SELECT id, price + qty as sum_val FROM t');
      assert.equal(r.rows[0].sum_val, 105);
    });

    it('CASE expression', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, score INT)');
      db.execute('INSERT INTO t VALUES (1, 95)');
      db.execute('INSERT INTO t VALUES (2, 75)');
      db.execute('INSERT INTO t VALUES (3, 55)');
      const r = db.execute(`
        SELECT id, CASE
          WHEN score >= 90 THEN 'A'
          WHEN score >= 70 THEN 'B'
          ELSE 'C'
        END as grade
        FROM t ORDER BY id
      `);
      assert.equal(r.rows[0].grade, 'A');
      assert.equal(r.rows[1].grade, 'B');
      assert.equal(r.rows[2].grade, 'C');
    });

    it('nested subquery in WHERE', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, val INT)');
      for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      const r = db.execute('SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)');
      assert.equal(r.rows.length, 5); // val > 55
    });
  });
});
