import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('Arithmetic Operator Precedence', () => {
  let db;
  
  function q(sql) {
    return db.execute(sql).rows;
  }

  describe('Basic precedence (* over +)', () => {
    it('multiplication before addition', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // 2 + 3 * 4 = 14, not 20
      const result = db.execute("SELECT 2 + 3 * 4 AS val FROM t");
      assert.equal(result.rows[0].val, 14);
    });

    it('division before subtraction', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // 10 - 6 / 2 = 7, not 2
      const result = db.execute("SELECT 10 - 6 / 2 AS val FROM t");
      assert.equal(result.rows[0].val, 7);
    });

    it('complex mixed expression', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // 1 + 2 * 3 + 4 = 11, not 13 or 21
      const result = db.execute("SELECT 1 + 2 * 3 + 4 AS val FROM t");
      assert.equal(result.rows[0].val, 11);
    });

    it('multiple multiplicative ops', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // 2 * 3 * 4 = 24
      const result = db.execute("SELECT 2 * 3 * 4 AS val FROM t");
      assert.equal(result.rows[0].val, 24);
    });
  });

  describe('WHERE clause precedence', () => {
    it('correct filtering with mixed operators', () => {
      db = new Database();
      db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)");
      db.execute("INSERT INTO products VALUES (1, 10, 5)");
      db.execute("INSERT INTO products VALUES (2, 20, 3)");
      db.execute("INSERT INTO products VALUES (3, 5, 10)");
      
      // price + qty * 2 should be 10+10=20, 20+6=26, 5+20=25
      const result = db.execute("SELECT * FROM products WHERE price + qty * 2 > 24");
      assert.equal(result.rows.length, 2);
    });
  });

  describe('Parentheses override precedence', () => {
    it('parenthesized addition', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // (2 + 3) * 4 = 20
      const result = db.execute("SELECT (2 + 3) * 4 AS val FROM t");
      assert.equal(result.rows[0].val, 20);
    });
  });

  describe('Column arithmetic precedence', () => {
    it('column references with precedence', () => {
      db = new Database();
      db.execute("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)");
      db.execute("INSERT INTO t VALUES (2, 3, 4)");
      
      // a + b * c = 2 + 12 = 14
      const result = db.execute("SELECT a + b * c AS val FROM t");
      assert.equal(result.rows[0].val, 14);
    });

    it('multiple columns mixed', () => {
      db = new Database();
      db.execute("CREATE TABLE t (w INTEGER, h INTEGER)");
      db.execute("INSERT INTO t VALUES (5, 3)");
      
      // 2 * w + 2 * h = 10 + 6 = 16
      const result = db.execute("SELECT 2 * w + 2 * h AS perimeter FROM t");
      assert.equal(result.rows[0].perimeter, 16);
    });
  });

  describe('Modulo precedence', () => {
    it('modulo at multiplicative level', () => {
      db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      db.execute("INSERT INTO t VALUES (1)");
      
      // 10 + 7 % 3 = 10 + 1 = 11, not 0
      const result = db.execute("SELECT 10 + 7 % 3 AS val FROM t");
      assert.equal(result.rows[0].val, 11);
    });
  });
});
