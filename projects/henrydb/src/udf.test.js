// udf.test.js — User-Defined Functions (CREATE FUNCTION / DROP FUNCTION)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function q(db, sql) {
  const r = db.execute(sql);
  return r.rows || r || [];
}

describe('User-Defined Functions (UDFs)', () => {
  describe('CREATE FUNCTION basics', () => {
    it('creates and calls a simple integer function', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 5)');
      db.execute('CREATE FUNCTION double_it(x INT) RETURNS INT AS $$ SELECT x * 2 $$');
      
      const r = q(db, 'SELECT double_it(val) as d FROM t');
      assert.equal(r[0].d, 10);
    });

    it('creates and calls a text function', () => {
      const db = new Database();
      db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO users VALUES (1, 'world')");
      db.execute("CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS $$ SELECT 'Hello ' || name $$");
      
      const r = q(db, 'SELECT greet(name) as g FROM users');
      assert.equal(r[0].g, 'Hello world');
    });

    it('function with multiple parameters', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
      db.execute('INSERT INTO t VALUES (1, 3, 7)');
      db.execute('CREATE FUNCTION add_em(x INT, y INT) RETURNS INT AS $$ SELECT x + y $$');
      
      const r = q(db, 'SELECT add_em(a, b) as total FROM t');
      assert.equal(r[0].total, 10);
    });

    it('function with no parameters', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('CREATE FUNCTION always_42() RETURNS INT AS $$ SELECT 42 $$');
      
      const r = q(db, 'SELECT always_42() as val FROM t');
      assert.equal(r[0].val, 42);
    });
  });

  describe('UDF in different contexts', () => {
    it('UDF in WHERE clause', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 5)');
      db.execute('INSERT INTO t VALUES (2, 15)');
      db.execute('CREATE FUNCTION double_it(x INT) RETURNS INT AS $$ SELECT x * 2 $$');
      
      const r = q(db, 'SELECT * FROM t WHERE double_it(val) > 20');
      assert.equal(r.length, 1);
      assert.equal(r[0].id, 2);
    });

    it('UDF in ORDER BY', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 30)');
      db.execute('INSERT INTO t VALUES (2, 10)');
      db.execute('INSERT INTO t VALUES (3, 20)');
      db.execute('CREATE FUNCTION negate(x INT) RETURNS INT AS $$ SELECT 0 - x $$');
      
      const r = q(db, 'SELECT id, val FROM t ORDER BY negate(val)');
      assert.equal(r[0].id, 1); // val=30 → negate=-30 (smallest)
      assert.equal(r[2].id, 2); // val=10 → negate=-10 (largest)
    });

    it('UDF in SELECT with alias', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 5)');
      db.execute('CREATE FUNCTION square(x INT) RETURNS INT AS $$ SELECT x * x $$');
      
      const r = q(db, 'SELECT square(val) as squared FROM t');
      assert.equal(r[0].squared, 25);
    });
  });

  describe('nested and composed UDFs', () => {
    it('nested UDF calls', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 2)');
      db.execute('CREATE FUNCTION double_it(x INT) RETURNS INT AS $$ SELECT x * 2 $$');
      db.execute('CREATE FUNCTION add_one(x INT) RETURNS INT AS $$ SELECT x + 1 $$');
      
      const r = q(db, 'SELECT double_it(add_one(val)) as result FROM t');
      assert.equal(r[0].result, 6); // (2+1)*2 = 6
    });

    it('UDF with literal arguments', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('CREATE FUNCTION add_em(a INT, b INT) RETURNS INT AS $$ SELECT a + b $$');
      
      const r = q(db, 'SELECT add_em(10, 20) as result FROM t');
      assert.equal(r[0].result, 30);
    });

    it('UDF with mixed column and literal args', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 5)');
      db.execute('CREATE FUNCTION add_em(a INT, b INT) RETURNS INT AS $$ SELECT a + b $$');
      
      const r = q(db, 'SELECT add_em(val, 100) as result FROM t');
      assert.equal(r[0].result, 105);
    });
  });

  describe('DROP FUNCTION', () => {
    it('drops a function', () => {
      const db = new Database();
      db.execute('CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x $$');
      db.execute('DROP FUNCTION f');
      
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      assert.throws(() => db.execute('SELECT f(1) FROM t'), /Unknown function/i);
    });

    it('DROP FUNCTION IF EXISTS on missing function', () => {
      const db = new Database();
      const r = db.execute('DROP FUNCTION IF EXISTS nonexistent');
      assert.ok(r.message.includes('does not exist'));
    });

    it('DROP FUNCTION on missing function throws', () => {
      const db = new Database();
      assert.throws(() => db.execute('DROP FUNCTION nonexistent'), /does not exist/);
    });
  });

  describe('OR REPLACE', () => {
    it('replaces an existing function', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      
      db.execute('CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x * 2 $$');
      assert.equal(q(db, 'SELECT f(5) as r FROM t')[0].r, 10);
      
      db.execute('CREATE OR REPLACE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x * 3 $$');
      assert.equal(q(db, 'SELECT f(5) as r FROM t')[0].r, 15);
    });

    it('CREATE without REPLACE on existing throws', () => {
      const db = new Database();
      db.execute('CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x $$');
      assert.throws(() => db.execute('CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x $$'), /already exists/);
    });
  });

  describe('LANGUAGE js', () => {
    it('executes a JavaScript function', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, 5)');
      db.execute("CREATE FUNCTION cube_it(x INT) RETURNS INT LANGUAGE js AS $$ x * x * x $$");
      
      const r = q(db, 'SELECT cube_it(val) as result FROM t');
      assert.equal(r[0].result, 125);
    });

    it('JS function with multiple params', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute("CREATE FUNCTION hypotenuse(a FLOAT, b FLOAT) RETURNS FLOAT LANGUAGE js AS $$ Math.sqrt(a*a + b*b) $$");
      
      const r = q(db, 'SELECT hypotenuse(3, 4) as h FROM t');
      assert.equal(r[0].h, 5);
    });
  });

  describe('edge cases', () => {
    it('function name is case-insensitive', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('CREATE FUNCTION MyFunc(x INT) RETURNS INT AS $$ SELECT x + 1 $$');
      
      const r = q(db, 'SELECT myfunc(5) as r FROM t');
      assert.equal(r[0].r, 6);
    });

    it('function with NULL argument', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO t VALUES (1, NULL)');
      db.execute('CREATE FUNCTION add_one(x INT) RETURNS INT AS $$ SELECT x + 1 $$');
      
      const r = q(db, 'SELECT add_one(val) as r FROM t');
      // NULL + 1 = NULL in SQL
      assert.equal(r[0].r, null);
    });

    it('dollar-quoting with tags', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute("CREATE FUNCTION has_dollar(x TEXT) RETURNS TEXT AS $body$ SELECT x || ' has $$' $body$");
      
      const r = q(db, "SELECT has_dollar('test') as r FROM t");
      assert.equal(r[0].r, 'test has $$');
    });

    it('function body with single quotes', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute("CREATE FUNCTION wrap(x TEXT) RETURNS TEXT AS $$ SELECT '(' || x || ')' $$");
      
      const r = q(db, "SELECT wrap('hello') as r FROM t");
      assert.equal(r[0].r, '(hello)');
    });
  });
});
