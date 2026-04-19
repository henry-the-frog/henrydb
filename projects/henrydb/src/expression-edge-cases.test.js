// expression-edge-cases.test.js — Expression evaluation correctness

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CAST and Type Coercion', () => {
  it('CAST INT to TEXT', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CAST(42 AS TEXT) as r').rows[0].r, '42');
  });

  it('CAST TEXT to INT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CAST('123' AS INT) as r").rows[0].r, 123);
  });

  it('CAST FLOAT to INT truncates', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CAST(3.7 AS INT) as r').rows[0].r, 3);
  });

  it('nested CAST', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CAST(CAST(3.7 AS INT) AS TEXT) as r").rows[0].r, '3');
  });

  it('implicit string-number comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 50)');
    const r = db.execute("SELECT * FROM t WHERE val = '50'");
    assert.equal(r.rows.length, 1);
  });
});

describe('NULL Arithmetic', () => {
  it('NULL + number = NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT NULL + 1 as r').rows[0].r, null);
  });

  it('number * NULL = NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT 5 * NULL as r').rows[0].r, null);
  });

  it('NULL / number = NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT NULL / 2 as r').rows[0].r, null);
  });
});

describe('Conditional Expressions', () => {
  it('NULLIF equal values returns NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT NULLIF(1, 1) as r').rows[0].r, null);
  });

  it('NULLIF different values returns first', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT NULLIF(1, 2) as r').rows[0].r, 1);
  });

  it('IIF true branch', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT IIF(1 > 0, 'yes', 'no') as r").rows[0].r, 'yes');
  });

  it('IIF false branch', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT IIF(1 < 0, 'yes', 'no') as r").rows[0].r, 'no');
  });

  it('COALESCE picks first non-null', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT COALESCE(NULL, NULL, 3, 4) as r').rows[0].r, 3);
  });

  it('nested CASE', () => {
    const db = new Database();
    const r = db.execute("SELECT CASE WHEN 1 > 0 THEN CASE WHEN 2 > 3 THEN 'inner-t' ELSE 'inner-f' END ELSE 'outer-f' END as r");
    assert.equal(r.rows[0].r, 'inner-f');
  });
});

describe('Expression Aliases', () => {
  it('ORDER BY alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 5)');
    const r = db.execute('SELECT val * 2 as doubled FROM t ORDER BY doubled DESC');
    assert.equal(r.rows[0].doubled, 40);
    assert.equal(r.rows[2].doubled, 10);
  });

  it('ORDER BY ordinal', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 30, 10), (2, 10, 20), (3, 20, 30)');
    const r = db.execute('SELECT a, b FROM t ORDER BY 2 DESC');
    assert.equal(r.rows[0].b, 30);
  });

  it('GROUP BY expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT val % 5 as bucket, COUNT(*) as cnt FROM t GROUP BY val % 5 ORDER BY bucket');
    assert.equal(r.rows.length, 5);
  });

  it('HAVING with aggregate expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 100), (4, 'B', 200)");
    const r = db.execute('SELECT grp, AVG(val) as avg_val FROM t GROUP BY grp HAVING AVG(val) > 50');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].grp, 'B');
  });
});

describe('Math Functions', () => {
  it('ABS', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ABS(-42) as r').rows[0].r, 42);
  });

  it('ROUND integer', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ROUND(3.7) as r').rows[0].r, 4);
  });

  it('ROUND decimal places', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ROUND(3.14159, 2) as r').rows[0].r, 3.14);
  });

  it('POWER', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT POWER(2, 10) as r').rows[0].r, 1024);
  });

  it('SQRT', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT SQRT(144) as r').rows[0].r, 12);
  });

  it('GREATEST and LEAST', () => {
    const db = new Database();
    const r = db.execute('SELECT GREATEST(1, 5, 3) as g, LEAST(1, 5, 3) as l');
    assert.equal(r.rows[0].g, 5);
    assert.equal(r.rows[0].l, 1);
  });

  it('integer division truncates toward zero', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT 7 / 2 as r').rows[0].r, 3);
    assert.equal(db.execute('SELECT -7 / 2 as r').rows[0].r, -3);
  });

  it('division by zero returns NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT 10 / 0 as r').rows[0].r, null);
  });
});

describe('String Functions', () => {
  it('UPPER/LOWER', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER('hello') as u, LOWER('WORLD') as l").rows[0].u, 'HELLO');
  });

  it('SUBSTRING', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT SUBSTRING('hello world', 1, 5) as r").rows[0].r, 'hello');
  });

  it('LENGTH', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LENGTH('hello') as r").rows[0].r, 5);
  });

  it('CONCAT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CONCAT('hello', ' ', 'world') as r").rows[0].r, 'hello world');
  });

  it('nested string functions', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER(SUBSTRING('hello world', 1, 5)) as r").rows[0].r, 'HELLO');
  });

  it('TRIM', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT TRIM('  hello  ') as r").rows[0].r, 'hello');
  });

  it('REPLACE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REPLACE('hello world', 'world', 'there') as r").rows[0].r, 'hello there');
  });
});

describe('Aggregate with FILTER', () => {
  it('COUNT with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'A')");
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE grp = 'A') as a_cnt, COUNT(*) as total FROM t");
    assert.equal(r.rows[0].a_cnt, 3);
    assert.equal(r.rows[0].total, 5);
  });

  it('SUM with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, active INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 1), (2, 20, 0), (3, 30, 1)');
    const r = db.execute('SELECT SUM(val) FILTER (WHERE active = 1) as active_sum, SUM(val) as total FROM t');
    assert.equal(r.rows[0].active_sum, 40);
    assert.equal(r.rows[0].total, 60);
  });
});
