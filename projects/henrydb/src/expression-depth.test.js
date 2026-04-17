// expression-depth.test.js — SQL expression evaluation correctness tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-expr-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('CASE WHEN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('simple CASE WHEN', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('INSERT INTO t VALUES (1, 90)');
    db.execute('INSERT INTO t VALUES (2, 75)');
    db.execute('INSERT INTO t VALUES (3, 55)');
    db.execute('INSERT INTO t VALUES (4, NULL)');

    const r = rows(db.execute(
      "SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END AS grade FROM t ORDER BY id"
    ));
    assert.equal(r[0].grade, 'A');
    assert.equal(r[1].grade, 'B');
    assert.equal(r[2].grade, 'C');
    // NULL score: WHEN conditions are false, falls to ELSE
    assert.equal(r[3].grade, 'C');
  });

  it('CASE WHEN without ELSE returns NULL', () => {
    const r = rows(db.execute(
      "SELECT CASE WHEN 1 = 0 THEN 'yes' END AS result"
    ));
    assert.equal(r[0].result, null);
  });

  it('searched CASE', () => {
    const r = rows(db.execute(
      "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END AS result"
    ));
    assert.equal(r[0].result, 'two');
  });
});

describe('COALESCE and NULLIF', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('COALESCE returns first non-null', () => {
    const r = rows(db.execute("SELECT COALESCE(NULL, NULL, 'hello', 'world') AS result"));
    assert.equal(r[0].result, 'hello');
  });

  it('COALESCE with all NULLs returns NULL', () => {
    const r = rows(db.execute('SELECT COALESCE(NULL, NULL) AS result'));
    assert.equal(r[0].result, null);
  });

  it('NULLIF returns NULL when values are equal', () => {
    const r = rows(db.execute('SELECT NULLIF(5, 5) AS result'));
    assert.equal(r[0].result, null);
  });

  it('NULLIF returns first value when not equal', () => {
    const r = rows(db.execute('SELECT NULLIF(5, 3) AS result'));
    assert.equal(r[0].result, 5);
  });
});

describe('String Functions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPPER and LOWER', () => {
    const r = rows(db.execute("SELECT UPPER('hello') AS u, LOWER('WORLD') AS l"));
    assert.equal(r[0].u, 'HELLO');
    assert.equal(r[0].l, 'world');
  });

  it('SUBSTR / SUBSTRING', () => {
    const r = rows(db.execute("SELECT SUBSTR('abcdef', 2, 3) AS result"));
    // SQL SUBSTR is 1-indexed: SUBSTR('abcdef', 2, 3) = 'bcd'
    assert.equal(r[0].result, 'bcd');
  });

  it('LENGTH', () => {
    const r = rows(db.execute("SELECT LENGTH('hello') AS len"));
    assert.equal(r[0].len, 5);
  });

  it('CONCAT or || operator', () => {
    const r = rows(db.execute("SELECT 'hello' || ' ' || 'world' AS result"));
    assert.equal(r[0].result, 'hello world');
  });

  it('TRIM', () => {
    const r = rows(db.execute("SELECT TRIM('  hello  ') AS result"));
    assert.equal(r[0].result, 'hello');
  });

  it('REPLACE', () => {
    const r = rows(db.execute("SELECT REPLACE('hello world', 'world', 'SQL') AS result"));
    assert.equal(r[0].result, 'hello SQL');
  });

  it('string functions with NULL', () => {
    const r = rows(db.execute("SELECT UPPER(NULL) AS u, LENGTH(NULL) AS l"));
    assert.equal(r[0].u, null);
    assert.equal(r[0].l, null);
  });
});

describe('Math Functions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ABS', () => {
    const r = rows(db.execute('SELECT ABS(-5) AS result'));
    assert.equal(r[0].result, 5);
  });

  it('ROUND', () => {
    const r = rows(db.execute('SELECT ROUND(3.7) AS r1, ROUND(3.14159, 2) AS r2'));
    assert.equal(r[0].r1, 4);
    assert.ok(Math.abs(r[0].r2 - 3.14) < 0.001);
  });

  it('MOD / modulo', () => {
    const r = rows(db.execute('SELECT 17 % 5 AS result'));
    assert.equal(r[0].result, 2);
  });

  it('arithmetic operations', () => {
    const r = rows(db.execute('SELECT 2 + 3 AS add, 10 - 4 AS sub, 3 * 7 AS mul, 15 / 4 AS div'));
    assert.equal(r[0].add, 5);
    assert.equal(r[0].sub, 6);
    assert.equal(r[0].mul, 21);
    // Integer division
    assert.ok(r[0].div === 3 || r[0].div === 3.75, `Division: ${r[0].div}`);
  });

  it('math with NULL propagates NULL', () => {
    const r = rows(db.execute('SELECT NULL + 5 AS add, NULL * 3 AS mul, ABS(NULL) AS abs'));
    assert.equal(r[0].add, null);
    assert.equal(r[0].mul, null);
    assert.equal(r[0].abs, null);
  });
});

describe('CAST / Type Conversion', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CAST string to integer', () => {
    const r = rows(db.execute("SELECT CAST('42' AS INT) AS result"));
    assert.equal(r[0].result, 42);
  });

  it('CAST integer to text', () => {
    const r = rows(db.execute("SELECT CAST(42 AS TEXT) AS result"));
    assert.equal(r[0].result, '42');
  });

  it('CAST NULL preserves NULL', () => {
    const r = rows(db.execute("SELECT CAST(NULL AS INT) AS result"));
    assert.equal(r[0].result, null);
  });
});

describe('Complex Expressions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('nested CASE with COALESCE', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 5)');

    const r = rows(db.execute(
      "SELECT id, CASE WHEN COALESCE(val, 0) > 0 THEN 'positive' ELSE 'zero-or-null' END AS result FROM t ORDER BY id"
    ));
    assert.equal(r[0].result, 'zero-or-null');
    assert.equal(r[1].result, 'positive');
  });

  it('expression in WHERE clause', () => {
    db.execute('CREATE TABLE t (name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES ('Alice', 25)");
    db.execute("INSERT INTO t VALUES ('Bob', 30)");

    const r = rows(db.execute("SELECT name FROM t WHERE UPPER(name) = 'ALICE'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Alice');
  });

  it('expression in ORDER BY', () => {
    db.execute('CREATE TABLE t (name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES ('A', 3)");
    db.execute("INSERT INTO t VALUES ('B', 1)");
    db.execute("INSERT INTO t VALUES ('C', 2)");

    const r = rows(db.execute('SELECT name FROM t ORDER BY score * -1'));
    assert.equal(r[0].name, 'A'); // score 3, *-1 = -3 (smallest → first)
    assert.equal(r[2].name, 'B'); // score 1, *-1 = -1 (largest → last)
  });
});
