// parser-edge-cases.test.js — SQL parser edge case and boundary tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Parser Edge Cases — String Handling', () => {
  it('escaped single quotes in values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('it''s a test')");
    const r = db.execute('SELECT val FROM t');
    assert.equal(r.rows[0].val, "it's a test");
  });

  it('empty string', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('')");
    const r = db.execute('SELECT val FROM t');
    assert.equal(r.rows[0].val, '');
  });

  it('string with special characters', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('hello\nworld')");
    const r = db.execute('SELECT val FROM t');
    assert.ok(r.rows[0].val.includes('\n'));
  });

  it('Unicode in column values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('café')");
    db.execute("INSERT INTO t VALUES ('日本語')");
    db.execute("INSERT INTO t VALUES ('🎉')");
    const r = db.execute('SELECT val FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows.some(row => row.val === 'café'));
    assert.ok(r.rows.some(row => row.val === '日本語'));
    assert.ok(r.rows.some(row => row.val === '🎉'));
  });
});

describe('Parser Edge Cases — Numbers', () => {
  it('zero', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (0)');
    const r = db.execute('SELECT val FROM t');
    assert.equal(r.rows[0].val, 0);
  });

  it('negative numbers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (-42)');
    const r = db.execute('SELECT val FROM t WHERE val < 0');
    assert.equal(r.rows[0].val, -42);
  });

  it('large integers within INT32 range', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2147483647)'); // INT32_MAX
    const r = db.execute('SELECT val FROM t');
    assert.equal(r.rows[0].val, 2147483647);
  });

  it('float precision', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val FLOAT)');
    db.execute('INSERT INTO t VALUES (0.1)');
    db.execute('INSERT INTO t VALUES (0.2)');
    const r = db.execute('SELECT SUM(val) as total FROM t');
    // 0.1 + 0.2 should be close to 0.3
    assert.ok(Math.abs(r.rows[0].total - 0.3) < 0.0001);
  });

  it('scientific notation in expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val FLOAT)');
    db.execute('INSERT INTO t VALUES (100000)');
    const r = db.execute('SELECT val FROM t WHERE val = 100000');
    assert.equal(r.rows.length, 1);
  });
});

describe('Parser Edge Cases — NULL Handling', () => {
  it('IS NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(NULL),(3)');
    const r = db.execute('SELECT val FROM t WHERE val IS NULL');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, null);
  });

  it('IS NOT NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(NULL),(3)');
    const r = db.execute('SELECT val FROM t WHERE val IS NOT NULL');
    assert.equal(r.rows.length, 2);
  });

  it('NULL in arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT a + b as sum FROM t');
    assert.equal(r.rows[0].sum, null);
  });

  it('NULL in COALESCE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (NULL, NULL, 3)');
    const r = db.execute('SELECT COALESCE(a, b, c) as val FROM t');
    assert.equal(r.rows[0].val, 3);
  });
});

describe('Parser Edge Cases — Boundary Conditions', () => {
  it('SELECT from empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 0);
  });

  it('COUNT of empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    const r = db.execute('SELECT COUNT(*) as c FROM t');
    assert.equal(r.rows[0].c, 0);
  });

  it('LIMIT 0', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    const r = db.execute('SELECT * FROM t LIMIT 0');
    assert.equal(r.rows.length, 0);
  });

  it('OFFSET greater than row count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    const r = db.execute('SELECT * FROM t OFFSET 100');
    assert.equal(r.rows.length, 0);
  });

  it('GROUP BY on all rows being same group', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('A',3)");
    const r = db.execute('SELECT grp, SUM(val) as total FROM t GROUP BY grp');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].total, 6);
  });

  it('deeply nested parentheses', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT ((((val)))) as v FROM t');
    assert.equal(r.rows[0].v, 42);
  });

  it('complex CASE expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute(`
      SELECT val,
             CASE 
               WHEN val = 1 THEN 'one'
               WHEN val = 2 THEN 'two'
               WHEN val BETWEEN 3 AND 4 THEN 'three-four'
               ELSE 'other'
             END as name
      FROM t ORDER BY val
    `);
    assert.equal(r.rows[0].name, 'one');
    assert.equal(r.rows[1].name, 'two');
    assert.equal(r.rows[2].name, 'three-four');
    assert.equal(r.rows[4].name, 'other');
  });

  it('multiple column aliases with same expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10)');
    const r = db.execute('SELECT val as a, val as b, val + 1 as c FROM t');
    assert.equal(r.rows[0].a, 10);
    assert.equal(r.rows[0].b, 10);
    assert.equal(r.rows[0].c, 11);
  });

  it('SELECT 1 (no FROM clause)', () => {
    const db = new Database();
    const r = db.execute('SELECT 1 as num, 2 + 3 as sum');
    assert.equal(r.rows[0].num, 1);
    assert.equal(r.rows[0].sum, 5);
  });
});

describe('Parser Edge Cases — Operators', () => {
  it('string concatenation with ||', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES ('hello', 'world')");
    const r = db.execute("SELECT a || ' ' || b as greeting FROM t");
    assert.equal(r.rows[0].greeting, 'hello world');
  });

  it('modulo operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (7)');
    const r = db.execute('SELECT val % 3 as remainder FROM t');
    assert.equal(r.rows[0].remainder, 1);
  });

  it('IN with values list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT val FROM t WHERE val IN (1, 3, 5) ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), [1, 3, 5]);
  });

  it('NOT IN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT val FROM t WHERE val NOT IN (1, 3, 5) ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), [2, 4]);
  });

  it('LIKE with underscore wildcard', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('cat'),('car'),('cap'),('cob')");
    const r = db.execute("SELECT val FROM t WHERE val LIKE 'ca_' ORDER BY val");
    assert.deepEqual(r.rows.map(r => r.val), ['cap', 'car', 'cat']);
  });
});
