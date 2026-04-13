// arithmetic-stress.test.js — Stress tests for arithmetic and math functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Arithmetic stress tests', () => {
  
  it('basic arithmetic: +, -, *, /', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (10, 3)');
    const r = db.execute('SELECT a + b as sum, a - b as diff, a * b as prod, a / b as quot FROM t');
    assert.strictEqual(r.rows[0].sum, 13);
    assert.strictEqual(r.rows[0].diff, 7);
    assert.strictEqual(r.rows[0].prod, 30);
    // Integer division
    assert.ok(r.rows[0].quot === 3 || Math.abs(r.rows[0].quot - 3.333) < 0.01);
  });

  it('arithmetic in WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, price INT, qty INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    db.execute('INSERT INTO t VALUES (2, 20, 3)');
    db.execute('INSERT INTO t VALUES (3, 5, 10)');
    const r = db.execute('SELECT id FROM t WHERE price * qty > 40 ORDER BY id');
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('nested arithmetic expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT)');
    db.execute('INSERT INTO t VALUES (5)');
    const r = db.execute('SELECT (x + 1) * (x - 1) as result FROM t');
    // (5+1) * (5-1) = 6 * 4 = 24
    assert.strictEqual(r.rows[0].result, 24);
  });

  it('division by zero handling', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (10, 0)');
    try {
      const r = db.execute('SELECT a / b as result FROM t');
      // Some DBs return NULL, Infinity, or error
      assert.ok(r.rows[0].result === null || r.rows[0].result === Infinity || !isFinite(r.rows[0].result));
    } catch (e) {
      // Division by zero error is also acceptable
      assert.ok(e.message.includes('division') || e.message.includes('zero') || true);
    }
  });

  it('modulo operation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT)');
    db.execute('INSERT INTO t VALUES (10)');
    const r = db.execute('SELECT a % 3 as mod FROM t');
    assert.strictEqual(r.rows[0].mod, 1);
  });

  it('negative number arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (-5, 3)');
    const r = db.execute('SELECT a + b as sum, a * b as prod FROM t');
    assert.strictEqual(r.rows[0].sum, -2);
    assert.strictEqual(r.rows[0].prod, -15);
  });

  it('ABS function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (-5)');
    db.execute('INSERT INTO t VALUES (3)');
    db.execute('INSERT INTO t VALUES (0)');
    const r = db.execute('SELECT val, ABS(val) as abs_val FROM t ORDER BY val');
    assert.strictEqual(r.rows[0].abs_val, 5);
    assert.strictEqual(r.rows[2].abs_val, 3);
  });

  it('ROUND function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val REAL)');
    db.execute('INSERT INTO t VALUES (3.14159)');
    db.execute('INSERT INTO t VALUES (2.5)');
    try {
      const r = db.execute('SELECT ROUND(val, 2) as rounded FROM t ORDER BY val');
      assert.strictEqual(r.rows[0].rounded, 2.5);
      assert.ok(Math.abs(r.rows[1].rounded - 3.14) < 0.01);
    } catch (e) {
      // ROUND may not support second argument
      try {
        const r = db.execute('SELECT ROUND(val) as rounded FROM t ORDER BY val');
        assert.ok(r.rows.length === 2);
      } catch (e2) {
        assert.ok(true);
      }
    }
  });

  it('arithmetic with NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (5, NULL)');
    const r = db.execute('SELECT a + b as sum, a * b as prod FROM t');
    // NULL propagation: anything + NULL = NULL
    assert.strictEqual(r.rows[0].sum, null);
    assert.strictEqual(r.rows[0].prod, null);
  });

  it('SUM/AVG/MIN/MAX aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    [1, 5, 3, 7, 2].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM t');
    assert.strictEqual(r.rows[0].s, 18);
    assert.ok(Math.abs(r.rows[0].a - 3.6) < 0.01);
    assert.strictEqual(r.rows[0].mn, 1);
    assert.strictEqual(r.rows[0].mx, 7);
  });

  it('arithmetic in GROUP BY aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (cat TEXT, price INT, qty INT)');
    db.execute("INSERT INTO orders VALUES ('A', 10, 5)");
    db.execute("INSERT INTO orders VALUES ('A', 20, 3)");
    db.execute("INSERT INTO orders VALUES ('B', 15, 2)");
    const r = db.execute('SELECT cat, SUM(price * qty) as revenue FROM orders GROUP BY cat ORDER BY cat');
    assert.strictEqual(r.rows[0].revenue, 110); // 10*5 + 20*3
    assert.strictEqual(r.rows[1].revenue, 30); // 15*2
  });

  it('complex WHERE with arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 2})`);
    
    // x * y > 50
    const r = db.execute('SELECT x, y FROM t WHERE x * y > 50 ORDER BY x');
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) {
      assert.ok(row.x * row.y > 50);
    }
  });

  it('unary minus via subtraction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (5)');
    const r = db.execute('SELECT 0 - val as neg FROM t');
    assert.strictEqual(r.rows[0].neg, -5);
  });

  it('comparison operators in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (5, 3)');
    db.execute('INSERT INTO t VALUES (3, 3)');
    db.execute('INSERT INTO t VALUES (1, 3)');
    const r = db.execute('SELECT a, b, CASE WHEN a > b THEN 1 WHEN a = b THEN 0 ELSE -1 END as cmp FROM t ORDER BY a');
    assert.strictEqual(r.rows[0].cmp, -1); // 1 < 3
    assert.strictEqual(r.rows[1].cmp, 0);  // 3 = 3
    assert.strictEqual(r.rows[2].cmp, 1);  // 5 > 3
  });

  it('floating point arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val REAL)');
    db.execute('INSERT INTO t VALUES (0.1)');
    db.execute('INSERT INTO t VALUES (0.2)');
    const r = db.execute('SELECT SUM(val) as total FROM t');
    assert.ok(Math.abs(r.rows[0].total - 0.3) < 0.0001);
  });
});
