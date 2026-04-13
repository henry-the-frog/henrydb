// expression-edge-cases.test.js — Regression tests for parser expression handling
// Covers: IN with expressions, INSERT with expressions, NULL/falsy in CASE WHEN,
// BETWEEN with expressions, comparison RHS arithmetic (collected in one place).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Expression Edge Cases', () => {
  let db;

  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10}, 'name${i}')`);
  });

  // IN list with expressions
  it('IN list with addition', () => {
    const r = db.execute('SELECT id FROM t WHERE id IN (1 + 1, 2 + 1) ORDER BY id');
    assert.deepStrictEqual(r.rows, [{ id: 2 }, { id: 3 }]);
  });

  it('IN list with multiplication', () => {
    const r = db.execute('SELECT id FROM t WHERE id IN (2 * 3, 4 * 2) ORDER BY id');
    assert.deepStrictEqual(r.rows, [{ id: 6 }, { id: 8 }]);
  });

  it('NOT IN with expressions', () => {
    const r = db.execute('SELECT id FROM t WHERE id NOT IN (1 * 2, 3 - 1) AND id < 5 ORDER BY id');
    // NOT IN (2, 2) = exclude id=2
    assert.deepStrictEqual(r.rows, [{ id: 1 }, { id: 3 }, { id: 4 }]);
  });

  // INSERT with expressions
  it('INSERT with arithmetic values', () => {
    db.execute('CREATE TABLE expr_insert (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO expr_insert VALUES (1, 5 * 20)');
    db.execute('INSERT INTO expr_insert VALUES (2, 10 + 5)');
    db.execute('INSERT INTO expr_insert VALUES (3, 100 - 30)');
    db.execute('INSERT INTO expr_insert VALUES (4, 100 / 4)');
    const rows = db.execute('SELECT * FROM expr_insert ORDER BY id').rows;
    assert.strictEqual(rows[0].val, 100);
    assert.strictEqual(rows[1].val, 15);
    assert.strictEqual(rows[2].val, 70);
    assert.strictEqual(rows[3].val, 25);
  });

  // NULL in CASE WHEN (SQL standard: NULL is falsy)
  it('CASE WHEN NULL returns ELSE', () => {
    const r = db.execute("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END as x");
    assert.strictEqual(r.rows[0].x, 'no');
  });

  it('CASE WHEN 0 returns ELSE', () => {
    const r = db.execute("SELECT CASE WHEN 0 THEN 'yes' ELSE 'no' END as x");
    assert.strictEqual(r.rows[0].x, 'no');
  });

  it('CASE WHEN 1 returns THEN', () => {
    const r = db.execute("SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END as x");
    assert.strictEqual(r.rows[0].x, 'yes');
  });

  // NULL arithmetic
  it('NULL arithmetic propagates NULL', () => {
    const r = db.execute('SELECT 1 + NULL as a, NULL * 5 as b, NULL - 1 as c');
    assert.strictEqual(r.rows[0].a, null);
    assert.strictEqual(r.rows[0].b, null);
    assert.strictEqual(r.rows[0].c, null);
  });

  // Comparison RHS expressions
  it('comparison with subtraction on RHS', () => {
    assert.strictEqual(db.execute('SELECT id FROM t WHERE id = 5 - 3').rows[0].id, 2);
  });

  it('comparison with multiplication on RHS', () => {
    assert.strictEqual(db.execute('SELECT id FROM t WHERE val = 3 * 10').rows[0].id, 3);
  });

  it('comparison with column expressions on both sides', () => {
    const r = db.execute('SELECT id FROM t WHERE val = id * 10 ORDER BY id');
    assert.strictEqual(r.rows.length, 10); // All rows match
  });

  // BETWEEN with expressions
  it('BETWEEN with arithmetic bounds', () => {
    const r = db.execute('SELECT id FROM t WHERE val BETWEEN 10 * 2 AND 10 * 4 ORDER BY id');
    assert.deepStrictEqual(r.rows, [{ id: 2 }, { id: 3 }, { id: 4 }]);
  });

  // Correlated EXISTS with arithmetic
  it('EXISTS with arithmetic in condition', () => {
    const r = db.execute('SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.id = t1.id + 1) AND t1.id <= 5 ORDER BY id');
    // ids 1-9 have a successor, id 10 doesn't; limited to id <= 5
    assert.deepStrictEqual(r.rows, [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]);
  });

  // UPDATE with complex SET
  it('UPDATE SET with arithmetic', () => {
    db.execute('CREATE TABLE upd (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO upd VALUES (1, 10)');
    db.execute('UPDATE upd SET val = val * 2 + 5 WHERE id = 1');
    assert.strictEqual(db.execute('SELECT val FROM upd WHERE id = 1').rows[0].val, 25);
  });

  // Negative numbers
  it('INSERT and SELECT negative numbers', () => {
    db.execute('CREATE TABLE neg (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO neg VALUES (-1, -100)');
    assert.strictEqual(db.execute('SELECT * FROM neg WHERE id = -1').rows[0].val, -100);
  });

  // Standalone VALUES with expressions
  it('VALUES with arithmetic', () => {
    const r = db.execute('VALUES (1 + 1, 2 * 3)');
    assert.strictEqual(r.rows[0].column1, 2);
    assert.strictEqual(r.rows[0].column2, 6);
  });
});
