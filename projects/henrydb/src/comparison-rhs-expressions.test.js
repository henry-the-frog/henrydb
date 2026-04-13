// comparison-rhs-expressions.test.js — Regression test for arithmetic expressions
// on the right side of comparison operators.
// Bug: parser called parsePrimary() instead of parsePrimaryWithConcat() for RHS,
// silently dropping arithmetic (+, -, *, /) after the first operand.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Comparison RHS Expressions', () => {
  let db;

  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
  });

  it('id = 2 - 1 returns id=1', () => {
    assert.deepStrictEqual(db.execute('SELECT id FROM t WHERE id = 2 - 1').rows, [{ id: 1 }]);
  });

  it('id = 1 + 1 returns id=2', () => {
    assert.deepStrictEqual(db.execute('SELECT id FROM t WHERE id = 1 + 1').rows, [{ id: 2 }]);
  });

  it('id = 2 * 2 returns id=4', () => {
    assert.deepStrictEqual(db.execute('SELECT id FROM t WHERE id = 2 * 2').rows, [{ id: 4 }]);
  });

  it('val = id * 10 returns all rows', () => {
    const rows = db.execute('SELECT id FROM t WHERE val = id * 10 ORDER BY id').rows;
    assert.strictEqual(rows.length, 5);
  });

  it('id > 5 - 2 returns id=4,5', () => {
    const rows = db.execute('SELECT id FROM t WHERE id > 5 - 2 ORDER BY id').rows;
    assert.deepStrictEqual(rows, [{ id: 4 }, { id: 5 }]);
  });

  it('id < 1 + 2 returns id=1,2', () => {
    const rows = db.execute('SELECT id FROM t WHERE id < 1 + 2 ORDER BY id').rows;
    assert.deepStrictEqual(rows, [{ id: 1 }, { id: 2 }]);
  });

  it('id >= 10 / 2 returns id=5', () => {
    assert.deepStrictEqual(db.execute('SELECT id FROM t WHERE id >= 10 / 2').rows, [{ id: 5 }]);
  });

  it('BETWEEN with expressions on bounds', () => {
    const rows = db.execute('SELECT id FROM t WHERE val BETWEEN 10 * 2 AND 10 * 4 ORDER BY id').rows;
    assert.deepStrictEqual(rows, [{ id: 2 }, { id: 3 }, { id: 4 }]);
  });

  it('NOT BETWEEN with expressions', () => {
    const rows = db.execute('SELECT id FROM t WHERE val NOT BETWEEN 10 + 10 AND 50 - 10 ORDER BY id').rows;
    // val NOT BETWEEN 20 AND 40 → val < 20 or val > 40 → val=10 (id=1), val=50 (id=5)
    assert.deepStrictEqual(rows, [{ id: 1 }, { id: 5 }]);
  });

  it('correlated subquery with arithmetic', () => {
    const rows = db.execute('SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.id = t1.id + 1) ORDER BY id').rows;
    assert.deepStrictEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
  });

  it('comparison with column expression on both sides', () => {
    const rows = db.execute('SELECT id FROM t WHERE id + 1 = val / 10 ORDER BY id').rows;
    // id+1 = val/10 → id+1 = id → never true (id+1 ≠ id)
    // Actually: id+1 = id*10/10 = id → only if id+1 = id, which is never true
    assert.strictEqual(rows.length, 0);
  });

  it('nested arithmetic in comparison', () => {
    const rows = db.execute('SELECT id FROM t WHERE id = (2 + 3) * 1').rows;
    assert.deepStrictEqual(rows, [{ id: 5 }]);
  });

  it('string concatenation on RHS', () => {
    db.execute('CREATE TABLE s (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO s VALUES (1, 'hello world')");
    const rows = db.execute("SELECT * FROM s WHERE name = 'hello' || ' ' || 'world'").rows;
    assert.strictEqual(rows.length, 1);
  });
});
