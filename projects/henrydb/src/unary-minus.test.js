// unary-minus.test.js — Test unary minus support
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Unary minus support', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE nums (val INTEGER, name TEXT)');
    db.execute("INSERT INTO nums VALUES (10, 'a')");
    db.execute("INSERT INTO nums VALUES (-5, 'b')");
    db.execute("INSERT INTO nums VALUES (0, 'c')");
    db.execute("INSERT INTO nums VALUES (3, 'd')");
    return db;
  }

  it('SELECT -val from table', () => {
    const db = makeDb();
    const r = db.execute('SELECT -val AS neg FROM nums ORDER BY val');
    assert.deepStrictEqual(r.rows.map(r => r.neg), [5, 0, -3, -10]);
  });

  it('WHERE -val > 0 (select negative originals)', () => {
    const db = makeDb();
    const r = db.execute('SELECT name FROM nums WHERE -val > 0');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['b']);
  });

  it('ORDER BY -val (descending without DESC)', () => {
    const db = makeDb();
    const r = db.execute('SELECT val FROM nums ORDER BY -val');
    // -val of 10=-10, -5=5, 0=0, 3=-3
    // ASC order: -10, -3, 0, 5 → original: 10, 3, 0, -5
    assert.deepStrictEqual(r.rows.map(r => r.val), [10, 3, 0, -5]);
  });

  it('arithmetic with unary minus: -val + 100', () => {
    const db = makeDb();
    const r = db.execute('SELECT -val + 100 AS result FROM nums WHERE val = 10');
    assert.strictEqual(r.rows[0].result, 90);
  });

  it('double negative: -(-val)', () => {
    const db = makeDb();
    const r = db.execute('SELECT -(-val) AS result FROM nums WHERE val = 10');
    assert.strictEqual(r.rows[0].result, 10);
  });

  it('unary minus with literal: SELECT -5', () => {
    const db = makeDb();
    const r = db.execute('SELECT -5 AS neg');
    assert.strictEqual(r.rows[0].neg, -5);
  });

  it('unary minus with NULL returns NULL', () => {
    const db = makeDb();
    db.execute("INSERT INTO nums VALUES (NULL, 'e')");
    const r = db.execute("SELECT -val AS neg FROM nums WHERE name = 'e'");
    assert.strictEqual(r.rows[0].neg, null);
  });

  it('unary minus in expression: val * -1', () => {
    const db = makeDb();
    const r = db.execute('SELECT val * -1 AS neg FROM nums WHERE val = 10');
    assert.strictEqual(r.rows[0].neg, -10);
  });
});
