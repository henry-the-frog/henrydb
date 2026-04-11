// operator-precedence.test.js — Verify SQL arithmetic operator precedence
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Operator Precedence', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE vals (a INT, b INT, c INT)');
    db.execute('INSERT INTO vals VALUES (2, 3, 4)');
    db.execute('INSERT INTO vals VALUES (10, 5, 2)');
  });

  // * / % bind tighter than + -
  it('2 + 3 * 4 = 14', () => assert.strictEqual(db.execute('SELECT 2 + 3 * 4 as r').rows[0].r, 14));
  it('2 * 3 + 4 = 10', () => assert.strictEqual(db.execute('SELECT 2 * 3 + 4 as r').rows[0].r, 10));
  it('10 - 2 * 3 = 4', () => assert.strictEqual(db.execute('SELECT 10 - 2 * 3 as r').rows[0].r, 4));
  it('10 / 2 + 3 = 8', () => assert.strictEqual(db.execute('SELECT 10 / 2 + 3 as r').rows[0].r, 8));
  it('2 + 3 * 4 - 1 = 13', () => assert.strictEqual(db.execute('SELECT 2 + 3 * 4 - 1 as r').rows[0].r, 13));
  it('10 % 3 + 1 = 2', () => assert.strictEqual(db.execute('SELECT 10 % 3 + 1 as r').rows[0].r, 2));

  // Parentheses override precedence
  it('(2 + 3) * 4 = 20', () => assert.strictEqual(db.execute('SELECT (2 + 3) * 4 as r').rows[0].r, 20));
  it('(10 - 2) / 4 = 2', () => assert.strictEqual(db.execute('SELECT (10 - 2) / 4 as r').rows[0].r, 2));
  it('2 * (3 + 4) = 14', () => assert.strictEqual(db.execute('SELECT 2 * (3 + 4) as r').rows[0].r, 14));
  it('(1 + 2) * (3 + 4) = 21', () => assert.strictEqual(db.execute('SELECT (1 + 2) * (3 + 4) as r').rows[0].r, 21));

  // Left-to-right within same precedence
  it('10 - 3 - 2 = 5', () => assert.strictEqual(db.execute('SELECT 10 - 3 - 2 as r').rows[0].r, 5));
  it('100 / 10 / 2 = 5', () => assert.strictEqual(db.execute('SELECT 100 / 10 / 2 as r').rows[0].r, 5));

  // Column references with precedence
  it('a + b * c (columns)', () => {
    const r = db.execute('SELECT a + b * c as r FROM vals');
    assert.strictEqual(r.rows[0].r, 14);  // 2 + 3*4 = 14
    assert.strictEqual(r.rows[1].r, 20);  // 10 + 5*2 = 20
  });

  // In WHERE clause
  it('WHERE with precedence', () => {
    const r = db.execute('SELECT * FROM vals WHERE a + b * c > 15');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].a, 10);
  });

  // Negative numbers
  it('negative number: -5 + 3 = -2', () => assert.strictEqual(db.execute('SELECT -5 + 3 as r').rows[0].r, -2));

  // Complex expressions
  it('3 * 4 + 5 * 6 = 42', () => assert.strictEqual(db.execute('SELECT 3 * 4 + 5 * 6 as r').rows[0].r, 42));
  it('(3 + 4) * (5 - 2) = 21', () => assert.strictEqual(db.execute('SELECT (3 + 4) * (5 - 2) as r').rows[0].r, 21));
});
