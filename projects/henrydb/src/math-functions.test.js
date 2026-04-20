// math-functions.test.js — Tests for math functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(actual, expected, tol = 0.001) {
  return Math.abs(actual - expected) < tol;
}

describe('Math Functions', () => {
  it('LN(1) = 0', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT LN(1) as r').rows[0].r, 0);
  });

  it('LN(e) ≈ 1', () => {
    const db = new Database();
    const r = db.execute('SELECT LN(EXP(1)) as r').rows[0].r;
    assert.ok(approx(r, 1.0));
  });

  it('LOG10(100) = 2', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT LOG10(100) as r').rows[0].r, 2);
  });

  it('LOG2(8) = 3', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT LOG2(8) as r').rows[0].r, 3);
  });

  it('SIGN positive', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT SIGN(42) as r').rows[0].r, 1);
  });

  it('SIGN negative', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT SIGN(-42) as r').rows[0].r, -1);
  });

  it('SIGN zero', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT SIGN(0) as r').rows[0].r, 0);
  });

  it('PI()', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT PI() as r').rows[0].r, Math.PI));
  });

  it('DEGREES(PI()) = 180', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT DEGREES(PI()) as r').rows[0].r, 180));
  });

  it('RADIANS(180) = PI', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT RADIANS(180) as r').rows[0].r, Math.PI));
  });

  it('SIN(0) = 0', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT SIN(0) as r').rows[0].r, 0));
  });

  it('COS(0) = 1', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT COS(0) as r').rows[0].r, 1);
  });

  it('SIN²(x) + COS²(x) = 1', () => {
    const db = new Database();
    const r = db.execute('SELECT POWER(SIN(1.5), 2) + POWER(COS(1.5), 2) as r').rows[0].r;
    assert.ok(approx(r, 1.0));
  });

  it('ATAN2(1, 1) = PI/4', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT ATAN2(1, 1) as r').rows[0].r, Math.PI / 4));
  });

  it('ASIN(1) = PI/2', () => {
    const db = new Database();
    assert.ok(approx(db.execute('SELECT ASIN(1) as r').rows[0].r, Math.PI / 2));
  });

  it('math functions in expressions', () => {
    const db = new Database();
    db.execute('CREATE TABLE angles (deg INT)');
    db.execute('INSERT INTO angles VALUES (0),(30),(45),(60),(90)');
    
    const r = db.execute(`
      SELECT deg, SIN(RADIANS(deg)) as sin_val, COS(RADIANS(deg)) as cos_val
      FROM angles ORDER BY deg
    `);
    assert.equal(r.rows.length, 5);
    assert.ok(approx(r.rows[0].sin_val, 0)); // sin(0) = 0
    assert.ok(approx(r.rows[0].cos_val, 1)); // cos(0) = 1
    assert.ok(approx(r.rows[2].sin_val, Math.SQRT2 / 2, 0.001)); // sin(45°)
    assert.ok(approx(r.rows[4].sin_val, 1)); // sin(90°) = 1
  });

  it('ABS works', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ABS(-5) as r').rows[0].r, 5);
    assert.equal(db.execute('SELECT ABS(5) as r').rows[0].r, 5);
    assert.equal(db.execute('SELECT ABS(0) as r').rows[0].r, 0);
  });

  it('CEIL and FLOOR', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CEIL(3.2) as r').rows[0].r, 4);
    assert.equal(db.execute('SELECT FLOOR(3.8) as r').rows[0].r, 3);
    assert.equal(db.execute('SELECT CEIL(-1.5) as r').rows[0].r, -1);
    assert.equal(db.execute('SELECT FLOOR(-1.5) as r').rows[0].r, -2);
  });

  it('ROUND with precision', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ROUND(3.14159, 2) as r').rows[0].r, 3.14);
    assert.equal(db.execute('SELECT ROUND(3.14159, 4) as r').rows[0].r, 3.1416);
  });

  it('POWER', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT POWER(2, 10) as r').rows[0].r, 1024);
    assert.equal(db.execute('SELECT POWER(3, 0) as r').rows[0].r, 1);
  });

  it('SQRT', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT SQRT(144) as r').rows[0].r, 12);
    assert.equal(db.execute('SELECT SQRT(0) as r').rows[0].r, 0);
  });
});
