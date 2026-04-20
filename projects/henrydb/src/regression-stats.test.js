// regression-stats.test.js — CORR, COVAR, REGR aggregate tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(actual, expected, tolerance = 0.01) {
  if (actual === null || expected === null) return actual === expected;
  return Math.abs(actual - expected) < tolerance;
}

describe('Correlation and Covariance', () => {
  it('CORR: perfect positive correlation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,2),(2,4),(3,6),(4,8),(5,10)');
    const r = db.execute('SELECT CORR(y, x) as c FROM t');
    assert.ok(approx(r.rows[0].c, 1.0), `Expected ~1.0, got ${r.rows[0].c}`);
  });

  it('CORR: perfect negative correlation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,8),(3,6),(4,4),(5,2)');
    const r = db.execute('SELECT CORR(y, x) as c FROM t');
    assert.ok(approx(r.rows[0].c, -1.0), `Expected ~-1.0, got ${r.rows[0].c}`);
  });

  it('CORR: no correlation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    // Y has no relationship with X
    db.execute('INSERT INTO t VALUES (1,5),(2,5),(3,5),(4,5),(5,5)');
    const r = db.execute('SELECT CORR(y, x) as c FROM t');
    // When Y is constant, var_y = 0, so corr is null (0/0)
    assert.equal(r.rows[0].c, null);
  });

  it('COVAR_POP: known values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,2),(2,4),(3,6),(4,8),(5,10)');
    // y = 2x, meanX=3, meanY=6
    // covar_pop = sum((y-6)(x-3))/5 = sum(2(x-3)^2)/5 = 2*var_x_pop = 2*2 = 4
    const r = db.execute('SELECT COVAR_POP(y, x) as cp FROM t');
    assert.ok(approx(r.rows[0].cp, 4.0), `Expected ~4.0, got ${r.rows[0].cp}`);
  });

  it('COVAR_SAMP: Bessel correction', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,2),(2,4),(3,6),(4,8),(5,10)');
    // covar_samp = covar_pop * n/(n-1) = 4 * 5/4 = 5
    const r = db.execute('SELECT COVAR_SAMP(y, x) as cs FROM t');
    assert.ok(approx(r.rows[0].cs, 5.0), `Expected ~5.0, got ${r.rows[0].cs}`);
  });
});

describe('Linear Regression', () => {
  it('REGR_SLOPE: y = 2x + 1', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,3),(2,5),(3,7),(4,9),(5,11)');
    const r = db.execute('SELECT REGR_SLOPE(y, x) as slope FROM t');
    assert.ok(approx(r.rows[0].slope, 2.0), `Expected ~2.0, got ${r.rows[0].slope}`);
  });

  it('REGR_INTERCEPT: y = 2x + 1', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,3),(2,5),(3,7),(4,9),(5,11)');
    const r = db.execute('SELECT REGR_INTERCEPT(y, x) as intercept FROM t');
    assert.ok(approx(r.rows[0].intercept, 1.0), `Expected ~1.0, got ${r.rows[0].intercept}`);
  });

  it('REGR_R2: perfect fit', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,3),(2,5),(3,7),(4,9),(5,11)');
    const r = db.execute('SELECT REGR_R2(y, x) as r2 FROM t');
    assert.ok(approx(r.rows[0].r2, 1.0), `Expected ~1.0, got ${r.rows[0].r2}`);
  });

  it('REGR_COUNT: counts non-null pairs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1,2),(2,NULL),(NULL,6),(4,8),(5,10)');
    const r = db.execute('SELECT REGR_COUNT(y, x) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 3); // Only (1,2), (4,8), (5,10) have both non-null
  });

  it('all regression stats together', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (x FLOAT, y FLOAT)');
    db.execute('INSERT INTO t VALUES (1.0,2.1),(2.0,3.9),(3.0,6.2),(4.0,7.8),(5.0,10.1)');
    
    const r = db.execute(`
      SELECT REGR_SLOPE(y, x) as slope,
             REGR_INTERCEPT(y, x) as intercept,
             REGR_R2(y, x) as r2,
             REGR_COUNT(y, x) as cnt,
             CORR(y, x) as corr
      FROM t
    `);
    const row = r.rows[0];
    assert.equal(row.cnt, 5);
    assert.ok(row.slope > 1.8 && row.slope < 2.2, `Slope should be ~2, got ${row.slope}`);
    assert.ok(row.r2 > 0.98, `R² should be >0.98, got ${row.r2}`);
    assert.ok(row.corr > 0.99, `Corr should be >0.99, got ${row.corr}`);
  });

  it('with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, x INT, y INT)');
    db.execute("INSERT INTO t VALUES ('A',1,2),('A',2,4),('A',3,6),('B',1,10),('B',2,8),('B',3,6)");
    
    const r = db.execute('SELECT grp, REGR_SLOPE(y, x) as slope, CORR(y, x) as corr FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 2);
    // A: y = 2x, slope = 2, corr = 1
    assert.ok(approx(r.rows[0].slope, 2.0));
    assert.ok(approx(r.rows[0].corr, 1.0));
    // B: y = -2x + 12, slope = -2, corr = -1
    assert.ok(approx(r.rows[1].slope, -2.0));
    assert.ok(approx(r.rows[1].corr, -1.0));
  });
});
