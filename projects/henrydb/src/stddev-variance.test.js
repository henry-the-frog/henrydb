// stddev-variance.test.js — STDDEV and VARIANCE aggregate function tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(actual, expected, tolerance = 0.001) {
  return Math.abs(actual - expected) < tolerance;
}

describe('STDDEV and VARIANCE', () => {
  it('STDDEV_POP of known values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    // Population stddev of [2,4,4,4,5,5,7,9]: mean=5, var_pop=4, stddev_pop=2
    const r = db.execute('SELECT STDDEV_POP(val) as sd FROM t');
    assert.ok(approx(r.rows[0].sd, 2.0), `Expected ~2.0, got ${r.rows[0].sd}`);
  });

  it('STDDEV_SAMP (Bessel correction)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    // Sample stddev: var_samp = 32/7 ≈ 4.571, stddev_samp ≈ 2.138
    const r = db.execute('SELECT STDDEV_SAMP(val) as sd FROM t');
    assert.ok(approx(r.rows[0].sd, 2.138, 0.01), `Expected ~2.138, got ${r.rows[0].sd}`);
  });

  it('STDDEV is alias for STDDEV_SAMP', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    const r1 = db.execute('SELECT STDDEV(val) as sd FROM t');
    const r2 = db.execute('SELECT STDDEV_SAMP(val) as sd FROM t');
    assert.equal(r1.rows[0].sd, r2.rows[0].sd);
  });

  it('VAR_POP of known values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    // Population variance = 4.0
    const r = db.execute('SELECT VAR_POP(val) as v FROM t');
    assert.ok(approx(r.rows[0].v, 4.0), `Expected ~4.0, got ${r.rows[0].v}`);
  });

  it('VAR_SAMP of known values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    // Sample variance = 32/7 ≈ 4.571
    const r = db.execute('SELECT VAR_SAMP(val) as v FROM t');
    assert.ok(approx(r.rows[0].v, 4.571, 0.01), `Expected ~4.571, got ${r.rows[0].v}`);
  });

  it('VARIANCE is alias for VAR_SAMP', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (2),(4),(4),(4),(5),(5),(7),(9)');
    const r1 = db.execute('SELECT VARIANCE(val) as v FROM t');
    const r2 = db.execute('SELECT VAR_SAMP(val) as v FROM t');
    assert.equal(r1.rows[0].v, r2.rows[0].v);
  });

  it('single value returns null for sample stats', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT STDDEV(val) as sd, VARIANCE(val) as v FROM t');
    assert.equal(r.rows[0].sd, null);
    assert.equal(r.rows[0].v, null);
  });

  it('single value: population stats return 0', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT STDDEV_POP(val) as sd, VAR_POP(val) as v FROM t');
    assert.equal(r.rows[0].sd, 0);
    assert.equal(r.rows[0].v, 0);
  });

  it('empty table returns null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    const r = db.execute('SELECT STDDEV(val) as sd, VARIANCE(val) as v, STDDEV_POP(val) as sdp, VAR_POP(val) as vp FROM t');
    assert.equal(r.rows[0].sd, null);
    assert.equal(r.rows[0].v, null);
    assert.equal(r.rows[0].sdp, null);
    assert.equal(r.rows[0].vp, null);
  });

  it('with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('A',30),('B',5),('B',5),('B',5)");
    const r = db.execute('SELECT grp, STDDEV_POP(val) as sd, VAR_POP(val) as v FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 2);
    // Group A: mean=20, var_pop=(100+0+100)/3=66.667
    assert.ok(approx(r.rows[0].v, 66.667, 0.01));
    // Group B: all 5s, var_pop=0
    assert.equal(r.rows[1].v, 0);
    assert.equal(r.rows[1].sd, 0);
  });

  it('with FLOAT values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val FLOAT)');
    db.execute('INSERT INTO t VALUES (1.5),(2.5),(3.5),(4.5)');
    // mean=3.0, var_pop=(2.25+0.25+0.25+2.25)/4=1.25
    const r = db.execute('SELECT VAR_POP(val) as v FROM t');
    assert.ok(approx(r.rows[0].v, 1.25), `Expected ~1.25, got ${r.rows[0].v}`);
  });
});
