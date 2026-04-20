// percentile-mode.test.js — PERCENTILE_CONT, PERCENTILE_DISC, MODE tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('PERCENTILE_CONT', () => {
  it('median of odd count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as median FROM t');
    assert.equal(r.rows[0].median, 30);
  });

  it('median of even count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as median FROM t');
    assert.equal(r.rows[0].median, 25); // lerp(20, 30, 0.5) = 25
  });

  it('P0 returns minimum', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0) as p0 FROM t');
    assert.equal(r.rows[0].p0, 10);
  });

  it('P100 returns maximum', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 1) as p100 FROM t');
    assert.equal(r.rows[0].p100, 30);
  });

  it('P25 and P75 with interpolation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (0),(10),(20),(30),(40)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.25) as p25, PERCENTILE_CONT(val, 0.75) as p75 FROM t');
    assert.equal(r.rows[0].p25, 10);
    assert.equal(r.rows[0].p75, 30);
  });

  it('single value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as median FROM t');
    assert.equal(r.rows[0].median, 42);
  });

  it('empty table returns null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as median FROM t');
    assert.equal(r.rows[0].median, null);
  });

  it('with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('A',30),('B',100),('B',200)");
    const r = db.execute('SELECT grp, PERCENTILE_CONT(val, 0.5) as median FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows[0].grp, 'A');
    assert.equal(r.rows[0].median, 20);
    assert.equal(r.rows[1].grp, 'B');
    assert.equal(r.rows[1].median, 150);
  });
});

describe('PERCENTILE_DISC', () => {
  it('median returns actual value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');
    const r = db.execute('SELECT PERCENTILE_DISC(val, 0.5) as median FROM t');
    assert.equal(r.rows[0].median, 30);
  });

  it('P25 returns discrete value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40)');
    const r = db.execute('SELECT PERCENTILE_DISC(val, 0.25) as p25 FROM t');
    assert.equal(r.rows[0].p25, 10);
  });

  it('P0 returns minimum', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');
    const r = db.execute('SELECT PERCENTILE_DISC(val, 0) as p0 FROM t');
    assert.equal(r.rows[0].p0, 10);
  });
});

describe('MODE', () => {
  it('returns most frequent value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(30),(30),(40)');
    const r = db.execute('SELECT MODE(val) as m FROM t');
    assert.equal(r.rows[0].m, 30);
  });

  it('single value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT MODE(val) as m FROM t');
    assert.equal(r.rows[0].m, 42);
  });

  it('empty table returns null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    const r = db.execute('SELECT MODE(val) as m FROM t');
    assert.equal(r.rows[0].m, null);
  });

  it('with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1),('A',1),('A',2),('B',3),('B',4),('B',4),('B',4)");
    const r = db.execute('SELECT grp, MODE(val) as m FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows[0].grp, 'A');
    assert.equal(r.rows[0].m, 1);
    assert.equal(r.rows[1].grp, 'B');
    assert.equal(r.rows[1].m, 4);
  });
});
