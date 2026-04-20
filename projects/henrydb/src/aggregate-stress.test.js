// aggregate-stress.test.js — Stress tests for new aggregate functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(actual, expected, tolerance = 0.01) {
  return Math.abs(actual - expected) < tolerance;
}

describe('NULL Handling in Aggregates', () => {
  it('PERCENTILE_CONT ignores NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(NULL),(20),(NULL),(30)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as p50 FROM t');
    assert.equal(r.rows[0].p50, 20); // median of [10,20,30]
  });

  it('STDDEV ignores NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(NULL),(20),(30)');
    const r = db.execute('SELECT STDDEV_POP(val) as sd FROM t');
    // [10,20,30]: mean=20, var=(100+0+100)/3=66.67, sd=8.165
    assert.ok(approx(r.rows[0].sd, 8.165, 0.01));
  });

  it('MODE ignores NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL),(NULL),(NULL),(5),(5),(10)');
    const r = db.execute('SELECT MODE(val) as m FROM t');
    assert.equal(r.rows[0].m, 5); // 5 appears twice, 10 once
  });

  it('all NULLs returns null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL),(NULL),(NULL)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as p, STDDEV(val) as sd, MODE(val) as m FROM t');
    assert.equal(r.rows[0].p, null);
    assert.equal(r.rows[0].sd, null);
    assert.equal(r.rows[0].m, null);
  });
});

describe('Aggregates in HAVING', () => {
  it('HAVING with STDDEV filter', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (dept TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES ('eng',80),('eng',85),('eng',90),('sales',50),('sales',100),('sales',75)");
    
    const r = db.execute(`
      SELECT dept, STDDEV_POP(score) as sd
      FROM scores
      GROUP BY dept
      HAVING STDDEV_POP(score) > 10
      ORDER BY dept
    `);
    // eng: sd ≈ 4.08 (not > 10), sales: sd ≈ 20.41 (> 10)
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].dept, 'sales');
  });

  it('HAVING with VARIANCE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('A',3),('B',10),('B',10),('B',10)");
    
    const r = db.execute('SELECT grp, VARIANCE(val) as v FROM t GROUP BY grp HAVING VARIANCE(val) > 0 ORDER BY grp');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].grp, 'A'); // A has variance, B doesn't
  });
});

describe('Aggregates in Subqueries', () => {
  it('PERCENTILE in scalar subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');
    
    const r = db.execute(`
      SELECT * FROM t
      WHERE val > (SELECT PERCENTILE_CONT(val, 0.5) FROM t)
      ORDER BY id
    `);
    // median = 30, so vals > 30 are 40 and 50
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 40);
    assert.equal(r.rows[1].val, 50);
  });

  it('STDDEV in CTE', () => {
    const db = new Database();
    db.execute('CREATE TABLE measurements (sensor TEXT, reading FLOAT)');
    db.execute("INSERT INTO measurements VALUES ('A',10),('A',11),('A',10.5),('B',100),('B',200),('B',150)");
    
    const r = db.execute(`
      WITH stats AS (
        SELECT sensor, AVG(reading) as mean, STDDEV_POP(reading) as sd
        FROM measurements
        GROUP BY sensor
      )
      SELECT sensor, mean, sd FROM stats ORDER BY sensor
    `);
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows[0].sd < 1, 'Sensor A should have low stddev');
    assert.ok(r.rows[1].sd > 40, 'Sensor B should have high stddev');
  });
});

describe('Multiple Aggregates in One Query', () => {
  it('all statistical aggregates together', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');
    
    const r = db.execute(`
      SELECT 
        COUNT(val) as cnt,
        AVG(val) as mean,
        STDDEV_POP(val) as sd_pop,
        STDDEV_SAMP(val) as sd_samp,
        VAR_POP(val) as var_pop,
        VAR_SAMP(val) as var_samp,
        PERCENTILE_CONT(val, 0.5) as median,
        PERCENTILE_CONT(val, 0.25) as p25,
        PERCENTILE_CONT(val, 0.75) as p75,
        MODE(val) as mode_val,
        MIN(val) as min_val,
        MAX(val) as max_val
      FROM t
    `);
    const row = r.rows[0];
    assert.equal(row.cnt, 5);
    assert.equal(row.mean, 30);
    assert.ok(approx(row.sd_pop, 14.14, 0.01));
    assert.ok(approx(row.var_pop, 200, 0.1));
    assert.equal(row.median, 30);
    assert.equal(row.p25, 20);
    assert.equal(row.p75, 40);
    assert.equal(row.min_val, 10);
    assert.equal(row.max_val, 50);
  });

  it('aggregates with GROUP BY + ORDER BY aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (category TEXT, value INT)');
    db.execute("INSERT INTO data VALUES ('X',1),('X',2),('X',3),('Y',10),('Y',20),('Z',5),('Z',5),('Z',5)");
    
    const r = db.execute(`
      SELECT category, 
             AVG(value) as avg_val,
             STDDEV_POP(value) as sd,
             PERCENTILE_CONT(value, 0.5) as median,
             MODE(value) as mode_val
      FROM data
      GROUP BY category
      ORDER BY avg_val DESC
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].category, 'Y'); // highest avg
    assert.equal(r.rows[2].category, 'X'); // lowest avg
    assert.equal(r.rows[1].mode_val, 5); // Z's mode
    assert.equal(r.rows[1].sd, 0); // Z has no variance
  });
});

describe('Aggregate Edge Cases', () => {
  it('PERCENTILE with all same values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42),(42),(42),(42)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as p50, STDDEV_POP(val) as sd FROM t');
    assert.equal(r.rows[0].p50, 42);
    assert.equal(r.rows[0].sd, 0);
  });

  it('PERCENTILE with two values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (0),(100)');
    const r = db.execute('SELECT PERCENTILE_CONT(val, 0.5) as p50 FROM t');
    assert.equal(r.rows[0].p50, 50); // lerp(0, 100, 0.5)
  });

  it('large dataset statistics', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    // Insert 1-100
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute(`
      SELECT AVG(val) as mean, STDDEV_POP(val) as sd, 
             PERCENTILE_CONT(val, 0.5) as median,
             PERCENTILE_CONT(val, 0.25) as p25,
             PERCENTILE_CONT(val, 0.75) as p75
      FROM t
    `);
    assert.equal(r.rows[0].mean, 50.5);
    assert.equal(r.rows[0].median, 50.5); // lerp(50, 51, 0.5) = 50.5
    // stddev of 1..100: sqrt((100^2-1)/12) ≈ 28.87
    assert.ok(approx(r.rows[0].sd, 28.87, 0.1));
  });
});
