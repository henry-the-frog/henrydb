// window-frames.test.js — Window frame specification tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function approx(a, b, tol = 0.01) { return Math.abs(a - b) < tol; }

describe('Window Frames', () => {
  it('ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');
    
    const r = db.execute(`
      SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rt
      FROM t ORDER BY id
    `);
    assert.equal(r.rows[0].rt, 10);
    assert.equal(r.rows[1].rt, 30);
    assert.equal(r.rows[2].rt, 60);
    assert.equal(r.rows[3].rt, 100);
    assert.equal(r.rows[4].rt, 150);
  });

  it('ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (moving average)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');
    
    const r = db.execute(`
      SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as ma
      FROM t ORDER BY id
    `);
    // id=1: avg(10,20) = 15
    // id=2: avg(10,20,30) = 20
    // id=3: avg(20,30,40) = 30
    // id=4: avg(30,40,50) = 40
    // id=5: avg(40,50) = 45
    assert.equal(r.rows[0].ma, 15);
    assert.equal(r.rows[1].ma, 20);
    assert.equal(r.rows[2].ma, 30);
    assert.equal(r.rows[3].ma, 40);
    assert.equal(r.rows[4].ma, 45);
  });

  it('default frame (no explicit ROWS clause) acts as running total', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    
    const r = db.execute(`
      SELECT id, SUM(val) OVER (ORDER BY id) as rt
      FROM t ORDER BY id
    `);
    assert.equal(r.rows[0].rt, 10);
    assert.equal(r.rows[1].rt, 30);
    assert.equal(r.rows[2].rt, 60);
  });

  it('PARTITION BY + window frame', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, id INT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1,10),('A',2,20),('A',3,30),('B',1,100),('B',2,200)");
    
    const r = db.execute(`
      SELECT grp, id, SUM(val) OVER (PARTITION BY grp ORDER BY id) as rt
      FROM t ORDER BY grp, id
    `);
    // Group A: running sum 10, 30, 60
    // Group B: running sum 100, 300
    assert.equal(r.rows[0].rt, 10);
    assert.equal(r.rows[1].rt, 30);
    assert.equal(r.rows[2].rt, 60);
    assert.equal(r.rows[3].rt, 100);
    assert.equal(r.rows[4].rt, 300);
  });

  it('ROW_NUMBER + running SUM in same query', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (day INT, amount INT)');
    db.execute('INSERT INTO sales VALUES (1,100),(2,200),(3,150),(4,300),(5,250)');
    
    const r = db.execute(`
      SELECT day, amount,
             ROW_NUMBER() OVER (ORDER BY day) as rn,
             SUM(amount) OVER (ORDER BY day) as running_total
      FROM sales ORDER BY day
    `);
    assert.equal(r.rows[0].rn, 1);
    assert.equal(r.rows[0].running_total, 100);
    assert.equal(r.rows[4].rn, 5);
    assert.equal(r.rows[4].running_total, 1000);
  });

  it('COUNT OVER with frame', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    
    const r = db.execute(`
      SELECT id, COUNT(*) OVER (ORDER BY id) as running_count
      FROM t ORDER BY id
    `);
    assert.equal(r.rows[0].running_count, 1);
    assert.equal(r.rows[4].running_count, 5);
  });

  it('MIN/MAX OVER running frame', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20),(5,40)');
    
    const r = db.execute(`
      SELECT id, val,
             MIN(val) OVER (ORDER BY id) as running_min,
             MAX(val) OVER (ORDER BY id) as running_max
      FROM t ORDER BY id
    `);
    // running_min: 30, 10, 10, 10, 10
    // running_max: 30, 30, 50, 50, 50
    assert.equal(r.rows[0].running_min, 30);
    assert.equal(r.rows[1].running_min, 10);
    assert.equal(r.rows[4].running_min, 10);
    assert.equal(r.rows[0].running_max, 30);
    assert.equal(r.rows[2].running_max, 50);
    assert.equal(r.rows[4].running_max, 50);
  });
});
