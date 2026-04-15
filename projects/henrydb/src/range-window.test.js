import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RANGE BETWEEN window frame', () => {
  it('correctly computes SUM with RANGE BETWEEN N PRECEDING AND N FOLLOWING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');

    const r = db.execute(`
      SELECT val, SUM(val) OVER (ORDER BY val RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) as range_sum
      FROM t
    `);
    // val=10: range [0,20] → 10+20 = 30
    // val=20: range [10,30] → 10+20+30 = 60
    // val=30: range [20,40] → 20+30+40 = 90
    // val=40: range [30,50] → 30+40+50 = 120
    // val=50: range [40,60] → 40+50 = 90
    assert.deepEqual(r.rows.map(r => r.range_sum), [30, 60, 90, 120, 90]);
  });

  it('RANGE with PARTITION BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('A',30),('B',10),('B',20),('B',30)");

    const r = db.execute(`
      SELECT grp, val, SUM(val) OVER (PARTITION BY grp ORDER BY val RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) as rs
      FROM t
      ORDER BY grp, val
    `);
    // A-10: [0,20] → 10+20 = 30
    // A-20: [10,30] → 10+20+30 = 60
    // A-30: [20,40] → 20+30 = 50
    assert.deepEqual(r.rows.filter(r => r.grp === 'A').map(r => r.rs), [30, 60, 50]);
  });

  it('ROWS BETWEEN still works correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as rows_sum
      FROM t
    `);
    // row 0: [10,20] = 30
    // row 1: [10,20,30] = 60
    // row 2: [20,30,40] = 90
    // row 3: [30,40,50] = 120
    // row 4: [40,50] = 90
    assert.deepEqual(r.rows.map(r => r.rows_sum), [30, 60, 90, 120, 90]);
  });

  it('RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (cumulative)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');

    const r = db.execute(`
      SELECT val, SUM(val) OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.cum), [10, 30, 60]);
  });

  it('RANGE with peer groups (duplicate values)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(10),(20),(20),(30)');

    const r = db.execute(`
      SELECT val, SUM(val) OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum
      FROM t
    `);
    // Peers: both 10s should see same cumulative sum, both 20s same
    const vals = r.rows.map(r => ({ val: r.val, cum: r.cum }));
    const tens = vals.filter(v => v.val === 10);
    assert.equal(tens[0].cum, tens[1].cum, 'Peer rows should have same value');
    assert.equal(tens[0].cum, 20, 'Both 10s in range');
  });

  it('AVG with RANGE BETWEEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val, AVG(val) OVER (ORDER BY val RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) as ravg
      FROM t
    `);
    // val=10: avg(10,20) = 15
    // val=20: avg(10,20,30) = 20
    // val=30: avg(20,30,40) = 30
    // val=40: avg(30,40,50) = 40
    // val=50: avg(40,50) = 45
    assert.deepEqual(r.rows.map(r => r.ravg), [15, 20, 30, 40, 45]);
  });
});
