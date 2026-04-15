import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('FIRST_VALUE / LAST_VALUE with frame specs', () => {
  it('LAST_VALUE with UNBOUNDED FOLLOWING frame returns last value in full partition', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val,
             LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.lv), [50, 50, 50, 50, 50]);
  });

  it('LAST_VALUE with default frame (ORDER BY present) returns current row value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');

    const r = db.execute(`
      SELECT val, LAST_VALUE(val) OVER (ORDER BY val) as lv FROM t
    `);
    // Default frame: RANGE UNBOUNDED PRECEDING TO CURRENT ROW → last value = current row
    assert.deepEqual(r.rows.map(r => r.lv), [10, 20, 30]);
  });

  it('LAST_VALUE with ROWS BETWEEN N PRECEDING AND N FOLLOWING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val,
             LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as lv
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.lv), [20, 30, 40, 50, 50]);
  });

  it('FIRST_VALUE respects frame spec', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val,
             FIRST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as fv
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.fv), [10, 10, 20, 30, 40]);
  });

  it('LAST_VALUE without ORDER BY returns partition last', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',30),('B',40)");

    const r = db.execute(`
      SELECT grp, val, LAST_VALUE(val) OVER (PARTITION BY grp) as lv FROM t
    `);
    const a = r.rows.filter(r => r.grp === 'A');
    const b = r.rows.filter(r => r.grp === 'B');
    // Without ORDER BY, entire partition is the frame
    assert.ok(a.every(r => r.lv === 20), 'All A rows should see 20 as last value');
    assert.ok(b.every(r => r.lv === 40), 'All B rows should see 40 as last value');
  });
});
