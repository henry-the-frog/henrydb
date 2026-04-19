// nth-value.test.js — NTH_VALUE window function tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NTH_VALUE window function', () => {
  it('NTH_VALUE basic: get 2nd value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30),(40),(50)');

    const r = db.execute(`
      SELECT val, NTH_VALUE(val, 2) OVER (ORDER BY val) as nv
      FROM t
    `);
    // Default frame: UNBOUNDED PRECEDING TO CURRENT ROW
    // Row 1 (val=10): frame=[10], no 2nd value → null
    // Row 2 (val=20): frame=[10,20], 2nd value → 20
    // Row 3+: frame includes row 2, 2nd value → 20
    assert.equal(r.rows[0].nv, null);
    assert.equal(r.rows[1].nv, 20);
    assert.equal(r.rows[2].nv, 20);
    assert.equal(r.rows[3].nv, 20);
    assert.equal(r.rows[4].nv, 20);
  });

  it('NTH_VALUE(val, 1) is same as FIRST_VALUE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');

    const r = db.execute(`
      SELECT val, 
             NTH_VALUE(val, 1) OVER (ORDER BY val) as nv,
             FIRST_VALUE(val) OVER (ORDER BY val) as fv
      FROM t
    `);
    for (const row of r.rows) {
      assert.equal(row.nv, row.fv, `NTH_VALUE(1) should equal FIRST_VALUE for val=${row.val}`);
    }
  });

  it('NTH_VALUE with PARTITION BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('A',30),('B',100),('B',200)");

    const r = db.execute(`
      SELECT grp, val, NTH_VALUE(val, 2) OVER (PARTITION BY grp ORDER BY val) as nv
      FROM t
    `);
    const a = r.rows.filter(row => row.grp === 'A');
    const b = r.rows.filter(row => row.grp === 'B');
    
    // Group A: 10, 20, 30. 2nd value = 20 (null for first row)
    assert.equal(a[0].nv, null);
    assert.equal(a[1].nv, 20);
    assert.equal(a[2].nv, 20);
    
    // Group B: 100, 200. 2nd value = 200 (null for first row)
    assert.equal(b[0].nv, null);
    assert.equal(b[1].nv, 200);
  });

  it('NTH_VALUE with n > partition size returns null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');

    const r = db.execute(`
      SELECT val, NTH_VALUE(val, 10) OVER (ORDER BY val) as nv
      FROM t
    `);
    // All rows should be null since 10 > 3 rows
    for (const row of r.rows) {
      assert.equal(row.nv, null);
    }
  });

  it('NTH_VALUE without ORDER BY (entire partition is frame)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10),(20),(30)');

    const r = db.execute(`
      SELECT val, NTH_VALUE(val, 2) OVER () as nv
      FROM t
    `);
    // Without ORDER BY, entire partition is frame for all rows
    // 2nd value should be the same for all rows (insertion order dependent)
    const nvValues = r.rows.map(row => row.nv);
    assert.ok(nvValues.every(v => v !== null), 'all rows should have a value when no ORDER BY');
    // All should be the same value
    assert.ok(nvValues.every(v => v === nvValues[0]));
  });

  it('NTH_VALUE with n=3, returns 3rd row value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES ('alice',90),('bob',85),('charlie',80),('dave',75)");

    const r = db.execute(`
      SELECT name, score, NTH_VALUE(score, 3) OVER (ORDER BY score DESC) as third_score
      FROM t
    `);
    // Ordered by score DESC: alice(90), bob(85), charlie(80), dave(75)
    // Row 1: frame=[90], no 3rd → null
    // Row 2: frame=[90,85], no 3rd → null  
    // Row 3: frame=[90,85,80], 3rd → 80
    // Row 4: frame=[90,85,80,75], 3rd → 80
    assert.equal(r.rows[0].third_score, null);
    assert.equal(r.rows[1].third_score, null);
    assert.equal(r.rows[2].third_score, 80);
    assert.equal(r.rows[3].third_score, 80);
  });
});
