// sliding-window.test.js — Tests for sliding window frame (ROWS N PRECEDING/FOLLOWING)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Sliding window frame', () => {
  
  it('3-row moving average (1 PRECEDING to 1 FOLLOWING)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute(`
      SELECT id, val,
        AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg
      FROM t ORDER BY id
    `);
    
    // Row 1: avg(10) = 10 (no preceding)
    // Row 2: avg(10, 20, 30) / 3 = 20
    // Row 3: avg(20, 30, 40) / 3 = 30
    // Row 4: avg(30, 40, 50) / 3 = 40
    // Row 5: avg(40, 50) / 2 = 45 (no following)
    assert.ok(Math.abs(r.rows[0].moving_avg - 15) < 0.01); // (10+20)/2
    assert.ok(Math.abs(r.rows[1].moving_avg - 20) < 0.01);
    assert.ok(Math.abs(r.rows[2].moving_avg - 30) < 0.01);
    assert.ok(Math.abs(r.rows[3].moving_avg - 40) < 0.01);
    assert.ok(Math.abs(r.rows[4].moving_avg - 45) < 0.01);
  });

  it('SUM with 2 PRECEDING to CURRENT ROW', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute(`
      SELECT id, val,
        SUM(val) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_sum
      FROM t ORDER BY id
    `);
    
    // Row 1: sum(1) = 1
    // Row 2: sum(1, 2) = 3
    // Row 3: sum(1, 2, 3) = 6
    // Row 4: sum(2, 3, 4) = 9
    // Row 5: sum(3, 4, 5) = 12
    assert.strictEqual(r.rows[0].rolling_sum, 1);
    assert.strictEqual(r.rows[1].rolling_sum, 3);
    assert.strictEqual(r.rows[2].rolling_sum, 6);
    assert.strictEqual(r.rows[3].rolling_sum, 9);
    assert.strictEqual(r.rows[4].rolling_sum, 12);
  });

  it('MIN/MAX with sliding window', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    const vals = [5, 3, 8, 1, 9, 2, 7];
    for (let i = 0; i < vals.length; i++) db.execute(`INSERT INTO t VALUES (${i}, ${vals[i]})`);
    
    const r = db.execute(`
      SELECT id, val,
        MIN(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as win_min,
        MAX(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as win_max
      FROM t ORDER BY id
    `);
    
    // Row 0 (5): window [5, 3] → min=3, max=5
    // Row 1 (3): window [5, 3, 8] → min=3, max=8
    // Row 2 (8): window [3, 8, 1] → min=1, max=8
    // Row 3 (1): window [8, 1, 9] → min=1, max=9
    // Row 4 (9): window [1, 9, 2] → min=1, max=9
    // Row 5 (2): window [9, 2, 7] → min=2, max=9
    // Row 6 (7): window [2, 7] → min=2, max=7
    assert.strictEqual(r.rows[0].win_min, 3);
    assert.strictEqual(r.rows[0].win_max, 5);
    assert.strictEqual(r.rows[1].win_min, 3);
    assert.strictEqual(r.rows[1].win_max, 8);
    assert.strictEqual(r.rows[3].win_min, 1);
    assert.strictEqual(r.rows[3].win_max, 9);
    assert.strictEqual(r.rows[6].win_min, 2);
    assert.strictEqual(r.rows[6].win_max, 7);
  });

  it('COUNT with sliding window', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute(`
      SELECT id,
        COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as cnt
      FROM t ORDER BY id
    `);
    
    // Row 1: 2 (self + next)
    // Row 2-4: 3 (prev + self + next)
    // Row 5: 2 (prev + self)
    assert.strictEqual(r.rows[0].cnt, 2);
    assert.strictEqual(r.rows[1].cnt, 3);
    assert.strictEqual(r.rows[2].cnt, 3);
    assert.strictEqual(r.rows[3].cnt, 3);
    assert.strictEqual(r.rows[4].cnt, 2);
  });

  it('UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING (entire partition)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 3; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute(`
      SELECT id, val,
        SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total
      FROM t ORDER BY id
    `);
    
    // All rows should see the total: 10 + 20 + 30 = 60
    for (const row of r.rows) {
      assert.strictEqual(row.total, 60);
    }
  });

  it('CURRENT ROW to UNBOUNDED FOLLOWING (suffix sum)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 4; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute(`
      SELECT id, val,
        SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as suffix_sum
      FROM t ORDER BY id
    `);
    
    // Row 1: 1+2+3+4 = 10
    // Row 2: 2+3+4 = 9
    // Row 3: 3+4 = 7
    // Row 4: 4
    assert.strictEqual(r.rows[0].suffix_sum, 10);
    assert.strictEqual(r.rows[1].suffix_sum, 9);
    assert.strictEqual(r.rows[2].suffix_sum, 7);
    assert.strictEqual(r.rows[3].suffix_sum, 4);
  });

  it('sliding window with PARTITION BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, id INT, val INT)');
    db.execute("INSERT INTO t VALUES ('A', 1, 10)");
    db.execute("INSERT INTO t VALUES ('A', 2, 20)");
    db.execute("INSERT INTO t VALUES ('A', 3, 30)");
    db.execute("INSERT INTO t VALUES ('B', 1, 100)");
    db.execute("INSERT INTO t VALUES ('B', 2, 200)");
    
    const r = db.execute(`
      SELECT cat, id, val,
        SUM(val) OVER (PARTITION BY cat ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as rolling
      FROM t ORDER BY cat, id
    `);
    
    // A: row 1=10, row 2=10+20=30, row 3=20+30=50
    // B: row 1=100, row 2=100+200=300
    assert.strictEqual(r.rows[0].rolling, 10);
    assert.strictEqual(r.rows[1].rolling, 30);
    assert.strictEqual(r.rows[2].rolling, 50);
    assert.strictEqual(r.rows[3].rolling, 100);
    assert.strictEqual(r.rows[4].rolling, 300);
  });

  it('large sliding window (100 rows, 10 preceding)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 1)`);
    
    const r = db.execute(`
      SELECT id,
        SUM(val) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling
      FROM t ORDER BY id
    `);
    
    // First 10 rows: rolling sum grows 1, 2, ..., 10
    // After that: always 11
    for (let i = 0; i < 10; i++) {
      assert.strictEqual(r.rows[i].rolling, i + 1);
    }
    for (let i = 10; i < 100; i++) {
      assert.strictEqual(r.rows[i].rolling, 11);
    }
  });
});
