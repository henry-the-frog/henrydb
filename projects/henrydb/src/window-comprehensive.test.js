// window-functions-comprehensive.test.js

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window Functions — Ranking', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, name TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'Sales', 'Alice', 50000)");
    db.execute("INSERT INTO emp VALUES (2, 'Sales', 'Bob', 60000)");
    db.execute("INSERT INTO emp VALUES (3, 'Sales', 'Carol', 60000)"); // tie with Bob
    db.execute("INSERT INTO emp VALUES (4, 'Eng', 'Dan', 80000)");
    db.execute("INSERT INTO emp VALUES (5, 'Eng', 'Eve', 90000)");
    db.execute("INSERT INTO emp VALUES (6, 'Eng', 'Frank', 70000)");
  });

  it('ROW_NUMBER partitioned by dept', () => {
    const r = db.execute('SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM emp ORDER BY dept, rn');
    // Eng: Eve(1), Dan(2), Frank(3)
    // Sales: Bob(1 or 2), Carol(1 or 2), Alice(3)
    const eng = r.rows.filter(r => r.dept === 'Eng');
    assert.equal(eng[0].rn, 1);
    assert.equal(eng[0].salary, 90000);
    assert.equal(eng[2].rn, 3);
  });

  it('RANK with ties', () => {
    const r = db.execute('SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rnk FROM emp ORDER BY rnk, name');
    assert.equal(r.rows[0].rnk, 1); // Eve 90000
    assert.equal(r.rows[1].rnk, 2); // Dan 80000
    // Bob and Carol at 60000 should both be rank 4 (tied)
    const tied = r.rows.filter(r => r.salary === 60000);
    assert.equal(tied.length, 2);
    assert.ok(tied.every(r => r.rnk === tied[0].rnk), 'Tied salaries have same rank');
  });

  it('DENSE_RANK — no gaps', () => {
    const r = db.execute('SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) as drnk FROM emp ORDER BY drnk');
    // 90000→1, 80000→2, 70000→3, 60000→4 (both Bob and Carol), 50000→5
    const ranks = [...new Set(r.rows.map(r => r.drnk))];
    assert.deepStrictEqual(ranks, [1, 2, 3, 4, 5], 'No gaps in dense rank');
  });

  it('NTILE distributes rows evenly', () => {
    const r = db.execute('SELECT name, NTILE(3) OVER (ORDER BY salary) as bucket FROM emp ORDER BY bucket, salary');
    // 6 rows into 3 buckets: 2 per bucket
    const counts = [0, 0, 0];
    for (const row of r.rows) counts[row.bucket - 1]++;
    assert.deepStrictEqual(counts, [2, 2, 2]);
  });
});

describe('Window Functions — Value', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE ts (id INT PRIMARY KEY, val INT, grp TEXT)');
    db.execute("INSERT INTO ts VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'A'), (4, 40, 'B'), (5, 50, 'B')");
  });

  it('LAG gets previous row value', () => {
    const r = db.execute('SELECT id, val, LAG(val, 1) OVER (ORDER BY id) as prev_val FROM ts ORDER BY id');
    assert.equal(r.rows[0].prev_val, null); // First row has no previous
    assert.equal(r.rows[1].prev_val, 10);
    assert.equal(r.rows[4].prev_val, 40);
  });

  it('LEAD gets next row value', () => {
    const r = db.execute('SELECT id, val, LEAD(val, 1) OVER (ORDER BY id) as next_val FROM ts ORDER BY id');
    assert.equal(r.rows[0].next_val, 20);
    assert.equal(r.rows[4].next_val, null); // Last row has no next
  });

  it('LAG partitioned', () => {
    const r = db.execute('SELECT grp, id, val, LAG(val, 1) OVER (PARTITION BY grp ORDER BY id) as prev FROM ts ORDER BY grp, id');
    const a = r.rows.filter(r => r.grp === 'A');
    assert.equal(a[0].prev, null); // First in partition
    assert.equal(a[1].prev, 10);
    assert.equal(a[2].prev, 20);
  });

  it('FIRST_VALUE', () => {
    const r = db.execute('SELECT grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY val) as first_val FROM ts ORDER BY grp, val');
    const a = r.rows.filter(r => r.grp === 'A');
    assert.ok(a.every(r => r.first_val === 10));
  });
});

describe('Window Functions — Aggregate', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
  });

  it('SUM running total', () => {
    const r = db.execute('SELECT id, val, SUM(val) OVER (ORDER BY id) as running FROM t ORDER BY id');
    assert.equal(r.rows[0].running, 10);
    assert.equal(r.rows[1].running, 30);
    assert.equal(r.rows[4].running, 150);
  });

  it('AVG moving average (3 rows)', () => {
    const r = db.execute('SELECT id, val, AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as ma FROM t ORDER BY id');
    assert.equal(r.rows[0].ma, 15);    // avg(10, 20)
    assert.equal(r.rows[1].ma, 20);    // avg(10, 20, 30)
    assert.equal(r.rows[2].ma, 30);    // avg(20, 30, 40)
    assert.equal(r.rows[4].ma, 45);    // avg(40, 50)
  });

  it('COUNT running count', () => {
    const r = db.execute('SELECT id, COUNT(*) OVER (ORDER BY id) as cnt FROM t ORDER BY id');
    assert.equal(r.rows[0].cnt, 1);
    assert.equal(r.rows[4].cnt, 5);
  });

  it('SUM over entire partition (no ORDER BY)', () => {
    const r = db.execute('SELECT id, val, SUM(val) OVER () as total FROM t');
    assert.ok(r.rows.every(row => row.total === 150), 'All rows see total 150');
  });

  it('multiple window functions', () => {
    const r = db.execute(`
      SELECT id, val,
        SUM(val) OVER (ORDER BY id) as running,
        AVG(val) OVER () as overall_avg,
        ROW_NUMBER() OVER (ORDER BY val DESC) as rank_desc
      FROM t ORDER BY id
    `);
    assert.equal(r.rows[0].running, 10);
    assert.equal(r.rows[0].overall_avg, 30);
    assert.equal(r.rows[4].rank_desc, 1); // id=5, val=50 is highest
  });
});

describe('Window Functions — Statistics', () => {
  it('PERCENT_RANK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT id, PERCENT_RANK() OVER (ORDER BY val) as pct FROM t ORDER BY val');
    assert.equal(r.rows[0].pct, 0); // First: 0
    assert.equal(r.rows[4].pct, 1); // Last: 1
  });

  it('CUME_DIST', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT id, CUME_DIST() OVER (ORDER BY val) as cd FROM t ORDER BY val');
    assert.equal(r.rows[0].cd, 0.2);
    assert.equal(r.rows[4].cd, 1);
  });
});
