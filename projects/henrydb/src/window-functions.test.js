import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setup() {
  const db = new Database();
  db.execute('CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT)');
  db.execute(`INSERT INTO emp VALUES
    (1, 'Alice', 'Eng', 100),
    (2, 'Bob', 'Eng', 120),
    (3, 'Carol', 'Eng', 90),
    (4, 'Dave', 'Sales', 80),
    (5, 'Eve', 'Sales', 110),
    (6, 'Frank', 'HR', 95),
    (7, 'Grace', 'HR', 105)`);
  return db;
}

describe('Window Functions: ROW_NUMBER', () => {
  it('ROW_NUMBER over all rows', () => {
    const db = setup();
    const r = db.execute('SELECT name, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM emp');
    assert.equal(r.rows.length, 7);
    assert.deepEqual(r.rows.map(r => r.rn), [1, 2, 3, 4, 5, 6, 7]);
  });

  it('ROW_NUMBER with PARTITION BY', () => {
    const db = setup();
    const r = db.execute('SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn FROM emp');
    // Within each department, row numbers restart from 1
    const eng = r.rows.filter(r => r.dept === 'Eng').map(r => r.rn);
    assert.deepEqual(eng, [1, 2, 3]);
    const sales = r.rows.filter(r => r.dept === 'Sales').map(r => r.rn);
    assert.deepEqual(sales, [1, 2]);
  });
});

describe('Window Functions: RANK and DENSE_RANK', () => {
  it('RANK with ties', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (name TEXT, score INT)');
    db.execute(`INSERT INTO scores VALUES
      ('A', 100), ('B', 90), ('C', 90), ('D', 80)`);
    const r = db.execute('SELECT name, RANK() OVER (ORDER BY score DESC) as rnk FROM scores');
    assert.deepEqual(r.rows.map(r => r.rnk), [1, 2, 2, 4]);
  });

  it('DENSE_RANK with ties', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (name TEXT, score INT)');
    db.execute(`INSERT INTO scores VALUES
      ('A', 100), ('B', 90), ('C', 90), ('D', 80)`);
    const r = db.execute('SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores');
    assert.deepEqual(r.rows.map(r => r.drnk), [1, 2, 2, 3]);
  });
});

describe('Window Functions: Aggregate Functions', () => {
  it('SUM OVER', () => {
    const db = setup();
    const r = db.execute('SELECT dept, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM emp');
    const eng = r.rows.find(r => r.dept === 'Eng');
    assert.equal(eng.dept_total, 310); // 100 + 120 + 90
  });

  it('AVG OVER', () => {
    const db = setup();
    const r = db.execute('SELECT dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM emp');
    const hr = r.rows.find(r => r.dept === 'HR');
    assert.equal(hr.dept_avg, 100); // (95 + 105) / 2
  });

  it('COUNT OVER', () => {
    const db = setup();
    const r = db.execute('SELECT dept, COUNT(*) OVER (PARTITION BY dept) as dept_count FROM emp');
    const eng = r.rows.find(r => r.dept === 'Eng');
    assert.equal(eng.dept_count, 3);
  });

  it('MIN/MAX OVER', () => {
    const db = setup();
    const r = db.execute(`SELECT dept,
      MIN(salary) OVER (PARTITION BY dept) as min_sal,
      MAX(salary) OVER (PARTITION BY dept) as max_sal
    FROM emp`);
    const eng = r.rows.find(r => r.dept === 'Eng');
    assert.equal(eng.min_sal, 90);
    assert.equal(eng.max_sal, 120);
  });
});

describe('Window Functions: Running Totals', () => {
  it('running sum', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)');
    const r = db.execute(`
      SELECT id, val, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.running_sum), [10, 30, 60, 100, 150]);
  });

  it('running average', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute(`
      SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_avg
      FROM t
    `);
    assert.deepEqual(r.rows.map(r => r.running_avg), [10, 15, 20]);
  });
});

describe('Window Functions: LAG/LEAD', () => {
  it('LAG', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT id, val, LAG(val) OVER (ORDER BY id) as prev_val FROM t');
    assert.equal(r.rows[0].prev_val, null);
    assert.equal(r.rows[1].prev_val, 10);
    assert.equal(r.rows[2].prev_val, 20);
  });

  it('LEAD', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r = db.execute('SELECT id, val, LEAD(val) OVER (ORDER BY id) as next_val FROM t');
    assert.equal(r.rows[0].next_val, 20);
    assert.equal(r.rows[1].next_val, 30);
    assert.equal(r.rows[2].next_val, null);
  });
});

describe('Window Functions: FIRST_VALUE/LAST_VALUE', () => {
  it('FIRST_VALUE', () => {
    const db = setup();
    const r = db.execute(`
      SELECT name, dept, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) as lowest_paid
      FROM emp
    `);
    const eng = r.rows.filter(r => r.dept === 'Eng');
    assert(eng.every(r => r.lowest_paid === 'Carol')); // Carol has lowest Eng salary
  });
});

describe('Window Functions: NTILE', () => {
  it('NTILE(2) splits into halves', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4)');
    const r = db.execute('SELECT id, NTILE(2) OVER (ORDER BY id) as tile FROM t');
    assert.deepEqual(r.rows.map(r => r.tile), [1, 1, 2, 2]);
  });

  it('NTILE(3) with uneven split', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    const r = db.execute('SELECT id, NTILE(3) OVER (ORDER BY id) as tile FROM t');
    assert.deepEqual(r.rows.map(r => r.tile), [1, 1, 2, 2, 3]);
  });
});
