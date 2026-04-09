// window-functions.test.js — Comprehensive tests for window functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window functions', () => {
  function setupSales() {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, dept TEXT, emp TEXT, amount INTEGER)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 'Alice', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'A', 'Bob', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'B', 'Charlie', 150)");
    db.execute("INSERT INTO sales VALUES (4, 'B', 'Dave', 300)");
    db.execute("INSERT INTO sales VALUES (5, 'A', 'Eve', 50)");
    db.execute("INSERT INTO sales VALUES (6, 'B', 'Frank', 150)"); // Tie with Charlie
    return db;
  }

  it('ROW_NUMBER with PARTITION BY and ORDER BY', () => {
    const db = setupSales();
    const r = db.execute('SELECT emp, dept, amount, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) as rn FROM sales');
    
    // Dept A: Bob(200)=1, Alice(100)=2, Eve(50)=3
    const deptA = r.rows.filter(r => r.dept === 'A').sort((a, b) => a.rn - b.rn);
    assert.equal(deptA[0].emp, 'Bob');
    assert.equal(deptA[0].rn, 1);
    assert.equal(deptA[2].emp, 'Eve');
    assert.equal(deptA[2].rn, 3);
  });

  it('RANK with ties', () => {
    const db = setupSales();
    const r = db.execute('SELECT emp, dept, amount, RANK() OVER (PARTITION BY dept ORDER BY amount DESC) as rnk FROM sales');
    
    // Dept B: Dave(300)=1, Charlie(150)=2, Frank(150)=2 (tied)
    const deptB = r.rows.filter(r => r.dept === 'B');
    assert.equal(deptB.length, 3);
    const dave = deptB.find(r => r.emp === 'Dave');
    assert.equal(dave.rnk, 1);
    // Charlie and Frank should have same rank (tied)
    const ties = deptB.filter(r => r.amount === 150);
    assert.equal(ties.length, 2);
    assert.equal(ties[0].rnk, ties[1].rnk);
  });

  it('DENSE_RANK with ties', () => {
    const db = setupSales();
    const r = db.execute('SELECT emp, dept, amount, DENSE_RANK() OVER (PARTITION BY dept ORDER BY amount DESC) as drnk FROM sales');
    
    // Dept B: Dave(300)=1, Charlie(150)=2, Frank(150)=2
    const deptB = r.rows.filter(r => r.dept === 'B').sort((a, b) => a.amount - b.amount);
    const ranks = deptB.map(r => r.drnk);
    assert.ok(ranks.includes(1)); // Dave
    assert.ok(ranks.includes(2)); // Charlie and Frank
    // No rank 3 — dense_rank skips nothing
  });

  it('LAG: access previous row', () => {
    const db = new Database();
    db.execute('CREATE TABLE ts (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO ts VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('SELECT id, val, LAG(val) OVER (ORDER BY id) as prev FROM ts');
    assert.equal(r.rows[0].prev, null); // No previous for first row
    assert.equal(r.rows[1].prev, 10);
    assert.equal(r.rows[2].prev, 20);
    assert.equal(r.rows[4].prev, 40);
  });

  it('LEAD: access next row', () => {
    const db = new Database();
    db.execute('CREATE TABLE ts (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO ts VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('SELECT id, val, LEAD(val) OVER (ORDER BY id) as nxt FROM ts');
    assert.equal(r.rows[0].nxt, 20);
    assert.equal(r.rows[3].nxt, 50);
    assert.equal(r.rows[4].nxt, null); // No next for last row
  });

  it('LAG with offset > 1', () => {
    const db = new Database();
    db.execute('CREATE TABLE ts (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO ts VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('SELECT id, val, LAG(val, 2) OVER (ORDER BY id) as prev2 FROM ts');
    assert.equal(r.rows[0].prev2, null);
    assert.equal(r.rows[1].prev2, null);
    assert.equal(r.rows[2].prev2, 10);
    assert.equal(r.rows[4].prev2, 30);
  });

  it('FIRST_VALUE', () => {
    const db = setupSales();
    const r = db.execute('SELECT emp, dept, amount, FIRST_VALUE(emp) OVER (PARTITION BY dept ORDER BY amount DESC) as top_emp FROM sales');
    
    const deptA = r.rows.filter(r => r.dept === 'A');
    for (const row of deptA) {
      assert.equal(row.top_emp, 'Bob'); // Highest amount in dept A
    }
  });

  it('running SUM window aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO nums VALUES (${i}, ${i})`);
    
    const r = db.execute('SELECT id, val, SUM(val) OVER (ORDER BY id) as running_sum FROM nums');
    assert.equal(r.rows[0].running_sum, 1);
    assert.equal(r.rows[1].running_sum, 3);   // 1+2
    assert.equal(r.rows[2].running_sum, 6);   // 1+2+3
    assert.equal(r.rows[3].running_sum, 10);  // 1+2+3+4
    assert.equal(r.rows[4].running_sum, 15);  // 1+2+3+4+5
  });

  it('running AVG window aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 4; i++) db.execute(`INSERT INTO nums VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('SELECT id, val, AVG(val) OVER (ORDER BY id) as running_avg FROM nums');
    assert.equal(r.rows[0].running_avg, 10);     // 10/1
    assert.equal(r.rows[1].running_avg, 15);     // 30/2
    assert.equal(r.rows[2].running_avg, 20);     // 60/3
    assert.equal(r.rows[3].running_avg, 25);     // 100/4
  });

  it('window function without ORDER BY (whole partition)', () => {
    const db = setupSales();
    const r = db.execute('SELECT emp, dept, SUM(amount) OVER (PARTITION BY dept) as dept_total FROM sales');
    
    const deptA = r.rows.filter(r => r.dept === 'A');
    const deptB = r.rows.filter(r => r.dept === 'B');
    
    // All rows in dept A should have same total
    for (const row of deptA) assert.equal(row.dept_total, 350);
    // All rows in dept B should have same total
    for (const row of deptB) assert.equal(row.dept_total, 600);
  });

  it('multiple window functions in same query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute(`
      SELECT id, val,
        ROW_NUMBER() OVER (ORDER BY val DESC) as rn,
        LAG(val) OVER (ORDER BY id) as prev,
        LEAD(val) OVER (ORDER BY id) as nxt
      FROM t
    `);
    
    assert.equal(r.rows.length, 5);
    // Each row should have all three window columns
    for (const row of r.rows) {
      assert.ok('rn' in row);
      assert.ok('prev' in row);
      assert.ok('nxt' in row);
    }
  });

  it('window function with WHERE clause', () => {
    const db = setupSales();
    const r = db.execute("SELECT emp, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM sales WHERE dept = 'A'");
    
    assert.equal(r.rows.length, 3); // Only dept A
    // ROW_NUMBER over the filtered result set
    const rns = r.rows.map(r => r.rn).sort((a, b) => a - b);
    assert.deepEqual(rns, [1, 2, 3]);
  });

  it('window function on BTreeTable', () => {
    const db = new Database();
    db.execute('CREATE TABLE ordered (id INTEGER PRIMARY KEY, score INTEGER) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO ordered VALUES (${i}, ${i * 5})`);
    
    const r = db.execute('SELECT id, score, RANK() OVER (ORDER BY score DESC) as rnk FROM ordered');
    const top = r.rows.find(row => row.score === 50);
    assert.ok(top);
    assert.equal(top.rnk, 1); // Highest score should have rank 1
  });

  it('stress: 1000 rows with window functions', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${i % 10}, ${Math.floor(Math.random() * 1000)})`);
    }
    
    const t0 = performance.now();
    const r = db.execute('SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) as rn FROM big');
    const elapsed = performance.now() - t0;
    
    assert.equal(r.rows.length, 1000);
    // Each group should have rn 1..100
    const grp0 = r.rows.filter(r => r.grp === 0);
    assert.equal(grp0.length, 100);
    assert.equal(grp0.some(r => r.rn === 1), true);
    assert.equal(grp0.some(r => r.rn === 100), true);
    
    console.log(`  1000 rows with ROW_NUMBER: ${elapsed.toFixed(1)}ms`);
  });
});
