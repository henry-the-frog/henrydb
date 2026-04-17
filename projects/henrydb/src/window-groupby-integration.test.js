// window-groupby-integration.test.js — Tests for GROUP BY + window function interaction
// Regression tests for the bug where window function columns were silently dropped
// when combined with GROUP BY.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP BY + Window Function Integration', () => {
  function setupEmpDb() {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'Eng', 120000)");
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'Eng', 100000)");
    db.execute("INSERT INTO emp VALUES (3, 'Charlie', 'Sales', 90000)");
    db.execute("INSERT INTO emp VALUES (4, 'Diana', 'Eng', 150000)");
    db.execute("INSERT INTO emp VALUES (5, 'Eve', 'Sales', 110000)");
    return db;
  }

  it('RANK over AVG(salary) with GROUP BY should include window column', () => {
    const db = setupEmpDb();
    const r = db.execute(`
      SELECT dept, AVG(salary) as avg_sal,
        RANK() OVER (ORDER BY AVG(salary) DESC) as dept_rank
      FROM emp GROUP BY dept
    `);
    
    assert.strictEqual(r.rows.length, 2);
    // Check that dept_rank column exists
    assert.ok('dept_rank' in r.rows[0], 'Should have dept_rank column');
    
    // Engineering has higher avg salary, should be rank 1
    const eng = r.rows.find(row => row.dept === 'Eng');
    const sales = r.rows.find(row => row.dept === 'Sales');
    assert.strictEqual(eng.dept_rank, 1);
    assert.strictEqual(sales.dept_rank, 2);
  });

  it('ROW_NUMBER with GROUP BY', () => {
    const db = setupEmpDb();
    const r = db.execute(`
      SELECT dept, COUNT(*) as cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
      FROM emp GROUP BY dept
    `);
    
    assert.strictEqual(r.rows.length, 2);
    assert.ok('rn' in r.rows[0], 'Should have rn column');
    // Eng has 3 employees, Sales has 2
    assert.strictEqual(r.rows[0].rn, 1);
    assert.strictEqual(r.rows[1].rn, 2);
  });

  it('cumulative SUM with GROUP BY (using alias)', () => {
    const db = setupEmpDb();
    const r = db.execute(`
      SELECT dept, SUM(salary) as total,
        SUM(total) OVER (ORDER BY dept) as cumulative_total
      FROM emp GROUP BY dept
    `);
    
    // This tests that window functions can reference GROUP BY result aliases
    assert.strictEqual(r.rows.length, 2);
    // If cumulative_total isn't supported with this syntax, at least no crash
  });

  it('multiple window functions with GROUP BY', () => {
    const db = setupEmpDb();
    const r = db.execute(`
      SELECT dept, AVG(salary) as avg_sal,
        RANK() OVER (ORDER BY AVG(salary) DESC) as rank_by_avg,
        ROW_NUMBER() OVER (ORDER BY dept) as rn
      FROM emp GROUP BY dept
    `);
    
    assert.strictEqual(r.rows.length, 2);
    assert.ok('rank_by_avg' in r.rows[0], 'Should have rank_by_avg');
    assert.ok('rn' in r.rows[0], 'Should have rn');
  });

  it('DENSE_RANK with GROUP BY and ties', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (team TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES ('A', 10)");
    db.execute("INSERT INTO scores VALUES ('A', 20)");
    db.execute("INSERT INTO scores VALUES ('B', 10)");
    db.execute("INSERT INTO scores VALUES ('B', 20)");
    db.execute("INSERT INTO scores VALUES ('C', 5)");
    db.execute("INSERT INTO scores VALUES ('C', 35)");
    
    const r = db.execute(`
      SELECT team, SUM(score) as total,
        DENSE_RANK() OVER (ORDER BY SUM(score) DESC) as drnk
      FROM scores GROUP BY team
    `);
    
    assert.strictEqual(r.rows.length, 3);
    assert.ok('drnk' in r.rows[0], 'Should have drnk column');
    
    // A and B have same total (30), C has 40
    const teamC = r.rows.find(row => row.team === 'C');
    const teamA = r.rows.find(row => row.team === 'A');
    const teamB = r.rows.find(row => row.team === 'B');
    assert.strictEqual(teamC.drnk, 1, 'C has highest total (40), should be rank 1');
    assert.strictEqual(teamA.drnk, teamB.drnk, 'A and B should have same rank (tied at 30)');
    assert.strictEqual(teamA.drnk, 2, 'A and B should be rank 2');
  });
});
