// window-comprehensive.test.js — Window function stress tests
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window functions (comprehensive)', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'eng', 100)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'eng', 120)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 'sales', 80)");
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 'sales', 90)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'eng', 110)");
    db.execute("INSERT INTO employees VALUES (6, 'Frank', 'hr', 95)");
    db.execute("INSERT INTO employees VALUES (7, 'Grace', 'hr', 85)");
    db.execute("INSERT INTO employees VALUES (8, 'Hank', 'sales', 75)");
  });

  describe('ROW_NUMBER', () => {
    it('ROW_NUMBER() OVER (ORDER BY salary DESC)', () => {
      const r = db.execute('SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees');
      assert.equal(r.rows.length, 8);
      assert.equal(r.rows[0].rn, 1);
      assert.equal(r.rows[0].salary, 120); // Bob
      assert.equal(r.rows[7].rn, 8);
    });

    it('ROW_NUMBER with PARTITION BY', () => {
      const r = db.execute('SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank FROM employees ORDER BY dept, dept_rank');
      // eng: Bob(120)→1, Eve(110)→2, Alice(100)→3
      const eng = r.rows.filter(row => row.dept === 'eng');
      assert.equal(eng[0].dept_rank, 1);
      assert.equal(eng[0].name, 'Bob');
      assert.equal(eng[2].dept_rank, 3);
    });
  });

  describe('RANK', () => {
    it('RANK() with ties', () => {
      db.execute('CREATE TABLE scores (name TEXT, score INT)');
      db.execute("INSERT INTO scores VALUES ('A', 100), ('B', 90), ('C', 90), ('D', 80)");
      const r = db.execute('SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores');
      // A→1, B→2, C→2, D→4 (ties get same rank, next rank skips)
      const bRow = r.rows.find(row => row.name === 'B');
      const cRow = r.rows.find(row => row.name === 'C');
      const dRow = r.rows.find(row => row.name === 'D');
      assert.equal(bRow.rank, cRow.rank); // B and C tied
      assert.equal(dRow.rank, 4); // D skips to 4
    });
  });

  describe('DENSE_RANK', () => {
    it('DENSE_RANK() without gaps', () => {
      db.execute('CREATE TABLE dr (name TEXT, val INT)');
      db.execute("INSERT INTO dr VALUES ('A', 100), ('B', 90), ('C', 90), ('D', 80)");
      const r = db.execute('SELECT name, val, DENSE_RANK() OVER (ORDER BY val DESC) AS dr FROM dr');
      const dRow = r.rows.find(row => row.name === 'D');
      assert.equal(dRow.dr, 3); // Dense rank: no gap (1, 2, 2, 3)
    });
  });

  describe('SUM OVER', () => {
    it('running total', () => {
      const r = db.execute('SELECT name, salary, SUM(salary) OVER (ORDER BY id) AS running_total FROM employees ORDER BY id');
      assert.equal(r.rows[0].running_total, 100); // Alice
      assert.equal(r.rows[1].running_total, 220); // + Bob
      assert.equal(r.rows[2].running_total, 300); // + Carol
    });

    it('SUM OVER with PARTITION BY and ORDER BY', () => {
      const r = db.execute('SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary) AS running_dept_total FROM employees ORDER BY dept, salary');
      // eng: Alice(100)→100, Eve(110)→210, Bob(120)→330
      const eng = r.rows.filter(row => row.dept === 'eng');
      assert.equal(eng[eng.length - 1].running_dept_total, 330);
    });
  });

  describe('AVG OVER', () => {
    it('running average', () => {
      const r = db.execute('SELECT name, salary, AVG(salary) OVER (ORDER BY id) AS running_avg FROM employees ORDER BY id');
      assert.equal(r.rows[0].running_avg, 100); // Alice alone
    });
  });

  describe('COUNT OVER', () => {
    it('COUNT(*) partitioned', () => {
      const r = db.execute('SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_size FROM employees ORDER BY dept, name');
      const eng = r.rows.filter(row => row.dept === 'eng');
      assert.ok(eng.every(row => row.dept_size === 3));
      const hr = r.rows.filter(row => row.dept === 'hr');
      assert.ok(hr.every(row => row.dept_size === 2));
    });
  });

  describe('Multiple window functions', () => {
    it('ROW_NUMBER + SUM + AVG in same query', () => {
      const r = db.execute(`
        SELECT name, dept, salary,
          ROW_NUMBER() OVER (ORDER BY salary DESC) AS global_rank,
          SUM(salary) OVER (PARTITION BY dept) AS dept_total
        FROM employees
        ORDER BY salary DESC
      `);
      assert.equal(r.rows.length, 8);
      assert.equal(r.rows[0].global_rank, 1);
      assert.ok(r.rows[0].dept_total > 0);
    });
  });

  describe('Window with WHERE', () => {
    it('window function after WHERE filter', () => {
      const r = db.execute(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
        FROM employees
        WHERE dept = 'eng'
      `);
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0].rn, 1);
    });
  });
});
