// window.test.js — Window function tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window Functions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, dept TEXT, emp TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'Eng', 'Alice', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'Eng', 'Bob', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'Eng', 'Carol', 200)");
    db.execute("INSERT INTO sales VALUES (4, 'Sales', 'Dave', 300)");
    db.execute("INSERT INTO sales VALUES (5, 'Sales', 'Eve', 150)");
  });

  describe('ROW_NUMBER', () => {
    it('assigns sequential numbers within partition', () => {
      const r = db.execute('SELECT emp, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) AS rn FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng').sort((a, b) => a.rn - b.rn);
      assert.deepEqual(eng.map(r => r.rn), [1, 2, 3]);
    });

    it('works without PARTITION BY', () => {
      const r = db.execute('SELECT emp, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM sales');
      const sorted = [...r.rows].sort((a, b) => a.rn - b.rn);
      assert.equal(sorted[0].rn, 1); // Highest amount gets 1
      assert.equal(sorted[sorted.length - 1].rn, 5);
    });
  });

  describe('RANK', () => {
    it('assigns same rank for ties', () => {
      const r = db.execute('SELECT emp, dept, amount, RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS rnk FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng').sort((a, b) => b.amount - a.amount || a.rnk - b.rnk);
      // Bob and Carol both have 200, Alice has 100
      assert.equal(eng[0].rnk, 1); // 200
      assert.equal(eng[1].rnk, 1); // 200 (tie)
      assert.equal(eng[2].rnk, 3); // 100 (skips 2)
    });
  });

  describe('DENSE_RANK', () => {
    it('no gaps in ranking', () => {
      const r = db.execute('SELECT emp, dept, amount, DENSE_RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS drnk FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng').sort((a, b) => b.amount - a.amount || a.drnk - b.drnk);
      assert.equal(eng[0].drnk, 1); // 200
      assert.equal(eng[1].drnk, 1); // 200 (tie)
      assert.equal(eng[2].drnk, 2); // 100 (no gap!)
    });
  });

  describe('SUM OVER', () => {
    it('computes partition total without ORDER BY', () => {
      const r = db.execute('SELECT emp, dept, SUM(amount) OVER (PARTITION BY dept) AS dept_total FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng');
      assert.ok(eng.every(r => r.dept_total === 500)); // 100 + 200 + 200
      const sales = r.rows.filter(r => r.dept === 'Sales');
      assert.ok(sales.every(r => r.dept_total === 450)); // 300 + 150
    });

    it('computes running sum with ORDER BY', () => {
      const r = db.execute('SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running FROM sales');
      assert.equal(r.rows[0].running, 100);
      assert.equal(r.rows[1].running, 300); // 100 + 200
      assert.equal(r.rows[2].running, 500); // 100 + 200 + 200
      assert.equal(r.rows[3].running, 800); // + 300
      assert.equal(r.rows[4].running, 950); // + 150
    });

    it('running sum within partition', () => {
      const r = db.execute('SELECT emp, dept, amount, SUM(amount) OVER (PARTITION BY dept ORDER BY amount) AS running FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng').sort((a, b) => a.amount - b.amount);
      assert.equal(eng[0].running, 100);   // Alice: 100
      assert.equal(eng[1].running, 300);   // + Bob: 200
      assert.equal(eng[2].running, 500);   // + Carol: 200
    });
  });

  describe('COUNT OVER', () => {
    it('partition count', () => {
      const r = db.execute('SELECT emp, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM sales');
      const eng = r.rows.filter(r => r.dept === 'Eng');
      assert.ok(eng.every(r => r.dept_count === 3));
    });

    it('running count', () => {
      const r = db.execute('SELECT id, COUNT(*) OVER (ORDER BY id) AS running_count FROM sales');
      assert.equal(r.rows[0].running_count, 1);
      assert.equal(r.rows[4].running_count, 5);
    });
  });

  describe('AVG OVER', () => {
    it('running average', () => {
      const r = db.execute('SELECT id, amount, AVG(amount) OVER (ORDER BY id) AS running_avg FROM sales');
      assert.equal(r.rows[0].running_avg, 100);   // 100/1
      assert.equal(r.rows[1].running_avg, 150);   // 300/2
    });
  });

  describe('Multiple window functions', () => {
    it('multiple window columns in same query', () => {
      const r = db.execute(`
        SELECT emp, dept, amount,
          ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) AS rn,
          SUM(amount) OVER (PARTITION BY dept) AS total
        FROM sales
      `);
      assert.equal(r.rows.length, 5);
      // Every row should have both rn and total
      assert.ok(r.rows.every(r => r.rn !== undefined && r.total !== undefined));
    });
  });
});
