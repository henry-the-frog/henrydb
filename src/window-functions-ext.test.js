// window-functions-ext.test.js — Tests for LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Extended Window Functions', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT, dept TEXT, amount REAL)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'A', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'A', 300)");
    db.execute("INSERT INTO sales VALUES (4, 'B', 150)");
    db.execute("INSERT INTO sales VALUES (5, 'B', 250)");
    db.execute("INSERT INTO sales VALUES (6, 'B', 350)");
    return db;
  }

  describe('LAG', () => {
    it('should return previous row value', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, amount, LAG(amount) OVER (ORDER BY amount) AS prev FROM sales');
      
      // Sorted by amount: 100, 150, 200, 250, 300, 350
      // Find the row with amount=200
      const row200 = result.rows.find(r => r.amount === 200);
      assert.equal(row200.prev, 150); // Previous in sorted order
    });

    it('should return null for first row', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, amount, LAG(amount) OVER (ORDER BY amount) AS prev FROM sales');
      const firstRow = result.rows.find(r => r.amount === 100);
      assert.equal(firstRow.prev, null);
    });

    it('should work with PARTITION BY', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, dept, amount, LAG(amount) OVER (PARTITION BY dept ORDER BY amount) AS prev FROM sales');
      
      // Dept A: 100, 200, 300 — LAG for 200 should be 100
      const a200 = result.rows.find(r => r.dept === 'A' && r.amount === 200);
      assert.equal(a200.prev, 100);
      
      // Dept B first: 150 — LAG should be null
      const b150 = result.rows.find(r => r.dept === 'B' && r.amount === 150);
      assert.equal(b150.prev, null);
    });

    it('should support custom offset', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, amount, LAG(amount, 2) OVER (ORDER BY amount) AS prev2 FROM sales');
      
      // Sorted: 100, 150, 200, 250, 300, 350
      const row200 = result.rows.find(r => r.amount === 200);
      assert.equal(row200.prev2, 100); // 2 rows back
    });
  });

  describe('LEAD', () => {
    it('should return next row value', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, amount, LEAD(amount) OVER (ORDER BY amount) AS next FROM sales');
      
      const row200 = result.rows.find(r => r.amount === 200);
      assert.equal(row200.next, 250);
    });

    it('should return null for last row', () => {
      const db = makeDB();
      const result = db.execute('SELECT id, amount, LEAD(amount) OVER (ORDER BY amount) AS next FROM sales');
      const lastRow = result.rows.find(r => r.amount === 350);
      assert.equal(lastRow.next, null);
    });

    it('should work with PARTITION BY', () => {
      const db = makeDB();
      const result = db.execute('SELECT dept, amount, LEAD(amount) OVER (PARTITION BY dept ORDER BY amount) AS next FROM sales');
      
      const a100 = result.rows.find(r => r.dept === 'A' && r.amount === 100);
      assert.equal(a100.next, 200);
      
      const a300 = result.rows.find(r => r.dept === 'A' && r.amount === 300);
      assert.equal(a300.next, null);
    });
  });

  describe('FIRST_VALUE', () => {
    it('should return first value in partition', () => {
      const db = makeDB();
      const result = db.execute('SELECT dept, amount, FIRST_VALUE(amount) OVER (PARTITION BY dept ORDER BY amount) AS first FROM sales');
      
      // Dept A first value: 100
      const aRows = result.rows.filter(r => r.dept === 'A');
      assert.ok(aRows.every(r => r.first === 100));
      
      // Dept B first value: 150
      const bRows = result.rows.filter(r => r.dept === 'B');
      assert.ok(bRows.every(r => r.first === 150));
    });

    it('should work without PARTITION BY', () => {
      const db = makeDB();
      const result = db.execute('SELECT amount, FIRST_VALUE(amount) OVER (ORDER BY amount) AS first FROM sales');
      assert.ok(result.rows.every(r => r.first === 100));
    });
  });

  describe('LAST_VALUE', () => {
    it('should return last value in partition', () => {
      const db = makeDB();
      const result = db.execute('SELECT dept, amount, LAST_VALUE(amount) OVER (PARTITION BY dept ORDER BY amount) AS last FROM sales');
      
      // Dept A last value: 300
      const aRows = result.rows.filter(r => r.dept === 'A');
      assert.ok(aRows.every(r => r.last === 300));
      
      // Dept B last value: 350
      const bRows = result.rows.filter(r => r.dept === 'B');
      assert.ok(bRows.every(r => r.last === 350));
    });
  });

  describe('NTILE', () => {
    it('should divide into n groups', () => {
      const db = makeDB();
      const result = db.execute('SELECT amount, NTILE(3) OVER (ORDER BY amount) AS bucket FROM sales');
      
      // 6 rows / 3 groups = 2 per group
      const buckets = result.rows.map(r => r.bucket).sort();
      // Should have values 1, 1, 2, 2, 3, 3
      assert.ok(buckets.filter(b => b === 1).length === 2);
      assert.ok(buckets.filter(b => b === 2).length === 2);
      assert.ok(buckets.filter(b => b === 3).length === 2);
    });

    it('should handle uneven division', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (val INT)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('INSERT INTO t VALUES (2)');
      db.execute('INSERT INTO t VALUES (3)');
      db.execute('INSERT INTO t VALUES (4)');
      db.execute('INSERT INTO t VALUES (5)');
      
      const result = db.execute('SELECT val, NTILE(3) OVER (ORDER BY val) AS bucket FROM t');
      // 5 rows / 3 groups: 2, 2, 1
      const buckets = result.rows.map(r => r.bucket);
      assert.ok(buckets.includes(1));
      assert.ok(buckets.includes(2));
      assert.ok(buckets.includes(3));
    });

    it('should work with PARTITION BY', () => {
      const db = makeDB();
      const result = db.execute('SELECT dept, amount, NTILE(2) OVER (PARTITION BY dept ORDER BY amount) AS half FROM sales');
      
      // Each dept has 3 rows, NTILE(2): 2 in group 1, 1 in group 2
      const aHalves = result.rows.filter(r => r.dept === 'A').map(r => r.half);
      assert.ok(aHalves.includes(1));
      assert.ok(aHalves.includes(2));
    });
  });
});
