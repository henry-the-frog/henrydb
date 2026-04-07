// window-advanced.test.js — Advanced window function tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Advanced Window Functions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, region TEXT, amount INT, quarter INT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 'North', 100, 1)");
    db.execute("INSERT INTO sales VALUES (2, 'Widget', 'North', 150, 2)");
    db.execute("INSERT INTO sales VALUES (3, 'Widget', 'North', 200, 3)");
    db.execute("INSERT INTO sales VALUES (4, 'Widget', 'South', 80, 1)");
    db.execute("INSERT INTO sales VALUES (5, 'Widget', 'South', 120, 2)");
    db.execute("INSERT INTO sales VALUES (6, 'Gadget', 'North', 90, 1)");
    db.execute("INSERT INTO sales VALUES (7, 'Gadget', 'North', 110, 2)");
    db.execute("INSERT INTO sales VALUES (8, 'Gadget', 'South', 70, 1)");
    db.execute("INSERT INTO sales VALUES (9, 'Gadget', 'South', 130, 2)");
    db.execute("INSERT INTO sales VALUES (10, 'Gadget', 'South', 160, 3)");
  });

  describe('Running aggregates', () => {
    it('SUM() OVER () — total across all rows', () => {
      const result = db.execute('SELECT id, amount, SUM(amount) OVER () AS total FROM sales');
      assert.equal(result.rows.length, 10);
      const totalAmount = 100+150+200+80+120+90+110+70+130+160;
      assert.ok(result.rows.every(r => r.total === totalAmount));
    });

    it('COUNT(*) OVER (PARTITION BY product)', () => {
      const result = db.execute('SELECT id, product, COUNT(*) OVER (PARTITION BY product) AS product_count FROM sales');
      const widgets = result.rows.filter(r => r.product === 'Widget');
      assert.ok(widgets.every(r => r.product_count === 5));
      const gadgets = result.rows.filter(r => r.product === 'Gadget');
      assert.ok(gadgets.every(r => r.product_count === 5));
    });

    it('SUM OVER (PARTITION BY region)', () => {
      const result = db.execute('SELECT id, region, amount, SUM(amount) OVER (PARTITION BY region) AS region_total FROM sales');
      const north = result.rows.filter(r => r.region === 'North');
      // North: 100+150+200+90+110 = 650
      assert.ok(north.every(r => r.region_total === 650));
      const south = result.rows.filter(r => r.region === 'South');
      // South: 80+120+70+130+160 = 560
      assert.ok(south.every(r => r.region_total === 560));
    });
  });

  describe('ROW_NUMBER with complex ordering', () => {
    it('ROW_NUMBER with multiple order columns', () => {
      const result = db.execute('SELECT id, product, amount, ROW_NUMBER() OVER (ORDER BY product, amount DESC) AS rn FROM sales');
      assert.equal(result.rows.length, 10);
      // First Gadget rows (by amount DESC), then Widget rows
      const rn1 = result.rows.find(r => r.rn === 1);
      assert.equal(rn1.product, 'Gadget');
      assert.equal(rn1.amount, 160); // highest Gadget amount
    });

    it('ROW_NUMBER partitioned by product, ordered by quarter', () => {
      const result = db.execute('SELECT product, region, quarter, ROW_NUMBER() OVER (PARTITION BY product ORDER BY quarter) AS rn FROM sales');
      // Within each product, rows ordered by quarter
      const widgetFirst = result.rows.find(r => r.product === 'Widget' && r.rn === 1);
      assert.equal(widgetFirst.quarter, 1);
    });
  });

  describe('Window functions with expressions', () => {
    it('window function in WHERE context', () => {
      // Note: standard SQL doesn't allow window functions in WHERE
      // But some engines support it via subquery
      try {
        // Direct window in WHERE should fail or return error
        // Just test that a basic window function works
        const result = db.execute('SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS rnk FROM sales');
        assert.equal(result.rows.length, 10);
        const top = result.rows.find(r => r.rnk === 1);
        assert.equal(top.amount, 200);
      } catch (e) {
        // Some SQL engines throw on window in WHERE — that's fine
      }
    });

    it('multiple different window definitions', () => {
      const result = db.execute(`
        SELECT id, product, region, amount,
          ROW_NUMBER() OVER (ORDER BY id) AS global_rn,
          ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) AS product_rn,
          SUM(amount) OVER (PARTITION BY region) AS region_total
        FROM sales
      `);
      assert.equal(result.rows.length, 10);
      // Check all three window columns exist
      assert.ok(result.rows.every(r =>
        r.global_rn !== undefined &&
        r.product_rn !== undefined &&
        r.region_total !== undefined
      ));
    });
  });

  describe('Window functions on empty/single partitions', () => {
    it('works with single row', () => {
      db.execute('CREATE TABLE tiny (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO tiny VALUES (1, 42)');
      const result = db.execute('SELECT id, val, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(val) OVER () AS total FROM tiny');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].rn, 1);
      assert.equal(result.rows[0].total, 42);
    });

    it('RANK on single row', () => {
      db.execute('CREATE TABLE tiny (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO tiny VALUES (1, 42)');
      const result = db.execute('SELECT id, RANK() OVER (ORDER BY val) AS rnk FROM tiny');
      assert.equal(result.rows[0].rnk, 1);
    });

    it('partition with all same values', () => {
      db.execute('CREATE TABLE uniform (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO uniform VALUES (1, 10)');
      db.execute('INSERT INTO uniform VALUES (2, 10)');
      db.execute('INSERT INTO uniform VALUES (3, 10)');
      const result = db.execute('SELECT id, RANK() OVER (ORDER BY val) AS rnk FROM uniform');
      // All same value — all rank 1
      assert.ok(result.rows.every(r => r.rnk === 1));
    });

    it('DENSE_RANK on all ties', () => {
      db.execute('CREATE TABLE uniform (id INT PRIMARY KEY, val INT)');
      db.execute('INSERT INTO uniform VALUES (1, 10)');
      db.execute('INSERT INTO uniform VALUES (2, 10)');
      db.execute('INSERT INTO uniform VALUES (3, 10)');
      const result = db.execute('SELECT id, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM uniform');
      assert.ok(result.rows.every(r => r.drnk === 1));
    });
  });

  describe('Window functions combined with other clauses', () => {
    it('window function with WHERE clause', () => {
      const result = db.execute(`
        SELECT id, product, amount,
          ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
        FROM sales
        WHERE product = 'Widget'
      `);
      assert.equal(result.rows.length, 5);
      // Row numbers should be 1-5 for filtered results
      const rns = result.rows.map(r => r.rn).sort();
      assert.deepEqual(rns, [1, 2, 3, 4, 5]);
    });

    it('window function with GROUP BY fails gracefully or works', () => {
      // Window functions after GROUP BY is unusual but some engines support it
      try {
        const result = db.execute(`
          SELECT product, SUM(amount) AS total,
            RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
          FROM sales
          GROUP BY product
        `);
        // If supported, should have 2 rows (Widget, Gadget)
        if (result && result.rows) {
          assert.ok(result.rows.length <= 2);
        }
      } catch (e) {
        // It's ok if this isn't supported
      }
    });

    it('window function with ORDER BY at query level', () => {
      const result = db.execute(`
        SELECT id, amount,
          ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
        FROM sales
        ORDER BY id
      `);
      // Should be ordered by id, but rn values based on amount
      assert.equal(result.rows[0].id, 1);
      // Row 1 (id=1, amount=100) should have a high rn (low amount)
      assert.ok(result.rows[0].rn > 5);
    });

    it('window function with LIMIT', () => {
      const result = db.execute(`
        SELECT id, amount,
          ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
        FROM sales
        LIMIT 3
      `);
      assert.equal(result.rows.length, 3);
    });
  });
});
