import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

// Stress tests for feature intersection boundaries
// Each test combines 2-3 features that historically produce bugs

describe('Feature Intersection Stress Tests (2026-04-19)', () => {
  let db;

  function fresh() {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT, product TEXT, amount INT, region TEXT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 100, 'East')");
    db.execute("INSERT INTO sales VALUES (2, 'Widget', 200, 'West')");
    db.execute("INSERT INTO sales VALUES (3, 'Gadget', 150, 'East')");
    db.execute("INSERT INTO sales VALUES (4, 'Gadget', 300, 'East')");
    db.execute("INSERT INTO sales VALUES (5, 'Widget', 50, 'East')");
    return db;
  }

  describe('Window + CASE', () => {
    it('ROW_NUMBER in CASE WHEN', () => {
      fresh();
      const r = db.execute("SELECT id, CASE WHEN ROW_NUMBER() OVER (ORDER BY id) <= 3 THEN 'top' ELSE 'bottom' END AS pos FROM sales");
      assert.equal(r.rows[0].pos, 'top');
      assert.equal(r.rows[3].pos, 'bottom');
    });

    it('LAG in CASE with NULL check', () => {
      fresh();
      const r = db.execute("SELECT id, amount, CASE WHEN LAG(amount) OVER (ORDER BY id) IS NULL THEN 'first' ELSE 'rest' END AS pos FROM sales");
      assert.equal(r.rows[0].pos, 'first');
      assert.equal(r.rows[1].pos, 'rest');
    });

    it('window function in CASE result', () => {
      fresh();
      const r = db.execute("SELECT id, CASE WHEN region = 'East' THEN ROW_NUMBER() OVER (ORDER BY id) ELSE 0 END AS east_rank FROM sales");
      assert.equal(r.rows[0].east_rank, 1);  // East, rn=1
      assert.equal(r.rows[1].east_rank, 0);  // West, gets 0
    });
  });

  describe('CTE + Window', () => {
    it('window function over CTE result', () => {
      fresh();
      const r = db.execute(`
        WITH totals AS (
          SELECT product, SUM(amount) AS total 
          FROM sales GROUP BY product
        )
        SELECT product, total, RANK() OVER (ORDER BY total DESC) AS rank
        FROM totals
      `);
      assert.equal(r.rows[0].rank, 1);  // higher total
      assert.equal(r.rows[1].rank, 2);
    });

    it('CTE with window function inside', () => {
      fresh();
      const r = db.execute(`
        WITH ranked AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) AS rn
          FROM sales
        )
        SELECT product, amount FROM ranked WHERE rn = 1
      `);
      assert.equal(r.rows.length, 2);  // top per product
      const widgetTop = r.rows.find(r => r.product === 'Widget');
      assert.equal(widgetTop.amount, 200);  // highest Widget amount
    });
  });

  describe('Subquery + Aggregate', () => {
    it('aggregate in subquery', () => {
      fresh();
      const r = db.execute(`
        SELECT product FROM sales 
        WHERE amount > (SELECT AVG(amount) FROM sales)
      `);
      // avg = (100+200+150+300+50)/5 = 160
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows.every(row => row.product === 'Widget' || row.product === 'Gadget'));
    });

    it('correlated subquery with aggregate', () => {
      fresh();
      const r = db.execute(`
        SELECT product, 
          (SELECT SUM(amount) FROM sales s2 WHERE s2.product = s1.product) AS product_total
        FROM (SELECT DISTINCT product FROM sales) s1
      `);
      assert.equal(r.rows.length, 2);
    });

    it('subquery in HAVING', () => {
      fresh();
      const r = db.execute(`
        SELECT product, SUM(amount) AS total 
        FROM sales 
        GROUP BY product 
        HAVING SUM(amount) > (SELECT AVG(amount) FROM sales)
      `);
      assert.ok(r.rows.length > 0);
    });
  });

  describe('Multi-feature Chains', () => {
    it('CTE + JOIN + GROUP BY + HAVING + ORDER BY', () => {
      fresh();
      db.execute('CREATE TABLE regions (name TEXT, manager TEXT)');
      db.execute("INSERT INTO regions VALUES ('East', 'Alice'), ('West', 'Bob')");
      const r = db.execute(`
        WITH regional_sales AS (
          SELECT region, SUM(amount) AS total
          FROM sales
          GROUP BY region
        )
        SELECT r.manager, rs.total
        FROM regions r
        JOIN regional_sales rs ON r.name = rs.region
        WHERE rs.total > 100
        ORDER BY rs.total DESC
      `);
      assert.ok(r.rows.length >= 1);
      assert.ok(r.rows[0].total >= r.rows[r.rows.length - 1].total);
    });

    it('INSERT SELECT + RETURNING + expression', () => {
      fresh();
      db.execute('CREATE TABLE summary (product TEXT, total INT, category TEXT)');
      const r = db.execute(`
        INSERT INTO summary (product, total, category)
        SELECT product, SUM(amount), 'computed'
        FROM sales
        GROUP BY product
        RETURNING product, total
      `);
      assert.equal(r.rows.length, 2);
      assert.ok(r.rows[0].total > 0);
    });

    it('UNION ALL + CTE + ORDER BY + LIMIT', () => {
      fresh();
      const r = db.execute(`
        WITH big AS (SELECT * FROM sales WHERE amount >= 200),
             small AS (SELECT * FROM sales WHERE amount < 100)
        SELECT product, amount, 'big' AS category FROM big
        UNION ALL
        SELECT product, amount, 'small' AS category FROM small
        ORDER BY amount DESC
        LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].amount >= r.rows[1].amount);
    });

    it('SELECT *, window + arithmetic', () => {
      fresh();
      const r = db.execute(`
        SELECT *, 
          amount - LAG(amount) OVER (ORDER BY id) AS change,
          SUM(amount) OVER (ORDER BY id) AS running
        FROM sales
      `);
      assert.equal(r.rows.length, 5);
      assert.equal(r.rows[0].change, null);  // first row
      assert.ok(r.rows[4].running > 0);  // running total
      assert.equal(r.rows[0].id, 1);  // * columns present
    });

    it('CASE + aggregate + GROUP BY', () => {
      fresh();
      const r = db.execute(`
        SELECT product,
          SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) AS east_total,
          SUM(CASE WHEN region = 'West' THEN amount ELSE 0 END) AS west_total
        FROM sales
        GROUP BY product
      `);
      const widget = r.rows.find(r => r.product === 'Widget');
      assert.equal(widget.east_total, 150);  // 100 + 50
      assert.equal(widget.west_total, 200);
    });
  });

  describe('Edge Cases', () => {
    it('empty table with window function', () => {
      db = new Database();
      db.execute('CREATE TABLE empty (id INT, val INT)');
      const r = db.execute('SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM empty');
      assert.equal(r.rows.length, 0);
    });

    it('single row with LAG/LEAD', () => {
      db = new Database();
      db.execute('CREATE TABLE one (id INT, val INT)');
      db.execute('INSERT INTO one VALUES (1, 100)');
      const r = db.execute('SELECT val, LAG(val) OVER (ORDER BY id) AS prev, LEAD(val) OVER (ORDER BY id) AS next FROM one');
      assert.equal(r.rows[0].val, 100);
      assert.equal(r.rows[0].prev, null);
      assert.equal(r.rows[0].next, null);
    });

    it('NULL values in aggregate', () => {
      db = new Database();
      db.execute('CREATE TABLE nulls (id INT, val INT)');
      db.execute('INSERT INTO nulls VALUES (1, 10), (2, NULL), (3, 30)');
      const r = db.execute('SELECT COUNT(val) AS cnt, SUM(val) AS total, AVG(val) AS avg FROM nulls');
      assert.equal(r.rows[0].cnt, 2);  // NULL excluded
      assert.equal(r.rows[0].total, 40);
    });

    it('NOT NOT NOT in expression', () => {
      db = new Database();
      const r = db.execute('SELECT NOT NOT NOT TRUE AS result');
      assert.equal(r.rows[0].result, false);
    });

    it('deeply nested subquery', () => {
      db = new Database();
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1), (2), (3)');
      const r = db.execute('SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE id > (SELECT MIN(id) FROM t))');
      assert.equal(r.rows.length, 2);  // 2 and 3
    });
  });
});
