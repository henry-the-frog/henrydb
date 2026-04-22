// derived-table-volcano.test.js — Derived table (subquery in FROM) Volcano tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Derived Table Volcano Integration', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL, region TEXT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT, tier TEXT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 10 + 1}, ${i * 10}, '${['East','West','North'][i % 3]}')`);
    }
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${i <= 3 ? 'Gold' : 'Silver'}')`);
    }
  });

  describe('Basic derived tables', () => {
    it('should handle simple FROM subquery', () => {
      const r = db.execute(`
        SELECT sub.customer_id, sub.total 
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub 
        ORDER BY sub.total DESC LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].total > r.rows[1].total);
      assert.ok(r.rows[0].customer_id !== undefined);
    });

    it('should handle derived table without alias qualification', () => {
      const r = db.execute(`
        SELECT customer_id, total 
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub 
        ORDER BY total DESC LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].total > 0);
    });

    it('should handle derived table with WHERE filter', () => {
      const r = db.execute(`
        SELECT sub.customer_id, sub.total 
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub 
        WHERE sub.total > 1200
        ORDER BY sub.total DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows.every(row => row.total > 1200));
    });
  });

  describe('Derived table + JOIN', () => {
    it('should join derived table with real table', () => {
      const r = db.execute(`
        SELECT c.name, sub.total
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub
        JOIN customers c ON sub.customer_id = c.id
        ORDER BY sub.total DESC LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].name.startsWith('Customer'));
      assert.ok(r.rows[0].total > 0);
    });

    it('should handle derived table with GROUP BY in outer query', () => {
      const r = db.execute(`
        SELECT c.tier, SUM(sub.total) as tier_total
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub
        JOIN customers c ON sub.customer_id = c.id
        GROUP BY c.tier
        ORDER BY tier_total DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows[0].tier !== undefined);
      assert.ok(r.rows[0].tier_total > 0);
    });
  });

  describe('Derived table correctness', () => {
    it('should match legacy engine results', () => {
      const sql = `
        SELECT c.name, sub.total
        FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub
        JOIN customers c ON sub.customer_id = c.id
        ORDER BY c.name
      `;
      
      const volcanoResult = db.execute(sql);
      
      db._useVolcano = false;
      const legacyResult = db.execute(sql);
      
      assert.equal(volcanoResult.rows.length, legacyResult.rows.length);
      for (let i = 0; i < volcanoResult.rows.length; i++) {
        assert.equal(volcanoResult.rows[i].name, legacyResult.rows[i].name);
        assert.equal(volcanoResult.rows[i].total, legacyResult.rows[i].total);
      }
    });

    it('should handle derived table with aggregates and having', () => {
      const r = db.execute(`
        SELECT sub.region, sub.order_count
        FROM (SELECT region, COUNT(*) as order_count FROM orders GROUP BY region HAVING COUNT(*) > 30) sub
        ORDER BY sub.order_count DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows.every(row => row.order_count > 30));
    });

    it('should handle nested derived tables', () => {
      const r = db.execute(`
        SELECT outer_sub.customer_id, outer_sub.total
        FROM (
          SELECT sub.customer_id, sub.total
          FROM (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) sub
          WHERE sub.total > 1000
        ) outer_sub
        ORDER BY outer_sub.total DESC LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows.every(row => row.total > 1000));
    });
  });

  describe('Function-wrapped aggregates with derived tables', () => {
    it('should handle COALESCE in derived table', () => {
      const r = db.execute(`
        SELECT sub.region, sub.total
        FROM (SELECT region, COALESCE(SUM(amount), 0) as total FROM orders GROUP BY region) sub
        ORDER BY sub.total DESC
      `);
      assert.ok(r.rows.length > 0);
      assert.ok(r.rows[0].total > 0);
    });

    it('should handle scalar correlated subquery with derived table', () => {
      const r = db.execute(`
        SELECT c.name, 
          (SELECT SUM(sub.amount) FROM (SELECT * FROM orders WHERE amount > 500) sub WHERE sub.customer_id = c.id) as big_total
        FROM customers c
        ORDER BY c.name LIMIT 3
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].name.startsWith('Customer'));
    });
  });
});
