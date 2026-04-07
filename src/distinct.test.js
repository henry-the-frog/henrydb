// distinct.test.js — DISTINCT tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('DISTINCT', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, category TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 'A', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'Gadget', 'B', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'Widget', 'A', 150)");
    db.execute("INSERT INTO sales VALUES (4, 'Doohickey', 'A', 100)");
    db.execute("INSERT INTO sales VALUES (5, 'Gadget', 'B', 200)");
    db.execute("INSERT INTO sales VALUES (6, 'Gizmo', 'C', 300)");
  });

  describe('SELECT DISTINCT', () => {
    it('removes duplicate rows', () => {
      const result = db.execute('SELECT DISTINCT category FROM sales');
      assert.equal(result.rows.length, 3); // A, B, C
    });

    it('DISTINCT with multiple columns', () => {
      const result = db.execute('SELECT DISTINCT product, category FROM sales');
      assert.equal(result.rows.length, 4); // Widget/A, Gadget/B, Doohickey/A, Gizmo/C
    });

    it('DISTINCT on all-unique data', () => {
      const result = db.execute('SELECT DISTINCT id FROM sales');
      assert.equal(result.rows.length, 6);
    });

    it('DISTINCT with ORDER BY', () => {
      const result = db.execute('SELECT DISTINCT category FROM sales ORDER BY category');
      assert.equal(result.rows[0].category, 'A');
      assert.equal(result.rows[1].category, 'B');
      assert.equal(result.rows[2].category, 'C');
    });

    it('DISTINCT with LIMIT', () => {
      const result = db.execute('SELECT DISTINCT category FROM sales LIMIT 2');
      assert.equal(result.rows.length, 2);
    });

    it('DISTINCT on empty result', () => {
      const result = db.execute("SELECT DISTINCT category FROM sales WHERE amount > 999");
      assert.equal(result.rows.length, 0);
    });
  });

  describe('COUNT(DISTINCT ...)', () => {
    it('counts distinct values', () => {
      const result = db.execute('SELECT COUNT(DISTINCT category) AS cnt FROM sales');
      assert.equal(result.rows[0].cnt, 3);
    });

    it('counts distinct products', () => {
      const result = db.execute('SELECT COUNT(DISTINCT product) AS cnt FROM sales');
      assert.equal(result.rows[0].cnt, 4); // Widget, Gadget, Doohickey, Gizmo
    });

    it('COUNT DISTINCT with GROUP BY', () => {
      const result = db.execute('SELECT category, COUNT(DISTINCT amount) AS unique_amounts FROM sales GROUP BY category');
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.unique_amounts, 2); // 100, 150
      const catB = result.rows.find(r => r.category === 'B');
      assert.equal(catB.unique_amounts, 1); // 200
    });

    it('COUNT DISTINCT vs COUNT', () => {
      const distinct = db.execute('SELECT COUNT(DISTINCT product) AS cnt FROM sales');
      const all = db.execute('SELECT COUNT(product) AS cnt FROM sales');
      assert.ok(distinct.rows[0].cnt <= all.rows[0].cnt);
      assert.equal(distinct.rows[0].cnt, 4);
      assert.equal(all.rows[0].cnt, 6);
    });
  });

  describe('Combined', () => {
    it('DISTINCT with WHERE', () => {
      const result = db.execute("SELECT DISTINCT category FROM sales WHERE amount >= 150");
      assert.equal(result.rows.length, 3); // A(150), B(200), C(300)
    });

    it('without DISTINCT has duplicates', () => {
      const result = db.execute('SELECT category FROM sales');
      assert.equal(result.rows.length, 6); // includes duplicates
    });
  });
});
