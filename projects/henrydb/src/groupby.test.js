// groupby.test.js — GROUP BY and HAVING tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP BY', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, category TEXT, amount INT, qty INT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 'A', 100, 5)");
    db.execute("INSERT INTO sales VALUES (2, 'Gadget', 'B', 200, 3)");
    db.execute("INSERT INTO sales VALUES (3, 'Doohickey', 'A', 150, 7)");
    db.execute("INSERT INTO sales VALUES (4, 'Thingamajig', 'B', 300, 2)");
    db.execute("INSERT INTO sales VALUES (5, 'Whatchamacallit', 'A', 50, 10)");
    db.execute("INSERT INTO sales VALUES (6, 'Gizmo', 'C', 250, 4)");
  });

  describe('Basic GROUP BY', () => {
    it('groups with COUNT', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category');
      assert.equal(result.rows.length, 3);
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.cnt, 3);
      const catB = result.rows.find(r => r.category === 'B');
      assert.equal(catB.cnt, 2);
      const catC = result.rows.find(r => r.category === 'C');
      assert.equal(catC.cnt, 1);
    });

    it('groups with SUM', () => {
      const result = db.execute('SELECT category, SUM(amount) AS total FROM sales GROUP BY category');
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.total, 300); // 100 + 150 + 50
      const catB = result.rows.find(r => r.category === 'B');
      assert.equal(catB.total, 500); // 200 + 300
    });

    it('groups with AVG', () => {
      const result = db.execute('SELECT category, AVG(amount) AS avg_amt FROM sales GROUP BY category');
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.avg_amt, 100); // (100+150+50)/3
    });

    it('groups with MIN and MAX', () => {
      const result = db.execute('SELECT category, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM sales GROUP BY category');
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.min_amt, 50);
      assert.equal(catA.max_amt, 150);
    });

    it('multiple aggregates in one query', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt, SUM(amount) AS total, AVG(qty) AS avg_qty FROM sales GROUP BY category');
      const catB = result.rows.find(r => r.category === 'B');
      assert.equal(catB.cnt, 2);
      assert.equal(catB.total, 500);
      assert.equal(catB.avg_qty, 2.5); // (3+2)/2
    });
  });

  describe('HAVING', () => {
    it('filters groups with HAVING', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category HAVING cnt > 1');
      assert.equal(result.rows.length, 2); // A(3) and B(2), not C(1)
    });

    it('HAVING with SUM', () => {
      const result = db.execute('SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING total >= 300');
      assert.equal(result.rows.length, 2); // A(300) and B(500)
    });

    it('HAVING with equality', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category HAVING cnt = 1');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].category, 'C');
    });
  });

  describe('GROUP BY with WHERE', () => {
    it('WHERE filters before grouping', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales WHERE amount > 100 GROUP BY category');
      const catA = result.rows.find(r => r.category === 'A');
      assert.equal(catA.cnt, 1); // only Doohickey(150), not Widget(100) or Whatchamacallit(50)
    });
  });

  describe('GROUP BY with ORDER BY', () => {
    it('ORDER BY aggregate', () => {
      const result = db.execute('SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY total DESC');
      assert.equal(result.rows[0].category, 'B'); // 500
      assert.equal(result.rows[1].category, 'A'); // 300
      assert.equal(result.rows[2].category, 'C'); // 250
    });

    it('ORDER BY group column', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category ORDER BY category');
      assert.equal(result.rows[0].category, 'A');
      assert.equal(result.rows[1].category, 'B');
      assert.equal(result.rows[2].category, 'C');
    });
  });

  describe('GROUP BY with LIMIT', () => {
    it('LIMIT after GROUP BY', () => {
      const result = db.execute('SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category ORDER BY cnt DESC LIMIT 2');
      assert.equal(result.rows.length, 2);
      assert.equal(result.rows[0].category, 'A'); // 3
    });
  });

  describe('Edge cases', () => {
    it('GROUP BY on empty result', () => {
      const result = db.execute("SELECT category, COUNT(*) AS cnt FROM sales WHERE amount > 9999 GROUP BY category");
      assert.equal(result.rows.length, 0);
    });

    it('GROUP BY all same category', () => {
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, val INT)');
      db.execute("INSERT INTO items VALUES (1, 'X', 10)");
      db.execute("INSERT INTO items VALUES (2, 'X', 20)");
      db.execute("INSERT INTO items VALUES (3, 'X', 30)");
      const result = db.execute('SELECT cat, SUM(val) AS total FROM items GROUP BY cat');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].total, 60);
    });

    it('GROUP BY with multiple columns', () => {
      db.execute('CREATE TABLE events (id INT PRIMARY KEY, year INT, month INT, count INT)');
      db.execute('INSERT INTO events VALUES (1, 2024, 1, 10)');
      db.execute('INSERT INTO events VALUES (2, 2024, 1, 20)');
      db.execute('INSERT INTO events VALUES (3, 2024, 2, 15)');
      db.execute('INSERT INTO events VALUES (4, 2025, 1, 5)');
      const result = db.execute('SELECT year, month, SUM(count) AS total FROM events GROUP BY year, month');
      assert.equal(result.rows.length, 3);
      const jan24 = result.rows.find(r => r.year === 2024 && r.month === 1);
      assert.equal(jan24.total, 30);
    });
  });
});
