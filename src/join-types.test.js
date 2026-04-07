// join-types.test.js — RIGHT JOIN, CROSS JOIN, aggregate JOINs
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JOIN types', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO colors VALUES (1, 'Red')");
    db.execute("INSERT INTO colors VALUES (2, 'Blue')");
    db.execute("INSERT INTO colors VALUES (3, 'Green')");

    db.execute('CREATE TABLE sizes (id INT PRIMARY KEY, label TEXT)');
    db.execute("INSERT INTO sizes VALUES (1, 'Small')");
    db.execute("INSERT INTO sizes VALUES (2, 'Large')");

    db.execute('CREATE TABLE products (id INT PRIMARY KEY, color_id INT, size_id INT, price INT)');
    db.execute('INSERT INTO products VALUES (1, 1, 1, 10)');
    db.execute('INSERT INTO products VALUES (2, 1, 2, 20)');
    db.execute('INSERT INTO products VALUES (3, 2, 1, 15)');
  });

  describe('CROSS JOIN', () => {
    it('cartesian product', () => {
      const r = db.execute('SELECT colors.name AS color, sizes.label AS size FROM colors CROSS JOIN sizes');
      assert.equal(r.rows.length, 6); // 3 colors × 2 sizes
    });

    it('CROSS JOIN with WHERE', () => {
      const r = db.execute("SELECT colors.name, sizes.label FROM colors CROSS JOIN sizes WHERE colors.name = 'Red'");
      assert.equal(r.rows.length, 2);
    });
  });

  describe('RIGHT JOIN', () => {
    it('includes unmatched from right', () => {
      const r = db.execute('SELECT products.id AS pid, colors.name AS color FROM products RIGHT JOIN colors ON products.color_id = colors.id');
      // Green (id=3) has no products
      assert.ok(r.rows.length >= 4); // 3 products + 1 unmatched Green
      const green = r.rows.find(row => row.color === 'Green');
      assert.ok(green);
      assert.equal(green.pid, null);
    });

    it('RIGHT JOIN all matched', () => {
      const r = db.execute('SELECT products.id, sizes.label FROM products RIGHT JOIN sizes ON products.size_id = sizes.id');
      assert.ok(r.rows.length >= 3); // All sizes have products
    });
  });

  describe('Aggregate with JOINs', () => {
    it('COUNT with JOIN', () => {
      const r = db.execute('SELECT colors.name, COUNT(products.id) AS cnt FROM colors LEFT JOIN products ON colors.id = products.color_id GROUP BY colors.name ORDER BY cnt DESC');
      assert.ok(r.rows.length === 3);
    });

    it('SUM with JOIN', () => {
      const r = db.execute('SELECT colors.name, SUM(products.price) AS total FROM colors JOIN products ON colors.id = products.color_id GROUP BY colors.name ORDER BY total DESC');
      assert.ok(r.rows.length >= 2);
    });

    it('AVG with JOIN + HAVING', () => {
      const r = db.execute('SELECT colors.name, AVG(products.price) AS avg_price FROM colors JOIN products ON colors.id = products.color_id GROUP BY colors.name HAVING avg_price > 14');
      assert.ok(r.rows.length >= 1);
    });
  });
});
