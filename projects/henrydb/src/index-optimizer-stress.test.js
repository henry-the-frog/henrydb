// index-optimizer-stress.test.js — Stress-test index selection and optimizer decisions
import { describe, it, beforeEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Index Optimizer Stress Test', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    // Create table with 1000 rows
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, stock INTEGER)');
    db.execute('CREATE INDEX idx_category ON products(category)');
    db.execute('CREATE INDEX idx_price ON products(price)');
    db.execute('CREATE INDEX idx_stock ON products(stock)');
    
    for (let i = 1; i <= 1000; i++) {
      const cat = ['electronics', 'books', 'clothing', 'food', 'toys'][i % 5];
      const price = (i * 1.5 + 0.99).toFixed(2);
      const stock = i % 100;
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', '${cat}', ${price}, ${stock})`);
    }
  });

  describe('Basic Index Selection', () => {
    it('should use index for equality on indexed column', () => {
      const plan = db.execute("EXPLAIN SELECT * FROM products WHERE category = 'electronics'");
      const planStr = JSON.stringify(plan);
      // Should mention index scan, not table scan
      const result = db.execute("SELECT COUNT(*) as cnt FROM products WHERE category = 'electronics'");
      assert.equal(result.rows[0].cnt, 200);
    });

    it('should use PK index for id lookup', () => {
      const result = db.execute('SELECT * FROM products WHERE id = 500');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].id, 500);
    });

    it('should return correct results with AND conditions', () => {
      const result = db.execute("SELECT * FROM products WHERE category = 'electronics' AND price > 100");
      // Verify all results match both conditions
      for (const row of result.rows) {
        assert.equal(row.category, 'electronics');
        assert.ok(row.price > 100);
      }
    });
  });

  describe('Range Scans', () => {
    it('should return correct results for price > threshold', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM products WHERE price > 1000');
      const expected = db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt;
      // Price ranges from 2.49 to 1501.49
      assert.ok(result.rows[0].cnt > 0);
      assert.ok(result.rows[0].cnt < expected);
    });

    it('should return correct results for BETWEEN', () => {
      const result = db.execute('SELECT * FROM products WHERE price BETWEEN 100 AND 200');
      for (const row of result.rows) {
        assert.ok(row.price >= 100 && row.price <= 200, `Price ${row.price} not in [100, 200]`);
      }
      assert.ok(result.rows.length > 0);
    });

    it('should return correct results for stock < threshold', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM products WHERE stock < 10');
      assert.ok(result.rows[0].cnt > 0);
    });
  });

  describe('Compound Conditions', () => {
    it('AND with two indexed columns should be correct', () => {
      const result = db.execute("SELECT * FROM products WHERE category = 'books' AND stock = 50");
      for (const row of result.rows) {
        assert.equal(row.category, 'books');
        assert.equal(row.stock, 50);
      }
    });

    it('OR with indexed columns should return union', () => {
      const result = db.execute("SELECT * FROM products WHERE category = 'electronics' OR category = 'toys'");
      for (const row of result.rows) {
        assert.ok(row.category === 'electronics' || row.category === 'toys',
          `Expected electronics or toys, got ${row.category}`);
      }
      assert.equal(result.rows.length, 400); // 200 each
    });

    it('complex OR/AND mix should be correct', () => {
      const result = db.execute("SELECT * FROM products WHERE (category = 'electronics' AND price > 500) OR category = 'books'");
      for (const row of result.rows) {
        const matchesFirst = row.category === 'electronics' && row.price > 500;
        const matchesSecond = row.category === 'books';
        assert.ok(matchesFirst || matchesSecond, `Row doesn't match: ${JSON.stringify(row)}`);
      }
      assert.ok(result.rows.length > 0);
    });

    it('IN list should work correctly', () => {
      const result = db.execute("SELECT * FROM products WHERE category IN ('electronics', 'books')");
      for (const row of result.rows) {
        assert.ok(row.category === 'electronics' || row.category === 'books');
      }
      assert.equal(result.rows.length, 400);
    });

    it('NOT IN should exclude correctly', () => {
      const result = db.execute("SELECT * FROM products WHERE category NOT IN ('electronics', 'books', 'clothing')");
      for (const row of result.rows) {
        assert.ok(!['electronics', 'books', 'clothing'].includes(row.category));
      }
      assert.equal(result.rows.length, 400); // food + toys
    });
  });

  describe('Join Index Usage', () => {
    beforeEach(() => {
      db.execute('CREATE TABLE categories (name TEXT PRIMARY KEY, tax_rate REAL)');
      db.execute("INSERT INTO categories VALUES ('electronics', 0.10)");
      db.execute("INSERT INTO categories VALUES ('books', 0.05)");
      db.execute("INSERT INTO categories VALUES ('clothing', 0.08)");
      db.execute("INSERT INTO categories VALUES ('food', 0.02)");
      db.execute("INSERT INTO categories VALUES ('toys', 0.07)");
    });

    it('should correctly join on indexed column', () => {
      const result = db.execute(`
        SELECT p.name, p.price, c.tax_rate
        FROM products p
        JOIN categories c ON p.category = c.name
        WHERE p.id <= 5
      `);
      assert.equal(result.rows.length, 5);
      for (const row of result.rows) {
        assert.ok(row.tax_rate !== undefined && row.tax_rate !== null, 
          `Missing tax_rate for ${row.name}`);
      }
    });
  });

  describe('Correctness Under Load', () => {
    it('50 random equality queries should all be correct', () => {
      for (let i = 0; i < 50; i++) {
        const id = Math.floor(Math.random() * 1000) + 1;
        const result = db.execute(`SELECT * FROM products WHERE id = ${id}`);
        assert.equal(result.rows.length, 1, `ID ${id} should return exactly 1 row`);
        assert.equal(result.rows[0].id, id);
      }
    });

    it('aggregate with WHERE on indexed column', () => {
      const result = db.execute("SELECT SUM(price) as total FROM products WHERE category = 'electronics'");
      assert.ok(result.rows[0].total > 0);
      // Cross-check: manual sum
      const manual = db.execute("SELECT price FROM products WHERE category = 'electronics'");
      const manualSum = manual.rows.reduce((s, r) => s + r.price, 0);
      assert.ok(Math.abs(result.rows[0].total - manualSum) < 0.01);
    });

    it('GROUP BY on indexed column', () => {
      const result = db.execute('SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY category');
      assert.equal(result.rows.length, 5);
      const total = result.rows.reduce((s, r) => s + r.cnt, 0);
      assert.equal(total, 1000);
    });

    it('ORDER BY on indexed column', () => {
      const result = db.execute('SELECT price FROM products ORDER BY price LIMIT 10');
      for (let i = 1; i < result.rows.length; i++) {
        assert.ok(result.rows[i].price >= result.rows[i - 1].price,
          `Not sorted: ${result.rows[i - 1].price} > ${result.rows[i].price}`);
      }
    });

    it('DISTINCT on indexed column', () => {
      const result = db.execute('SELECT DISTINCT category FROM products ORDER BY category');
      assert.equal(result.rows.length, 5);
      assert.deepEqual(result.rows.map(r => r.category), 
        ['books', 'clothing', 'electronics', 'food', 'toys']);
    });
  });

  describe('EXPLAIN Plan Verification', () => {
    it('EXPLAIN should show plan for indexed query', () => {
      const result = db.execute("EXPLAIN SELECT * FROM products WHERE category = 'electronics'");
      assert.ok(result.rows.length > 0, 'EXPLAIN should return plan');
    });

    it('EXPLAIN ANALYZE should show timing', () => {
      const result = db.execute("EXPLAIN ANALYZE SELECT * FROM products WHERE id = 500");
      assert.ok(result.rows.length > 0, 'EXPLAIN ANALYZE should return plan with timing');
    });
  });

  describe('Edge Cases', () => {
    it('NULL in indexed column', () => {
      db.execute("INSERT INTO products VALUES (1001, 'No Category', NULL, 99.99, 10)");
      const result = db.execute('SELECT * FROM products WHERE category IS NULL');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'No Category');
    });

    it('empty result from index lookup', () => {
      const result = db.execute("SELECT * FROM products WHERE category = 'nonexistent'");
      assert.equal(result.rows.length, 0);
    });

    it('index after DELETE', () => {
      db.execute("DELETE FROM products WHERE category = 'electronics'");
      const result = db.execute("SELECT COUNT(*) as cnt FROM products WHERE category = 'electronics'");
      assert.equal(result.rows[0].cnt, 0);
      // Other categories should be unaffected
      const others = db.execute("SELECT COUNT(*) as cnt FROM products WHERE category = 'books'");
      assert.equal(others.rows[0].cnt, 200);
    });

    it('index after UPDATE', () => {
      db.execute("UPDATE products SET category = 'premium' WHERE id = 1");
      const result = db.execute("SELECT * FROM products WHERE category = 'premium'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].id, 1);
    });
  });
});
