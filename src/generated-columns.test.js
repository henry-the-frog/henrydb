import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('Generated Columns', () => {
  describe('STORED generated columns', () => {
    it('computes value on INSERT', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      const result = db.execute("SELECT * FROM products");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].price, 100);
      assert.equal(result.rows[0].tax, 10);
      assert.equal(result.rows[0].total, 110);
    });

    it('recomputes on UPDATE', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      db.execute("UPDATE products SET price = 200 WHERE price = 100");
      
      const result = db.execute("SELECT * FROM products");
      assert.equal(result.rows[0].total, 210);
    });

    it('prevents direct INSERT to generated column', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      
      assert.throws(() => {
        db.execute("INSERT INTO products (price, tax, total) VALUES (100, 10, 999)");
      }, /Cannot INSERT.*generated column/);
    });

    it('prevents direct UPDATE of generated column', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      assert.throws(() => {
        db.execute("UPDATE products SET total = 999 WHERE price = 100");
      }, /Cannot INSERT.*generated column/);
    });

    it('works with multiple generated columns', () => {
      const db = new Database();
      db.execute("CREATE TABLE rect (w REAL, h REAL, area REAL GENERATED ALWAYS AS (w * h) STORED, perimeter REAL GENERATED ALWAYS AS (2 * w + 2 * h) STORED)");
      db.execute("INSERT INTO rect (w, h) VALUES (5, 3)");
      
      const result = db.execute("SELECT * FROM rect");
      assert.equal(result.rows[0].area, 15);
      assert.equal(result.rows[0].perimeter, 16);
    });

    it('generated column usable in WHERE', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      db.execute("INSERT INTO products (price, tax) VALUES (200, 20)");
      db.execute("INSERT INTO products (price, tax) VALUES (50, 5)");
      
      const result = db.execute("SELECT * FROM products WHERE total > 100");
      assert.equal(result.rows.length, 2);
    });

    it('works with string expressions', () => {
      const db = new Database();
      db.execute("CREATE TABLE users (fname TEXT, lname TEXT, full_name TEXT GENERATED ALWAYS AS (fname || ' ' || lname) STORED)");
      db.execute("INSERT INTO users (fname, lname) VALUES ('Alice', 'Smith')");
      
      const result = db.execute("SELECT full_name FROM users");
      assert.equal(result.rows[0].full_name, 'Alice Smith');
    });
  });

  describe('VIRTUAL generated columns', () => {
    it('computes value on SELECT (not stored)', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) VIRTUAL)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      const result = db.execute("SELECT * FROM products");
      assert.equal(result.rows[0].total, 110);
    });

    it('VIRTUAL column reflects updates to base columns', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) VIRTUAL)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      db.execute("UPDATE products SET price = 200 WHERE price = 100");
      
      const result = db.execute("SELECT * FROM products");
      assert.equal(result.rows[0].total, 210);
    });

    it('default mode is VIRTUAL when not specified', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax))");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      const result = db.execute("SELECT * FROM products");
      assert.equal(result.rows[0].total, 110);
    });
  });

  describe('Generated columns with functions', () => {
    it('LOWER function in generated column', () => {
      const db = new Database();
      db.execute("CREATE TABLE users (name TEXT, lower_name TEXT GENERATED ALWAYS AS (LOWER(name)) STORED)");
      db.execute("INSERT INTO users (name) VALUES ('Alice')");
      
      const result = db.execute("SELECT * FROM users");
      assert.equal(result.rows[0].lower_name, 'alice');
    });

    it('LENGTH function in generated column', () => {
      const db = new Database();
      db.execute("CREATE TABLE words (word TEXT, word_len INTEGER GENERATED ALWAYS AS (LENGTH(word)) STORED)");
      db.execute("INSERT INTO words (word) VALUES ('hello')");
      
      const result = db.execute("SELECT * FROM words");
      assert.equal(result.rows[0].word_len, 5);
    });
  });
});
