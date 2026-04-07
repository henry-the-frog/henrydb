// null-arith.test.js — IS NULL, COALESCE, NULLIF, arithmetic tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NULL handling', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, name TEXT, score INT, grade TEXT)');
    db.execute("INSERT INTO data VALUES (1, 'Alice', 95, 'A')");
    db.execute("INSERT INTO data VALUES (2, 'Bob', NULL, 'B')");
    db.execute("INSERT INTO data VALUES (3, 'Charlie', 80, NULL)");
    db.execute('INSERT INTO data VALUES (4, NULL, NULL, NULL)');
  });

  describe('IS NULL', () => {
    it('finds NULL values', () => {
      const result = db.execute('SELECT * FROM data WHERE score IS NULL');
      assert.equal(result.rows.length, 2); // Bob and id=4
    });

    it('IS NOT NULL', () => {
      const result = db.execute('SELECT * FROM data WHERE score IS NOT NULL');
      assert.equal(result.rows.length, 2); // Alice and Charlie
    });

    it('IS NULL on text column', () => {
      const result = db.execute('SELECT * FROM data WHERE name IS NULL');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].id, 4);
    });

    it('IS NOT NULL on text column', () => {
      const result = db.execute('SELECT * FROM data WHERE grade IS NOT NULL');
      assert.equal(result.rows.length, 2);
    });

    it('IS NULL with AND', () => {
      const result = db.execute('SELECT * FROM data WHERE score IS NULL AND name IS NOT NULL');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Bob');
    });
  });

  describe('COALESCE', () => {
    it('returns first non-null', () => {
      const result = db.execute("SELECT COALESCE(grade, 'N/A') AS g FROM data WHERE id = 3");
      assert.equal(result.rows[0].g, 'N/A');
    });

    it('returns column value when not null', () => {
      const result = db.execute("SELECT COALESCE(grade, 'N/A') AS g FROM data WHERE id = 1");
      assert.equal(result.rows[0].g, 'A');
    });

    it('multi-arg COALESCE', () => {
      const result = db.execute("SELECT COALESCE(grade, name, 'unknown') AS val FROM data WHERE id = 4");
      assert.equal(result.rows[0].val, 'unknown');
    });

    it('COALESCE with numeric', () => {
      const result = db.execute('SELECT COALESCE(score, 0) AS s FROM data WHERE id = 2');
      assert.equal(result.rows[0].s, 0);
    });
  });

  describe('NULLIF', () => {
    it('returns NULL when equal', () => {
      const result = db.execute("SELECT NULLIF(grade, 'A') AS g FROM data WHERE id = 1");
      assert.equal(result.rows[0].g, null);
    });

    it('returns first arg when not equal', () => {
      const result = db.execute("SELECT NULLIF(grade, 'A') AS g FROM data WHERE id = 2");
      assert.equal(result.rows[0].g, 'B');
    });

    it('NULLIF with numbers', () => {
      const result = db.execute('SELECT NULLIF(score, 0) AS s FROM data WHERE id = 1');
      assert.equal(result.rows[0].s, 95);
    });
  });
});

describe('Arithmetic Expressions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, qty INT, discount INT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 100, 5, 10)");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 200, 3, 20)");
    db.execute("INSERT INTO products VALUES (3, 'Doohickey', 50, 10, 5)");
  });

  it('addition in SELECT', () => {
    const result = db.execute('SELECT name, price + 50 AS adjusted FROM products WHERE id = 1');
    assert.equal(result.rows[0].adjusted, 150);
  });

  it('subtraction in SELECT', () => {
    const result = db.execute('SELECT name, price - discount AS net FROM products WHERE id = 1');
    assert.equal(result.rows[0].net, 90);
  });

  it('multiplication in SELECT', () => {
    const result = db.execute('SELECT name, price * qty AS total FROM products WHERE id = 1');
    assert.equal(result.rows[0].total, 500);
  });

  it('division in SELECT', () => {
    const result = db.execute('SELECT name, price / qty AS unit FROM products WHERE id = 3');
    assert.equal(result.rows[0].unit, 5);
  });

  it('modulo in SELECT', () => {
    const result = db.execute('SELECT price % 30 AS remainder FROM products WHERE id = 1');
    assert.equal(result.rows[0].remainder, 10);
  });

  it('arithmetic in WHERE', () => {
    const result = db.execute('SELECT * FROM products WHERE price * qty > 500');
    assert.equal(result.rows.length, 1); // Only Gadget(600)
    assert.equal(result.rows[0].name, 'Gadget');
  });

  it('division by zero returns null', () => {
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO nums VALUES (1, 10, 0)');
    const result = db.execute('SELECT a / b AS r FROM nums WHERE id = 1');
    assert.equal(result.rows[0].r, null);
  });

  it('arithmetic with NULL returns null', () => {
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, a INT)');
    db.execute('INSERT INTO nums VALUES (1, NULL)');
    const result = db.execute('SELECT a + 5 AS r FROM nums WHERE id = 1');
    assert.equal(result.rows[0].r, null);
  });

  it('combined arithmetic and COALESCE', () => {
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, base INT, bonus INT)');
    db.execute('INSERT INTO scores VALUES (1, 100, NULL)');
    const result = db.execute('SELECT base + COALESCE(bonus, 0) AS total FROM scores WHERE id = 1');
    assert.equal(result.rows[0].total, 100);
  });
});
