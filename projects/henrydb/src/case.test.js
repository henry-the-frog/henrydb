// case.test.js — CASE expression tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CASE Expressions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10, 'A')");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 50, 'B')");
    db.execute("INSERT INTO products VALUES (3, 'Premium', 200, 'A')");
    db.execute("INSERT INTO products VALUES (4, 'Budget', 5, 'C')");
    db.execute("INSERT INTO products VALUES (5, 'Standard', 25, 'B')");
  });

  it('basic CASE in SELECT', () => {
    const result = db.execute("SELECT name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 20 THEN 'moderate' ELSE 'cheap' END AS tier FROM products ORDER BY id");
    assert.equal(result.rows[0].tier, 'cheap');     // Widget(10)
    assert.equal(result.rows[1].tier, 'moderate');   // Gadget(50)
    assert.equal(result.rows[2].tier, 'expensive');  // Premium(200)
    assert.equal(result.rows[3].tier, 'cheap');      // Budget(5)
  });

  it('CASE without ELSE returns null', () => {
    const result = db.execute("SELECT name, CASE WHEN price > 100 THEN 'high' END AS tier FROM products WHERE id = 1");
    assert.equal(result.rows[0].tier, null);
  });

  it('CASE in WHERE', () => {
    const result = db.execute("SELECT * FROM products WHERE CASE WHEN price > 30 THEN 1 ELSE 0 END = 1");
    assert.equal(result.rows.length, 2); // Gadget(50) and Premium(200)
  });

  it('CASE with category mapping', () => {
    const result = db.execute("SELECT name, CASE WHEN category = 'A' THEN 'Alpha' WHEN category = 'B' THEN 'Beta' ELSE 'Other' END AS cat_name FROM products ORDER BY id");
    assert.equal(result.rows[0].cat_name, 'Alpha');
    assert.equal(result.rows[1].cat_name, 'Beta');
    assert.equal(result.rows[3].cat_name, 'Other');
  });

  it('multiple WHEN clauses', () => {
    const result = db.execute("SELECT name, CASE WHEN price < 10 THEN 'tier1' WHEN price < 30 THEN 'tier2' WHEN price < 100 THEN 'tier3' WHEN price < 300 THEN 'tier4' ELSE 'tier5' END AS tier FROM products ORDER BY price");
    assert.equal(result.rows[0].tier, 'tier1');  // Budget(5)
    assert.equal(result.rows[1].tier, 'tier2');  // Widget(10)
    assert.equal(result.rows[2].tier, 'tier2');  // Standard(25)
    assert.equal(result.rows[3].tier, 'tier3');  // Gadget(50)
    assert.equal(result.rows[4].tier, 'tier4');  // Premium(200)
  });

  it('CASE evaluates conditions in order', () => {
    // First matching WHEN wins
    const result = db.execute("SELECT name, CASE WHEN price > 1 THEN 'first' WHEN price > 100 THEN 'second' END AS match FROM products WHERE id = 3");
    assert.equal(result.rows[0].match, 'first'); // price>1 matches first even though price>100 also true
  });

  it('CASE with NULL comparison', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO items VALUES (1, NULL)');
    db.execute('INSERT INTO items VALUES (2, 42)');
    const result = db.execute("SELECT id, CASE WHEN val = 42 THEN 'found' ELSE 'missing' END AS status FROM items ORDER BY id");
    assert.equal(result.rows[0].status, 'missing');
    assert.equal(result.rows[1].status, 'found');
  });

  it('CASE with string results', () => {
    const result = db.execute("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS answer FROM products LIMIT 1");
    assert.equal(result.rows[0].answer, 'yes');
  });

  it('CASE with numeric results', () => {
    const result = db.execute('SELECT name, CASE WHEN price > 100 THEN 3 WHEN price > 20 THEN 2 ELSE 1 END AS tier_num FROM products ORDER BY id');
    assert.equal(result.rows[0].tier_num, 1);
    assert.equal(result.rows[1].tier_num, 2);
    assert.equal(result.rows[2].tier_num, 3);
  });
});
