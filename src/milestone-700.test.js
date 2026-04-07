// milestone-700.test.js — Push HenryDB to 700 tests!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Milestone: 700 Tests', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('complex nested subquery', () => {
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)');
    db.execute("INSERT INTO products VALUES (1, 'A', 100)");
    db.execute("INSERT INTO products VALUES (2, 'B', 200)");
    db.execute("INSERT INTO products VALUES (3, 'C', 150)");
    const result = db.execute('SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products)');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'B');
  });

  it('BETWEEN in WHERE', () => {
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO nums VALUES (${i}, ${i * 10})`);
    const result = db.execute('SELECT * FROM nums WHERE val BETWEEN 30 AND 70');
    assert.equal(result.rows.length, 5);
  });

  it('multiple ORDER BY columns', () => {
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'A', 100)");
    db.execute("INSERT INTO emp VALUES (2, 'B', 200)");
    db.execute("INSERT INTO emp VALUES (3, 'A', 200)");
    db.execute("INSERT INTO emp VALUES (4, 'B', 100)");
    const result = db.execute('SELECT * FROM emp ORDER BY dept ASC, salary DESC');
    assert.equal(result.rows[0].dept, 'A');
    assert.equal(result.rows[0].salary, 200);
  });

  it('COUNT DISTINCT', () => {
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 'Alice')");
    db.execute("INSERT INTO orders VALUES (2, 'Bob')");
    db.execute("INSERT INTO orders VALUES (3, 'Alice')");
    const result = db.execute('SELECT COUNT(DISTINCT customer) AS unique_customers FROM orders');
    assert.equal(result.rows[0].unique_customers, 2);
  });

  it('nested CASE expression', () => {
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO scores VALUES (1, 95)');
    db.execute('INSERT INTO scores VALUES (2, 75)');
    db.execute('INSERT INTO scores VALUES (3, 55)');
    const result = db.execute("SELECT score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 60 THEN 'C' ELSE 'F' END AS grade FROM scores ORDER BY score DESC");
    assert.equal(result.rows[0].grade, 'A');
    assert.equal(result.rows[1].grade, 'B');
    assert.equal(result.rows[2].grade, 'F');
  });

  it('UPDATE with WHERE', () => {
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 1)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 1)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 0)");
    db.execute("UPDATE users SET active = 0 WHERE name = 'Bob'");
    const result = db.execute('SELECT * FROM users WHERE active = 1');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('DELETE with complex WHERE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO items VALUES (${i}, ${i})`);
    db.execute('DELETE FROM items WHERE val > 5 AND val < 8');
    const result = db.execute('SELECT COUNT(*) AS cnt FROM items');
    assert.equal(result.rows[0].cnt, 8); // removed 6 and 7
  });

  it('🎯 700th test — multiple aggregates with GROUP BY', () => {
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'North', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'North', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'South', 150)");
    db.execute("INSERT INTO sales VALUES (4, 'South', 250)");
    db.execute("INSERT INTO sales VALUES (5, 'East', 300)");
    const result = db.execute('SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, MIN(amount) AS mn, MAX(amount) AS mx FROM sales GROUP BY region ORDER BY total DESC');
    assert.equal(result.rows.length, 3);
    const east = result.rows.find(r => r.region === 'East');
    assert.equal(east.cnt, 1);
    assert.equal(east.total, 300);
  });
});
