// advanced-query.test.js — NOT LIKE, NOT BETWEEN, complex queries
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NOT LIKE', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@test.org')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");
  });

  it('NOT LIKE filters out matching', () => {
    const result = db.execute("SELECT * FROM users WHERE email NOT LIKE '%@example.com'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Bob');
  });

  it('NOT LIKE with prefix', () => {
    const result = db.execute("SELECT * FROM users WHERE name NOT LIKE 'A%'");
    assert.equal(result.rows.length, 2);
  });
});

describe('NOT BETWEEN', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO nums VALUES (1, 5)');
    db.execute('INSERT INTO nums VALUES (2, 15)');
    db.execute('INSERT INTO nums VALUES (3, 25)');
    db.execute('INSERT INTO nums VALUES (4, 35)');
  });

  it('NOT BETWEEN excludes range', () => {
    const result = db.execute('SELECT * FROM nums WHERE val NOT BETWEEN 10 AND 30');
    assert.equal(result.rows.length, 2);
    assert.ok(result.rows.some(r => r.val === 5));
    assert.ok(result.rows.some(r => r.val === 35));
  });
});

describe('Complex queries', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, product TEXT, amount INT, status TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 'Widget', 100, 'shipped')");
    db.execute("INSERT INTO orders VALUES (2, 'Bob', 'Gadget', 200, 'pending')");
    db.execute("INSERT INTO orders VALUES (3, 'Alice', 'Doohickey', 50, 'shipped')");
    db.execute("INSERT INTO orders VALUES (4, 'Charlie', 'Widget', 100, 'cancelled')");
    db.execute("INSERT INTO orders VALUES (5, 'Bob', 'Widget', 150, 'shipped')");
    db.execute("INSERT INTO orders VALUES (6, 'Alice', 'Gadget', 300, 'pending')");
  });

  it('GROUP BY + HAVING + ORDER BY', () => {
    const result = db.execute("SELECT customer, SUM(amount) AS total FROM orders WHERE status != 'cancelled' GROUP BY customer HAVING total > 100 ORDER BY total DESC");
    assert.equal(result.rows[0].customer, 'Alice'); // 100+50+300=450
    assert.equal(result.rows[1].customer, 'Bob'); // 200+150=350
  });

  it('COUNT with GROUP BY and WHERE', () => {
    const result = db.execute("SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC");
    assert.equal(result.rows[0].status, 'shipped');
    assert.equal(result.rows[0].cnt, 3);
  });

  it('Subquery with aggregate', () => {
    const result = db.execute('SELECT * FROM orders WHERE amount > (SELECT AVG(amount) AS avg FROM orders)');
    // AVG = (100+200+50+100+150+300)/6 = 150
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.every(r => r.amount > 150));
  });

  it('CTE + GROUP BY + HAVING', () => {
    const result = db.execute("WITH active AS (SELECT * FROM orders WHERE status != 'cancelled') SELECT customer, COUNT(*) AS orders FROM active GROUP BY customer HAVING orders >= 2 ORDER BY orders DESC");
    assert.equal(result.rows[0].customer, 'Alice'); // 3 active orders
    assert.equal(result.rows[0].orders, 3);
  });

  it('DISTINCT + ORDER BY', () => {
    const result = db.execute('SELECT DISTINCT product FROM orders ORDER BY product');
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].product, 'Doohickey');
    assert.equal(result.rows[1].product, 'Gadget');
    assert.equal(result.rows[2].product, 'Widget');
  });

  it('CASE in GROUP BY query', () => {
    const result = db.execute("SELECT CASE WHEN amount >= 200 THEN 'high' ELSE 'low' END AS tier, COUNT(*) AS cnt FROM orders GROUP BY tier ORDER BY cnt DESC");
    assert.ok(result.rows.length > 0);
  });

  it('Window function + WHERE', () => {
    const result = db.execute("SELECT customer, amount, ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC) AS rn FROM orders WHERE status = 'shipped'");
    assert.ok(result.rows.length > 0);
    // Alice's highest shipped amount should be rn=1
    const aliceFirst = result.rows.find(r => r.customer === 'Alice' && r.rn === 1);
    assert.ok(aliceFirst);
  });

  it('Nested NOT and OR', () => {
    const result = db.execute("SELECT * FROM orders WHERE NOT (status = 'cancelled' OR status = 'pending')");
    assert.equal(result.rows.length, 3); // only shipped
  });

  it('BETWEEN with ORDER BY and LIMIT', () => {
    const result = db.execute('SELECT * FROM orders WHERE amount BETWEEN 100 AND 200 ORDER BY amount DESC LIMIT 2');
    assert.equal(result.rows.length, 2);
    assert.ok(result.rows[0].amount >= result.rows[1].amount);
  });

  it('COUNT DISTINCT in complex query', () => {
    const result = db.execute("SELECT COUNT(DISTINCT customer) AS unique_customers FROM orders WHERE status = 'shipped'");
    assert.equal(result.rows[0].unique_customers, 2); // Alice, Bob
  });
});
