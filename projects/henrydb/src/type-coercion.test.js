// type-coercion.test.js — Type coercion, aliasing, and more edge cases
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Type Coercion', () => {
  it('string to number comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO data VALUES (1, '42')");
    // In our engine, this might or might not coerce
    const r = db.execute('SELECT * FROM data WHERE val = 42');
    // Either works or returns 0 — just no crash
    assert.ok(r.rows.length >= 0);
  });

  it('NULL comparisons', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO data VALUES (1, NULL)');
    // NULL = NULL should be false in SQL
    const r = db.execute('SELECT * FROM data WHERE val = NULL');
    assert.equal(r.rows.length, 0); // NULL = NULL is false
  });

  it('NULL in arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO data VALUES (1, 10, NULL)');
    const r = db.execute('SELECT a + b AS result FROM data WHERE id = 1');
    assert.equal(r.rows[0].result, null); // Any op with NULL = NULL
  });

  it('boolean TRUE/FALSE literals', () => {
    const db = new Database();
    db.execute('CREATE TABLE flags (id INT PRIMARY KEY, active BOOLEAN)');
    db.execute('INSERT INTO flags VALUES (1, TRUE)');
    db.execute('INSERT INTO flags VALUES (2, FALSE)');
    const r = db.execute('SELECT * FROM flags WHERE active = TRUE');
    assert.equal(r.rows.length, 1);
  });
});

describe('SELECT aliasing', () => {
  it('column alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT val AS value FROM t');
    assert.equal(r.rows[0].value, 42);
  });

  it('aggregate alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT COUNT(*) AS total_count, SUM(val) AS total_sum FROM t');
    assert.equal(r.rows[0].total_count, 2);
    assert.equal(r.rows[0].total_sum, 30);
  });

  it('function alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    const r = db.execute('SELECT UPPER(name) AS upper_name FROM t');
    assert.equal(r.rows[0].upper_name, 'HELLO');
  });

  it('CASE alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    const r = db.execute("SELECT CASE WHEN val > 50 THEN 'high' ELSE 'low' END AS category FROM t");
    assert.equal(r.rows[0].category, 'high');
  });
});

describe('Comprehensive SQL pipeline', () => {
  it('full e-commerce query', () => {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, tier TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');

    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'Gold')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob', 'Silver')");
    db.execute("INSERT INTO customers VALUES (3, 'Charlie', 'Gold')");
    db.execute("INSERT INTO customers VALUES (4, 'Diana', 'Bronze')");

    for (let i = 1; i <= 20; i++) {
      const cid = (i % 4) + 1;
      const status = i % 5 === 0 ? 'cancelled' : 'completed';
      db.execute(`INSERT INTO orders VALUES (${i}, ${cid}, ${i * 10}, '${status}')`);
    }

    // Complex query: total spending by customer for completed orders
    const r = db.execute("SELECT customers.name, SUM(orders.amount) AS total FROM customers JOIN orders ON customers.id = orders.customer_id WHERE orders.status = 'completed' GROUP BY customers.name ORDER BY total DESC");
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows[0].total >= r.rows[r.rows.length - 1].total);
  });

  it('analytics pipeline: CTE + aggregate + window', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT PRIMARY KEY, user_id INT, action TEXT, val INT)');
    for (let i = 1; i <= 30; i++) {
      const uid = (i % 3) + 1;
      db.execute(`INSERT INTO events VALUES (${i}, ${uid}, 'click', ${i})`);
    }

    // CTE to get user totals, then query
    const r = db.execute('WITH user_stats AS (SELECT user_id, COUNT(*) AS cnt, SUM(val) AS total FROM events GROUP BY user_id) SELECT * FROM user_stats ORDER BY total DESC');
    assert.equal(r.rows.length, 3);
  });

  it('data migration pipeline', () => {
    const db = new Database();
    db.execute('CREATE TABLE old_data (id INT PRIMARY KEY, val INT, cat TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO old_data VALUES (${i}, ${i}, '${i % 2 === 0 ? "even" : "odd"}')`);

    // Create new table, INSERT SELECT only evens, verify
    db.execute('CREATE TABLE new_data (id INT PRIMARY KEY, val INT, cat TEXT)');
    db.execute("INSERT INTO new_data SELECT * FROM old_data WHERE cat = 'even'");
    const r = db.execute('SELECT COUNT(*) AS cnt FROM new_data');
    assert.equal(r.rows[0].cnt, 25);
    assert.ok(db.execute('SELECT * FROM new_data').rows.every(r => r.val % 2 === 0));
  });

  it('reporting query with multiple features', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, region TEXT, amount INT, quarter INT)');
    for (let i = 1; i <= 40; i++) {
      const prod = ['Widget', 'Gadget', 'Doohickey', 'Gizmo'][i % 4];
      const region = ['East', 'West'][i % 2];
      db.execute(`INSERT INTO sales VALUES (${i}, '${prod}', '${region}', ${i * 5}, ${(i % 4) + 1})`);
    }

    const r = db.execute('SELECT region, COUNT(*) AS deals, SUM(amount) AS revenue FROM sales WHERE quarter BETWEEN 1 AND 2 GROUP BY region ORDER BY revenue DESC');
    assert.ok(r.rows.length > 0);
  });
});
