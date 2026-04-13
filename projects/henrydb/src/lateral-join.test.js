// lateral-join.test.js — LATERAL JOIN (PostgreSQL extension)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LATERAL JOIN', () => {
  it('top-N per group with LATERAL', () => {
    const db = new Database();
    db.execute('CREATE TABLE depts (id INT, name TEXT)');
    db.execute('CREATE TABLE emps (id INT, dept_id INT, name TEXT, salary INT)');
    db.execute("INSERT INTO depts VALUES (1, 'Eng'), (2, 'Sales')");
    db.execute("INSERT INTO emps VALUES (1, 1, 'Alice', 120), (2, 1, 'Bob', 100), (3, 1, 'Carol', 110), (4, 2, 'Dave', 90), (5, 2, 'Eve', 95)");

    const r = db.execute(`
      SELECT d.name AS dept, e.name AS emp, e.salary
      FROM depts d
      JOIN LATERAL (SELECT name, salary FROM emps WHERE dept_id = d.id ORDER BY salary DESC LIMIT 2) e ON true
      ORDER BY d.name, e.salary DESC
    `);
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].dept, 'Eng');
    assert.equal(r.rows[0].emp, 'Alice');
    assert.equal(r.rows[2].dept, 'Sales');
    assert.equal(r.rows[2].emp, 'Eve');
  });

  it('LEFT LATERAL JOIN preserves left rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE cats (id INT, name TEXT)');
    db.execute('CREATE TABLE items (cat_id INT, val INT)');
    db.execute("INSERT INTO cats VALUES (1, 'A'), (2, 'B'), (3, 'C')");
    db.execute("INSERT INTO items VALUES (1, 10), (1, 20), (2, 30)");

    const r = db.execute(`
      SELECT c.name, i.val
      FROM cats c
      LEFT JOIN LATERAL (SELECT val FROM items WHERE cat_id = c.id ORDER BY val DESC LIMIT 1) i ON true
      ORDER BY c.name
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].name, 'A');
    assert.equal(r.rows[0].val, 20);
    assert.equal(r.rows[2].name, 'C');
    assert.equal(r.rows[2].val, undefined); // no items for C
  });

  it('LATERAL with aggregation in subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)");

    const r = db.execute(`
      SELECT c.name, s.total, s.cnt
      FROM customers c
      JOIN LATERAL (SELECT SUM(amount) AS total, COUNT(*) AS cnt FROM orders WHERE customer_id = c.id) s ON true
      ORDER BY c.name
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].total, 300);
    assert.equal(r.rows[0].cnt, 2);
    assert.equal(r.rows[1].total, 50);
  });
});
