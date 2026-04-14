import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

test('CROSS JOIN LATERAL with subquery', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, val INT)');
  db.execute('INSERT INTO t VALUES (1, 10)');
  db.execute('INSERT INTO t VALUES (2, 20)');
  
  const r = db.execute('SELECT t.id, g.doubled FROM t CROSS JOIN LATERAL (SELECT val * 2 AS doubled) AS g');
  assert.equal(r.rows.length, 2);
  assert.equal(r.rows[0].doubled, 20);
  assert.equal(r.rows[1].doubled, 40);
});

test('Comma LATERAL syntax (implicit cross join)', () => {
  const db = new Database();
  db.execute('CREATE TABLE items (id INT, price INT)');
  db.execute('INSERT INTO items VALUES (1, 100)');
  db.execute('INSERT INTO items VALUES (2, 200)');
  
  const r = db.execute('SELECT i.id, calc.tax FROM items i, LATERAL (SELECT price * 0.1 AS tax) AS calc');
  assert.equal(r.rows.length, 2);
  assert.equal(r.rows[0].tax, 10);
  assert.equal(r.rows[1].tax, 20);
});

test('LEFT JOIN LATERAL', () => {
  const db = new Database();
  db.execute('CREATE TABLE parents (id INT, name TEXT)');
  db.execute("INSERT INTO parents VALUES (1, 'Alice')");
  db.execute("INSERT INTO parents VALUES (2, 'Bob')");
  db.execute('CREATE TABLE children (parent_id INT, name TEXT)');
  db.execute("INSERT INTO children VALUES (1, 'Charlie')");
  db.execute("INSERT INTO children VALUES (1, 'Diana')");
  // Bob has no children
  
  const r = db.execute("SELECT p.name, c.name AS child FROM parents p LEFT JOIN LATERAL (SELECT name FROM children WHERE parent_id = p.id) AS c ON TRUE");
  // Alice→Charlie, Alice→Diana, Bob→null
  assert.equal(r.rows.length, 3);
});

test('Comma LATERAL with computed column', () => {
  const db = new Database();
  db.execute('CREATE TABLE products (name TEXT, price INT, qty INT)');
  db.execute("INSERT INTO products VALUES ('Widget', 10, 5)");
  db.execute("INSERT INTO products VALUES ('Gadget', 20, 3)");
  
  const r = db.execute('SELECT p.name, c.total FROM products p, LATERAL (SELECT price * qty AS total) AS c');
  assert.equal(r.rows.length, 2);
  const widget = r.rows.find(row => row.name === 'Widget');
  assert.equal(widget.total, 50);
  const gadget = r.rows.find(row => row.name === 'Gadget');
  assert.equal(gadget.total, 60);
});

test('Multiple comma LATERAL joins', () => {
  const db = new Database();
  db.execute('CREATE TABLE nums (n INT)');
  db.execute('INSERT INTO nums VALUES (5)');
  db.execute('INSERT INTO nums VALUES (10)');
  
  const r = db.execute('SELECT nums.n, a.doubled, b.tripled FROM nums, LATERAL (SELECT n * 2 AS doubled) AS a, LATERAL (SELECT n * 3 AS tripled) AS b');
  assert.equal(r.rows.length, 2);
  assert.equal(r.rows[0].doubled, 10);
  assert.equal(r.rows[0].tripled, 15);
  assert.equal(r.rows[1].doubled, 20);
  assert.equal(r.rows[1].tripled, 30);
});
