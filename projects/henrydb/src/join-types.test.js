// join-types.test.js — All JOIN type tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setup() {
  const db = new Database();
  db.execute('CREATE TABLE a (id INT, name TEXT)');
  db.execute('CREATE TABLE b (id INT, a_id INT, val TEXT)');
  db.execute("INSERT INTO a VALUES (1,'alice'),(2,'bob'),(3,'charlie')");
  db.execute("INSERT INTO b VALUES (1,1,'x'),(2,1,'y'),(3,2,'z'),(4,9,'orphan')");
  return db;
}

describe('JOIN Types', () => {
  it('INNER JOIN', () => {
    const db = setup();
    const r = db.execute('SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id ORDER BY a.name, b.val');
    assert.equal(r.rows.length, 3); // alice×2 + bob×1
  });

  it('LEFT JOIN keeps unmatched left rows', () => {
    const db = setup();
    const r = db.execute('SELECT a.name, b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.name');
    assert.equal(r.rows.length, 4); // alice×2 + bob×1 + charlie×1(null)
    const charlie = r.rows.find(row => row.name === 'charlie');
    assert.equal(charlie.val, null);
  });

  it('RIGHT JOIN keeps unmatched right rows', () => {
    const db = setup();
    const r = db.execute('SELECT a.name, b.val FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.val');
    assert.equal(r.rows.length, 4); // alice×2 + bob×1 + orphan×1(null name)
    const orphan = r.rows.find(row => row.val === 'orphan');
    assert.equal(orphan.name, null);
  });

  it('FULL OUTER JOIN keeps both unmatched', () => {
    const db = setup();
    const r = db.execute('SELECT a.name, b.val FROM a FULL OUTER JOIN b ON a.id = b.a_id ORDER BY a.name, b.val');
    assert.ok(r.rows.length >= 5); // All matched + charlie(null) + orphan(null)
  });

  it('CROSS JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE x (v INT)');
    db.execute('CREATE TABLE y (v INT)');
    db.execute('INSERT INTO x VALUES (1),(2)');
    db.execute('INSERT INTO y VALUES (10),(20),(30)');
    const r = db.execute('SELECT x.v as xv, y.v as yv FROM x CROSS JOIN y ORDER BY xv, yv');
    assert.equal(r.rows.length, 6); // 2 × 3
  });

  it('NATURAL JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT, val TEXT)');
    db.execute("INSERT INTO t1 VALUES (1,'alice'),(2,'bob')");
    db.execute("INSERT INTO t2 VALUES (1,'x'),(2,'y')");
    const r = db.execute('SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'alice');
    assert.equal(r.rows[0].val, 'x');
  });

  it('self join', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL),(2,'VP',1),(3,'Dir',2)");
    const r = db.execute(`
      SELECT e.name as employee, m.name as manager
      FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id
      ORDER BY e.id
    `);
    assert.equal(r.rows[0].employee, 'CEO');
    assert.equal(r.rows[0].manager, null);
    assert.equal(r.rows[1].manager, 'CEO');
  });

  it('multi-table join', () => {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute('CREATE TABLE orders (id INT, cust_id INT, date TEXT)');
    db.execute('CREATE TABLE items (id INT, order_id INT, product TEXT)');
    db.execute("INSERT INTO customers VALUES (1,'alice')");
    db.execute("INSERT INTO orders VALUES (1,1,'2026-01-01')");
    db.execute("INSERT INTO items VALUES (1,1,'widget'),(2,1,'gadget')");
    
    const r = db.execute(`
      SELECT c.name, o.date, i.product
      FROM customers c
      JOIN orders o ON c.id = o.cust_id
      JOIN items i ON o.id = i.order_id
      ORDER BY i.product
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'alice');
  });
});
