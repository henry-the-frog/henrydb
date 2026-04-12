// lateral-join.test.js — LATERAL JOIN tests

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LATERAL JOIN', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales')");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 'Engineering')");
    db.execute('CREATE TABLE orders (id INT, employee_id INT, amount REAL, product TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 1, 500, 'Widget')");
    db.execute("INSERT INTO orders VALUES (2, 1, 300, 'Gadget')");
    db.execute("INSERT INTO orders VALUES (3, 1, 800, 'Gizmo')");
    db.execute("INSERT INTO orders VALUES (4, 2, 200, 'Widget')");
    db.execute("INSERT INTO orders VALUES (5, 2, 150, 'Gadget')");
    db.execute("INSERT INTO orders VALUES (6, 3, 1000, 'Gizmo')");
    db.execute("INSERT INTO orders VALUES (7, 3, 400, 'Widget')");
    return db;
  }

  it('should support CROSS JOIN LATERAL with basic subquery', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, o.amount, o.product
      FROM employees e
      CROSS JOIN LATERAL (
        SELECT amount, product FROM orders WHERE employee_id = e.id ORDER BY amount DESC LIMIT 1
      ) o
    `);
    
    assert.ok(result.rows, 'Should return rows');
    assert.equal(result.rows.length, 3, 'Each employee has a top order');
    
    // Alice's top order is Gizmo (800)
    const alice = result.rows.find(r => r.name === 'Alice');
    assert.ok(alice, 'Should have Alice');
    assert.equal(alice.amount, 800);
    assert.equal(alice.product, 'Gizmo');
    
    // Bob's top order is Widget (200)
    const bob = result.rows.find(r => r.name === 'Bob');
    assert.ok(bob, 'Should have Bob');
    assert.equal(bob.amount, 200);
    
    // Carol's top order is Gizmo (1000)
    const carol = result.rows.find(r => r.name === 'Carol');
    assert.ok(carol, 'Should have Carol');
    assert.equal(carol.amount, 1000);
  });

  it('should support LEFT JOIN LATERAL (include rows with no matches)', () => {
    const db = makeDB();
    // Add an employee with no orders
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 'Marketing')");
    
    const result = db.execute(`
      SELECT e.name, o.amount
      FROM employees e
      LEFT JOIN LATERAL (
        SELECT amount FROM orders WHERE employee_id = e.id LIMIT 1
      ) o ON TRUE
    `);
    
    assert.equal(result.rows.length, 4, 'Should include Dave even with no orders');
    const dave = result.rows.find(r => r.name === 'Dave');
    assert.ok(dave, 'Should have Dave');
  });

  it('should support LATERAL with LIMIT per outer row', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, o.product
      FROM employees e
      CROSS JOIN LATERAL (
        SELECT product FROM orders WHERE employee_id = e.id ORDER BY amount DESC LIMIT 2
      ) o
    `);
    
    // Alice has 3 orders but LIMIT 2 → 2 results
    const aliceRows = result.rows.filter(r => r.name === 'Alice');
    assert.equal(aliceRows.length, 2, 'Alice should have exactly 2 orders (LIMIT 2)');
    
    // Bob has 2 orders → 2 results
    const bobRows = result.rows.filter(r => r.name === 'Bob');
    assert.equal(bobRows.length, 2);
  });

  it('should support LATERAL with aggregates in subquery', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, stats.total, stats.cnt
      FROM employees e
      CROSS JOIN LATERAL (
        SELECT SUM(amount) AS total, COUNT(*) AS cnt
        FROM orders WHERE employee_id = e.id
      ) stats
    `);
    
    assert.equal(result.rows.length, 3);
    
    const alice = result.rows.find(r => r.name === 'Alice');
    assert.equal(alice.total, 1600); // 500 + 300 + 800
    assert.equal(alice.cnt, 3);
  });

  it('should allow LATERAL subquery to reference outer columns', () => {
    const db = makeDB();
    const result = db.execute(`
      SELECT e.name, o.product
      FROM employees e
      CROSS JOIN LATERAL (
        SELECT product FROM orders WHERE employee_id = e.id AND amount > 400
      ) o
    `);
    
    // Alice: Widget(500), Gizmo(800) > 400
    const aliceRows = result.rows.filter(r => r.name === 'Alice');
    assert.equal(aliceRows.length, 2);
    
    // Bob: Widget(200), Gadget(150) — none > 400
    const bobRows = result.rows.filter(r => r.name === 'Bob');
    assert.equal(bobRows.length, 0);
    
    // Carol: Gizmo(1000) > 400
    const carolRows = result.rows.filter(r => r.name === 'Carol');
    assert.equal(carolRows.length, 1);
  });
});
