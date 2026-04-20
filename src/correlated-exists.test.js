import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

function setup() {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INTEGER, name TEXT, active INTEGER, region TEXT)');
  db.execute('CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount NUMERIC, status TEXT)');
  db.execute('CREATE TABLE products (id INTEGER, name TEXT, category TEXT)');
  db.execute('CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, qty INTEGER)');

  // Customers
  db.execute("INSERT INTO customers VALUES (1, 'Alice', 1, 'east')");
  db.execute("INSERT INTO customers VALUES (2, 'Bob', 0, 'west')");
  db.execute("INSERT INTO customers VALUES (3, 'Carol', 1, 'east')");
  db.execute("INSERT INTO customers VALUES (4, 'Dave', 1, 'west')");

  // Orders
  db.execute("INSERT INTO orders VALUES (10, 1, 100, 'shipped')");
  db.execute("INSERT INTO orders VALUES (11, 1, 50, 'pending')");
  db.execute("INSERT INTO orders VALUES (12, 2, 200, 'shipped')");
  db.execute("INSERT INTO orders VALUES (13, 3, 300, 'shipped')");
  db.execute("INSERT INTO orders VALUES (14, 99, 10, 'pending')"); // orphan

  // Products
  db.execute("INSERT INTO products VALUES (100, 'Widget', 'tools')");
  db.execute("INSERT INTO products VALUES (101, 'Gadget', 'electronics')");

  // Order items
  db.execute('INSERT INTO order_items VALUES (10, 100, 5)');
  db.execute('INSERT INTO order_items VALUES (10, 101, 2)');
  db.execute('INSERT INTO order_items VALUES (12, 100, 1)');
  db.execute('INSERT INTO order_items VALUES (13, 101, 10)');

  return db;
}

describe('Correlated EXISTS Decorrelation', () => {

  it('basic correlated EXISTS — active customers with orders', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM orders o 
      WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.active = 1)
    `);
    // Active customers: 1 (Alice), 3 (Carol), 4 (Dave)
    // Orders for active customers: 10, 11, 13
    assert.equal(rows.length, 3);
    const ids = rows.map(r => r.id).sort();
    assert.deepEqual(ids, [10, 11, 13]);
  });

  it('correlated NOT EXISTS — orders without matching customer', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM orders o 
      WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)
    `);
    // Only order 14 (customer_id=99) has no matching customer
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 14);
  });

  it('correlated EXISTS with additional local filter', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM orders o 
      WHERE EXISTS (
        SELECT 1 FROM customers c 
        WHERE c.id = o.customer_id AND c.region = 'east'
      )
    `);
    // East region: Alice (1), Carol (3)
    // Orders: 10, 11 (Alice), 13 (Carol)
    assert.equal(rows.length, 3);
    const ids = rows.map(r => r.id).sort();
    assert.deepEqual(ids, [10, 11, 13]);
  });

  it('correlated NOT EXISTS — customers without orders', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM customers c 
      WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
    `);
    // Dave (4) has no orders
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Dave');
  });

  it('EXISTS with empty result set', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM orders o 
      WHERE EXISTS (
        SELECT 1 FROM customers c 
        WHERE c.id = o.customer_id AND c.name = 'Nobody'
      )
    `);
    assert.equal(rows.length, 0);
  });

  it('EXISTS combined with other WHERE conditions', () => {
    const db = setup();
    const rows = query(db, `
      SELECT * FROM orders o 
      WHERE o.status = 'shipped' 
        AND EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.active = 1)
    `);
    // Shipped + active customer: orders 10 (Alice, shipped), 13 (Carol, shipped)
    assert.equal(rows.length, 2);
    const ids = rows.map(r => r.id).sort();
    assert.deepEqual(ids, [10, 13]);
  });

  it('NOT EXISTS with all rows matching', () => {
    const db = setup();
    // All customers have IDs 1-4, and orders exist for 1,2,3
    // NOT EXISTS should return customer 4 (Dave)
    const rows = query(db, `
      SELECT * FROM customers c 
      WHERE NOT EXISTS (
        SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'shipped'
      )
    `);
    // Shipped orders: 10 (cust 1), 12 (cust 2), 13 (cust 3)
    // Dave (4) has no shipped orders → returned
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Dave');
  });

  it('performance: EXISTS decorrelation is batch (not N+1)', () => {
    const db = new Database();
    db.execute('CREATE TABLE big_outer (id INTEGER)');
    db.execute('CREATE TABLE big_inner (ref_id INTEGER, val INTEGER)');
    
    // Insert 1000 rows
    for (let i = 0; i < 1000; i++) {
      db.execute('INSERT INTO big_outer VALUES (' + i + ')');
      if (i % 2 === 0) {
        db.execute('INSERT INTO big_inner VALUES (' + i + ', ' + (i * 10) + ')');
      }
    }
    
    const t0 = Date.now();
    const rows = query(db, 
      'SELECT * FROM big_outer o WHERE EXISTS (SELECT 1 FROM big_inner i WHERE i.ref_id = o.id)'
    );
    const elapsed = Date.now() - t0;
    
    // Even-numbered IDs have matches
    assert.equal(rows.length, 500);
    // Should be fast (batch, not N+1). Allow generous 5s for CI
    assert.ok(elapsed < 5000, 'Took ' + elapsed + 'ms — possibly not decorrelated');
  });
});
