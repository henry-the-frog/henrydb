// join-mvcc.test.js — Complex JOINs + MVCC snapshot isolation
// Tests JOIN operations with concurrent modifications.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-join-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('JOINs + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INNER JOIN sees snapshot with concurrent insert on left table', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 2, 200)');
    
    const s1 = db.session();
    s1.begin();
    
    // Insert new order outside s1
    db.execute('INSERT INTO orders VALUES (3, 1, 300)');
    
    const r = rows(s1.execute(
      'SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 orders (snapshot)');
    s1.commit();
    
    // After commit, see all 3
    const r2 = rows(db.execute(
      'SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id'
    ));
    assert.equal(r2.length, 3, 'After commit, should see 3 orders');
  });

  it('INNER JOIN sees snapshot with concurrent insert on right table', () => {
    db.execute('CREATE TABLE items (id INT, cat_id INT, name TEXT)');
    db.execute('CREATE TABLE categories (id INT, name TEXT)');
    db.execute("INSERT INTO categories VALUES (1, 'electronics')");
    db.execute("INSERT INTO items VALUES (1, 1, 'phone')");
    db.execute("INSERT INTO items VALUES (2, 2, 'shirt')"); // orphan — no category 2
    
    const s1 = db.session();
    s1.begin();
    
    // Add category 2 outside s1
    db.execute("INSERT INTO categories VALUES (2, 'clothing')");
    
    // s1 should only see 1 matched row (phone→electronics)
    const r = rows(s1.execute(
      'SELECT i.name as item, c.name as category FROM items i JOIN categories c ON i.cat_id = c.id ORDER BY i.name'
    ));
    
    assert.equal(r.length, 1, 'Should see 1 matched pair (snapshot)');
    assert.equal(r[0].item, 'phone');
    
    s1.commit();
    
    // After commit, both match
    const r2 = rows(db.execute(
      'SELECT i.name as item, c.name as category FROM items i JOIN categories c ON i.cat_id = c.id ORDER BY i.name'
    ));
    assert.equal(r2.length, 2, 'After insert, both items have categories');
  });

  it('LEFT JOIN with concurrent delete on right table', () => {
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute('CREATE TABLE profiles (user_id INT, bio TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute("INSERT INTO profiles VALUES (1, 'Engineer')");
    db.execute("INSERT INTO profiles VALUES (2, 'Designer')");
    
    const s1 = db.session();
    s1.begin();
    
    // Delete Bob's profile outside s1
    db.execute('DELETE FROM profiles WHERE user_id = 2');
    
    // s1 should see both profiles (snapshot)
    const r = rows(s1.execute(
      'SELECT u.name, p.bio FROM users u LEFT JOIN profiles p ON u.id = p.user_id ORDER BY u.name'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 users');
    assert.equal(r[0].bio, 'Engineer'); // Alice
    assert.equal(r[1].bio, 'Designer'); // Bob still has profile in snapshot
    
    s1.commit();
    
    // After commit, Bob has no profile
    const r2 = rows(db.execute(
      'SELECT u.name, p.bio FROM users u LEFT JOIN profiles p ON u.id = p.user_id ORDER BY u.name'
    ));
    assert.equal(r2.length, 2, 'Still 2 users');
    assert.equal(r2[1].bio, null, 'Bob has no profile after delete');
  });

  it('self-JOIN with concurrent modifications', () => {
    db.execute('CREATE TABLE employees (id INT, name TEXT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'CEO', NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'VP', 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Dev', 2)");
    
    const s1 = db.session();
    s1.begin();
    
    // Change Dev's manager outside s1
    db.execute('UPDATE employees SET manager_id = 1 WHERE id = 3');
    
    // s1 should see original hierarchy: Dev→VP→CEO
    const r = rows(s1.execute(
      'SELECT e.name as employee, m.name as manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id'
    ));
    
    assert.equal(r.length, 3);
    const dev = r.find(x => x.employee === 'Dev');
    assert.equal(dev.manager, 'VP', 'Dev should report to VP in snapshot');
    
    s1.commit();
    
    // After commit, Dev→CEO
    const r2 = rows(db.execute(
      'SELECT e.name as employee, m.name as manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id'
    ));
    const dev2 = r2.find(x => x.employee === 'Dev');
    assert.equal(dev2.manager, 'CEO', 'Dev should report to CEO after update');
  });

  it('3-way JOIN with concurrent modifications on middle table', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT, product_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute('CREATE TABLE products (id INT, name TEXT, price INT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10)");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 20)");
    db.execute('INSERT INTO orders VALUES (1, 1, 1)');
    db.execute('INSERT INTO orders VALUES (2, 1, 2)');
    
    const s1 = db.session();
    s1.begin();
    
    // Update product price outside s1
    db.execute('UPDATE products SET price = 99 WHERE id = 1');
    
    // s1 should see original prices
    const r = rows(s1.execute(
      'SELECT c.name, p.name as product, p.price FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id ORDER BY o.id'
    ));
    
    assert.equal(r.length, 2);
    assert.equal(r[0].price, 10, 'Widget price should be 10 in snapshot');
    assert.equal(r[1].price, 20, 'Gadget price should be 20');
    
    s1.commit();
  });

  it('CROSS JOIN with concurrent modifications', () => {
    db.execute('CREATE TABLE colors (name TEXT)');
    db.execute('CREATE TABLE sizes (name TEXT)');
    db.execute("INSERT INTO colors VALUES ('red')");
    db.execute("INSERT INTO colors VALUES ('blue')");
    db.execute("INSERT INTO sizes VALUES ('S')");
    db.execute("INSERT INTO sizes VALUES ('M')");
    
    const s1 = db.session();
    s1.begin();
    
    // Add more options outside s1
    db.execute("INSERT INTO colors VALUES ('green')");
    db.execute("INSERT INTO sizes VALUES ('L')");
    
    // s1 should see 2×2=4 combinations
    const r = rows(s1.execute(
      'SELECT c.name as color, s.name as size FROM colors c, sizes s ORDER BY c.name, s.name'
    ));
    
    assert.equal(r.length, 4, 'Should see 2×2=4 combinations (snapshot)');
    
    s1.commit();
    
    // After commit: 3×3=9
    const r2 = rows(db.execute(
      'SELECT c.name as color, s.name as size FROM colors c, sizes s ORDER BY c.name, s.name'
    ));
    assert.equal(r2.length, 9, 'After inserts, 3×3=9 combinations');
  });

  it('JOIN with WHERE clause and concurrent modifications', () => {
    db.execute('CREATE TABLE orders (id INT, status TEXT, customer_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute("INSERT INTO orders VALUES (1, 'pending', 1)");
    db.execute("INSERT INTO orders VALUES (2, 'shipped', 1)");
    db.execute("INSERT INTO orders VALUES (3, 'pending', 2)");
    
    const s1 = db.session();
    s1.begin();
    
    // Ship order 1 outside s1
    db.execute("UPDATE orders SET status = 'shipped' WHERE id = 1");
    
    // s1 should see 2 pending orders (1 and 3)
    const r = rows(s1.execute(
      "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'pending' ORDER BY o.id"
    ));
    
    assert.equal(r.length, 2, 'Should see 2 pending orders (snapshot)');
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 3);
    
    s1.commit();
    
    // After commit, only 1 pending
    const r2 = rows(db.execute(
      "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'pending' ORDER BY o.id"
    ));
    assert.equal(r2.length, 1, 'After update, only 1 pending order');
    assert.equal(r2[0].id, 3);
  });

  it('JOIN result survives close/reopen', () => {
    db.execute('CREATE TABLE a (id INT, val TEXT)');
    db.execute('CREATE TABLE b (id INT, a_id INT, data TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO a VALUES (2, 'y')");
    db.execute("INSERT INTO b VALUES (1, 1, 'b1')");
    db.execute("INSERT INTO b VALUES (2, 2, 'b2')");
    
    const r1 = rows(db.execute(
      'SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id ORDER BY a.id'
    ));
    assert.equal(r1.length, 2);
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r2 = rows(db.execute(
      'SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id ORDER BY a.id'
    ));
    assert.equal(r2.length, 2);
    assert.equal(r2[0].val, 'x');
    assert.equal(r2[1].data, 'b2');
  });
});
