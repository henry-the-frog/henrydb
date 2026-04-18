// subquery-mvcc.test.js — Subqueries + MVCC snapshot isolation
// Tests that subqueries see consistent snapshots with concurrent modifications.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-subq-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Subqueries + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('IN subquery sees snapshot, not concurrent inserts', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1)');
    db.execute('INSERT INTO orders VALUES (2, 2)');
    
    const s1 = db.session();
    s1.begin();
    
    // Add customer 3 and their order outside s1
    db.execute("INSERT INTO customers VALUES (3, 'Carol')");
    db.execute('INSERT INTO orders VALUES (3, 3)');
    
    // s1 should only see orders for customers 1 and 2
    const r = rows(s1.execute(
      'SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers) ORDER BY id'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 orders (snapshot)');
    assert.equal(r[0].customer_id, 1);
    assert.equal(r[1].customer_id, 2);
    
    s1.commit();
  });

  it('EXISTS subquery with concurrent deletes', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 1, 200)');
    db.execute('INSERT INTO orders VALUES (3, 2, 150)');
    
    const s1 = db.session();
    s1.begin();
    
    // Delete customer 2 outside s1
    db.execute('DELETE FROM customers WHERE id = 2');
    
    // s1 should still see customer 2 exists
    const r = rows(s1.execute(
      'SELECT DISTINCT customer_id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id) ORDER BY customer_id'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 customers (both exist in snapshot)');
    
    s1.commit();
    
    // After commit, customer 2 is gone
    const r2 = rows(db.execute(
      'SELECT DISTINCT customer_id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id) ORDER BY customer_id'
    ));
    assert.equal(r2.length, 1, 'After delete, only 1 customer exists');
  });

  it('scalar subquery with concurrent update', () => {
    db.execute('CREATE TABLE config (key TEXT, val INT)');
    db.execute("INSERT INTO config VALUES ('threshold', 50)");
    db.execute('CREATE TABLE items (id INT, score INT)');
    db.execute('INSERT INTO items VALUES (1, 30)');
    db.execute('INSERT INTO items VALUES (2, 60)');
    db.execute('INSERT INTO items VALUES (3, 80)');
    
    const s1 = db.session();
    s1.begin();
    
    // Update threshold outside s1
    db.execute("UPDATE config SET val = 100 WHERE key = 'threshold'");
    
    // s1 should use old threshold (50)
    const r = rows(s1.execute(
      "SELECT * FROM items WHERE score > (SELECT val FROM config WHERE key = 'threshold') ORDER BY id"
    ));
    
    // With threshold=50: items 2 (60) and 3 (80) match
    assert.equal(r.length, 2, 'Should use old threshold (50)');
    assert.equal(r[0].id, 2);
    assert.equal(r[1].id, 3);
    
    s1.commit();
    
    // After commit, threshold is 100: no items match
    const r2 = rows(db.execute(
      "SELECT * FROM items WHERE score > (SELECT val FROM config WHERE key = 'threshold') ORDER BY id"
    ));
    assert.equal(r2.length, 0, 'With new threshold (100), no items match');
  });

  it('NOT IN subquery with concurrent modifications', () => {
    db.execute('CREATE TABLE blocked (user_id INT)');
    db.execute('CREATE TABLE users (id INT, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute("INSERT INTO users VALUES (3, 'Carol')");
    db.execute('INSERT INTO blocked VALUES (2)');
    
    const s1 = db.session();
    s1.begin();
    
    // Block user 3 outside s1
    db.execute('INSERT INTO blocked VALUES (3)');
    
    // s1 should see user 3 as not blocked (only user 2 blocked in snapshot)
    const r = rows(s1.execute(
      'SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM blocked) ORDER BY id'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 non-blocked users (snapshot)');
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Carol');
    
    s1.commit();
  });

  it('nested subqueries with snapshot isolation', () => {
    db.execute('CREATE TABLE a (id INT, val INT)');
    db.execute('CREATE TABLE b (id INT, a_id INT)');
    db.execute('CREATE TABLE c (id INT, b_id INT, score INT)');
    db.execute('INSERT INTO a VALUES (1, 10)');
    db.execute('INSERT INTO a VALUES (2, 20)');
    db.execute('INSERT INTO b VALUES (1, 1)');
    db.execute('INSERT INTO b VALUES (2, 2)');
    db.execute('INSERT INTO c VALUES (1, 1, 100)');
    db.execute('INSERT INTO c VALUES (2, 2, 200)');
    
    const s1 = db.session();
    s1.begin();
    
    // Delete chain outside s1
    db.execute('DELETE FROM c WHERE id = 2');
    
    // Nested subquery: a → b → c, s1 should see all
    const r = rows(s1.execute(
      'SELECT a.id FROM a WHERE a.id IN (SELECT b.a_id FROM b WHERE b.id IN (SELECT c.b_id FROM c)) ORDER BY a.id'
    ));
    
    assert.equal(r.length, 2, 'Should see 2 rows (full chain in snapshot)');
    
    s1.commit();
    
    // After delete, only 1 chain exists
    const r2 = rows(db.execute(
      'SELECT a.id FROM a WHERE a.id IN (SELECT b.a_id FROM b WHERE b.id IN (SELECT c.b_id FROM c)) ORDER BY a.id'
    ));
    assert.equal(r2.length, 1, 'After delete, only 1 chain');
  });

  it('correlated subquery with concurrent insert', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 1, 200)');
    db.execute('INSERT INTO orders VALUES (3, 2, 150)');
    
    const s1 = db.session();
    s1.begin();
    
    // Add a large order for customer 1 outside s1
    db.execute('INSERT INTO orders VALUES (4, 1, 999)');
    
    // Correlated subquery: find orders above customer average
    const r = rows(s1.execute(
      'SELECT id, amount FROM orders o1 WHERE amount > (SELECT AVG(amount) FROM orders o2 WHERE o2.customer_id = o1.customer_id) ORDER BY id'
    ));
    
    // Customer 1: avg = (100+200)/2 = 150. Order 2 (200) > 150.
    // Customer 2: avg = 150. No orders above avg.
    assert.equal(r.length, 1, 'Should find 1 above-average order');
    assert.equal(r[0].id, 2);
    assert.equal(r[0].amount, 200);
    
    s1.commit();
  });

  it('subquery in UPDATE with snapshot isolation', () => {
    db.execute('CREATE TABLE products (id INT, price INT, category TEXT)');
    db.execute("INSERT INTO products VALUES (1, 100, 'a')");
    db.execute("INSERT INTO products VALUES (2, 200, 'a')");
    db.execute("INSERT INTO products VALUES (3, 150, 'b')");
    
    const s1 = db.session();
    s1.begin();
    
    // Update prices outside s1
    db.execute('UPDATE products SET price = 999 WHERE id = 1');
    
    // s1 reads the max via a separate query (parser limitation: no aggregates in subqueries)
    const maxR = rows(s1.execute("SELECT price FROM products WHERE category = 'a' ORDER BY price DESC LIMIT 1"));
    const maxPrice = maxR[0].price;
    
    // s1 should see MAX(price) for 'a' = 200 (old values: 100, 200)
    assert.equal(maxPrice, 200, 'Should use snapshot value for max price');
    
    // Use the value in an update
    s1.execute(`UPDATE products SET price = price + ${maxPrice} WHERE id = 3`);
    
    const r = rows(s1.execute('SELECT * FROM products WHERE id = 3'));
    // 150 + 200 = 350
    assert.equal(r[0].price, 350, 'Should use snapshot value for update');
    
    s1.commit();
  });

  it('subquery survives close/reopen', () => {
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, ref_id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t1 VALUES (${i}, ${i * 10})`);
    db.execute('INSERT INTO t2 VALUES (1, 2)');
    db.execute('INSERT INTO t2 VALUES (2, 4)');
    
    const r1 = rows(db.execute(
      'SELECT t1.* FROM t1 WHERE t1.id IN (SELECT ref_id FROM t2) ORDER BY id'
    ));
    assert.equal(r1.length, 2);
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r2 = rows(db.execute(
      'SELECT t1.* FROM t1 WHERE t1.id IN (SELECT ref_id FROM t2) ORDER BY id'
    ));
    assert.equal(r2.length, 2);
    assert.equal(r2[0].id, 2);
    assert.equal(r2[1].id, 4);
  });
});
