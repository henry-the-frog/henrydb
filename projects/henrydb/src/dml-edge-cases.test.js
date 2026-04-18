// dml-edge-cases.test.js — DELETE/UPDATE edge cases with MVCC
// Tests adversarial DML patterns that commonly break MVCC implementations.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-dml-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('DELETE/UPDATE Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DELETE WHERE IN (SELECT ...) with subquery referencing same table', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    // Delete rows where val > average (self-referencing subquery)
    db.execute('DELETE FROM t WHERE val > (SELECT AVG(val) FROM t)');
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    // AVG = 55. Rows with val > 55: 6,7,8,9,10 deleted. Remaining: 1-5
    assert.equal(r.length, 5, 'Should have 5 rows after delete');
    assert.equal(r[4].id, 5);
  });

  it('UPDATE SET col = (SELECT ...) with scalar subquery', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE TABLE config (multiplier INT)');
    db.execute('INSERT INTO config VALUES (3)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    db.execute('UPDATE t SET val = val * (SELECT multiplier FROM config)');
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r[0].val, 30); // 10 * 3
    assert.equal(r[1].val, 60); // 20 * 3
  });

  it('DELETE all rows then INSERT in same transaction', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'old')");
    db.execute("INSERT INTO t VALUES (2, 'old')");
    
    const s = db.session();
    s.begin();
    s.execute('DELETE FROM t');
    s.execute("INSERT INTO t VALUES (3, 'new')");
    
    // Within tx, should see only the new row
    const r = rows(s.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'new');
    
    s.commit();
    
    const r2 = rows(db.execute('SELECT * FROM t'));
    assert.equal(r2.length, 1);
    assert.equal(r2[0].id, 3);
  });

  it('UPDATE same row twice in one transaction', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const s = db.session();
    s.begin();
    s.execute('UPDATE t SET val = 20 WHERE id = 1');
    s.execute('UPDATE t SET val = 30 WHERE id = 1');
    
    const r = rows(s.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r[0].val, 30, 'Should see second update');
    
    s.commit();
    
    const r2 = rows(db.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r2[0].val, 30);
  });

  it('concurrent DELETE of same row — only first succeeds', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'target')");
    
    const s1 = db.session();
    s1.begin();
    const s2 = db.session();
    s2.begin();
    
    // Both try to delete the same row
    s1.execute('DELETE FROM t WHERE id = 1');
    
    // s1 commits first
    s1.commit();
    
    // s2 should either fail or see no rows to delete
    try {
      s2.execute('DELETE FROM t WHERE id = 1');
      s2.commit();
    } catch (e) {
      // Write conflict is acceptable
      assert.ok(true, `Write conflict: ${e.message}`);
    }
    
    // Table should be empty
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 0, 'Row should be deleted');
  });

  it('UPDATE with arithmetic expression', () => {
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    
    db.execute('UPDATE t SET a = a + b, b = a - b WHERE id = 1');
    
    const r = rows(db.execute('SELECT * FROM t WHERE id = 1'));
    // a = 10 + 5 = 15, b = 10 - 5 = 5 (using original values)
    // Note: in PostgreSQL, SET uses old values for all expressions
    assert.equal(r[0].a, 15);
    assert.equal(r[0].b, 5);
  });

  it('DELETE with complex WHERE clause', () => {
    db.execute('CREATE TABLE t (id INT, cat TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'a', 90)");
    db.execute("INSERT INTO t VALUES (2, 'b', 80)");
    db.execute("INSERT INTO t VALUES (3, 'a', 70)");
    db.execute("INSERT INTO t VALUES (4, 'b', 60)");
    db.execute("INSERT INTO t VALUES (5, 'a', 50)");
    
    // Delete rows where cat='a' AND score < 80
    db.execute("DELETE FROM t WHERE cat = 'a' AND score < 80");
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3); // rows 1, 2, 4 remain
    assert.equal(r[0].id, 1); // cat='a', score=90 (not < 80)
    assert.equal(r[1].id, 2); // cat='b'
    assert.equal(r[2].id, 4); // cat='b'
  });

  it('UPDATE with WHERE IN subquery', () => {
    db.execute('CREATE TABLE items (id INT, price INT, category TEXT)');
    db.execute("INSERT INTO items VALUES (1, 100, 'a')");
    db.execute("INSERT INTO items VALUES (2, 200, 'b')");
    db.execute("INSERT INTO items VALUES (3, 300, 'a')");
    db.execute('CREATE TABLE discounted (category TEXT)');
    db.execute("INSERT INTO discounted VALUES ('a')");
    
    db.execute('UPDATE items SET price = price * 0.9 WHERE category IN (SELECT category FROM discounted)');
    
    const r = rows(db.execute('SELECT * FROM items ORDER BY id'));
    assert.equal(r[0].price, 90);  // 100 * 0.9 (category a)
    assert.equal(r[1].price, 200); // unchanged (category b)
    assert.equal(r[2].price, 270); // 300 * 0.9 (category a)
  });

  it('DELETE WHERE NOT EXISTS (orphan cleanup)', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute('INSERT INTO orders VALUES (1, 1)');
    db.execute('INSERT INTO orders VALUES (2, 999)'); // orphan — no customer 999
    
    db.execute('DELETE FROM orders WHERE NOT EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id)');
    
    const r = rows(db.execute('SELECT * FROM orders'));
    assert.equal(r.length, 1, 'Only non-orphan order should remain');
    assert.equal(r[0].customer_id, 1);
  });

  it('DML survives close/reopen', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = 99 WHERE id = 1');
    db.execute('DELETE FROM t WHERE id = 2');
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
    assert.equal(r[0].val, 99);
  });
});
