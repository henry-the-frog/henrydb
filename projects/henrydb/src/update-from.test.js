// update-from.test.js — UPDATE ... FROM (PostgreSQL style)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPDATE ... FROM', () => {
  it('updates with values from another table', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT, name TEXT, price INT)');
    db.execute('CREATE TABLE updates (pid INT, new_price INT)');
    db.execute("INSERT INTO products VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
    db.execute("INSERT INTO updates VALUES (1, 15), (3, 25)");

    db.execute('UPDATE products SET price = updates.new_price FROM updates WHERE products.id = updates.pid');
    
    const r = db.execute('SELECT * FROM products ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.price), [15, 20, 25]);
  });

  it('UPDATE FROM with alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT, val TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'old1'), (2, 'old2')");
    db.execute("INSERT INTO t2 VALUES (1, 'new1')");

    db.execute('UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id');
    
    const r = db.execute('SELECT * FROM t1 ORDER BY id');
    assert.equal(r.rows[0].val, 'new1');
    assert.equal(r.rows[1].val, 'old2');
  });

  it('regular UPDATE still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20)');
    
    db.execute('UPDATE t SET val = val * 2 WHERE id = 1');
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].val, 20);
    assert.equal(r.rows[1].val, 20);
  });
});
