// hot-update-integration.test.js — Tests that UPDATE automatically uses HOT when possible
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('HOT Update Integration', () => {
  it('non-indexed column update uses HOT (no index churn)', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute('CREATE INDEX idx_age ON users (age)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    
    // Update non-indexed column — should use HOT
    db.execute("UPDATE users SET name = 'Alice-Updated' WHERE id = 1");
    
    const r = db.execute('SELECT * FROM users WHERE id = 1');
    assert.equal(r.rows[0].name, 'Alice-Updated');
    assert.equal(r.rows[0].age, 30); // Unchanged
    
    // Index scan on age should still work
    const r2 = db.execute('SELECT * FROM users WHERE age = 30');
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].name, 'Alice-Updated');
  });

  it('indexed column update uses regular path (index updated)', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT)');
    db.execute('CREATE INDEX idx_price ON items (price)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)");
    
    // Update indexed column — must use regular update
    db.execute('UPDATE items SET price = 19.99 WHERE id = 1');
    
    // Old index value should not find the row
    const r1 = db.execute('SELECT * FROM items WHERE price = 9.99');
    assert.equal(r1.rows.length, 0, 'Old index value should not match');
    
    // New index value should find the row
    const r2 = db.execute('SELECT * FROM items WHERE price = 19.99');
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].name, 'Widget');
  });

  it('multiple HOT updates on same row work', () => {
    const db = new Database();
    db.execute('CREATE TABLE logs (id INT PRIMARY KEY, status TEXT, count INT)');
    db.execute('CREATE INDEX idx_count ON logs (count)');
    db.execute("INSERT INTO logs VALUES (1, 'active', 0)");
    
    // Multiple updates on non-indexed column
    for (let i = 1; i <= 10; i++) {
      db.execute(`UPDATE logs SET status = 'v${i}' WHERE id = 1`);
    }
    
    const r = db.execute('SELECT * FROM logs WHERE id = 1');
    assert.equal(r.rows[0].status, 'v10');
    assert.equal(r.rows[0].count, 0);
    
    // Should still have only 1 logical row
    const all = db.execute('SELECT COUNT(*) as cnt FROM logs');
    assert.equal(all.rows[0].cnt, 1);
  });

  it('mixed HOT and regular updates', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price FLOAT)');
    db.execute('CREATE INDEX idx_cat ON products (category)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 'A', 10)");
    
    // HOT update (name is not indexed)
    db.execute("UPDATE products SET name = 'Super Widget' WHERE id = 1");
    
    // Regular update (category IS indexed)
    db.execute("UPDATE products SET category = 'B' WHERE id = 1");
    
    // HOT update again
    db.execute("UPDATE products SET price = 15 WHERE id = 1");
    
    const r = db.execute('SELECT * FROM products WHERE id = 1');
    assert.equal(r.rows[0].name, 'Super Widget');
    assert.equal(r.rows[0].category, 'B');
    assert.equal(r.rows[0].price, 15);
    
    // Category index should work
    const r2 = db.execute("SELECT * FROM products WHERE category = 'B'");
    assert.equal(r2.rows.length, 1);
    
    const r3 = db.execute("SELECT * FROM products WHERE category = 'A'");
    assert.equal(r3.rows.length, 0);
  });

  it('HOT works with RETURNING clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const r = db.execute("UPDATE t SET val = 'b' WHERE id = 1 RETURNING *");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'b');
  });

  it('table with no indexes always uses HOT', () => {
    const db = new Database();
    db.execute('CREATE TABLE simple (id INT, name TEXT)');
    db.execute("INSERT INTO simple VALUES (1, 'Alice'), (2, 'Bob')");
    
    db.execute("UPDATE simple SET name = 'Alice-Updated' WHERE id = 1");
    
    const r = db.execute('SELECT * FROM simple ORDER BY id');
    assert.equal(r.rows[0].name, 'Alice-Updated');
    assert.equal(r.rows[1].name, 'Bob');
  });

  it('EXPLAIN ANALYZE shows scan after HOT update', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, val INT, data TEXT)');
    db.execute('CREATE INDEX idx_val ON bench (val)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO bench VALUES (${i}, ${i}, 'data')`);
    
    // HOT update (data is not indexed)
    db.execute("UPDATE bench SET data = 'updated' WHERE val = 25");
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM bench WHERE val = 25');
    assert.equal(r.actual_rows, 1);
    assert.ok(r.text.includes('Scan'));
  });
});
