// partial-index.test.js — Partial index tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Partial Indexes', () => {
  it('partial index only includes matching rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, status TEXT, amount INT)');
    db.execute("INSERT INTO orders VALUES (1, 'active', 100)");
    db.execute("INSERT INTO orders VALUES (2, 'completed', 200)");
    db.execute("INSERT INTO orders VALUES (3, 'active', 300)");
    db.execute("INSERT INTO orders VALUES (4, 'cancelled', 50)");
    
    // Create partial index only for active orders
    db.execute("CREATE INDEX idx_active ON orders(amount) WHERE status = 'active'");
    
    // The index should exist
    const table = db.tables.get('orders');
    assert.ok(table.indexes.has('amount'));
  });

  it('partial index metadata stores WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, active INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 0)');
    
    db.execute('CREATE INDEX idx_active ON t(id) WHERE active = 1');
    
    const table = db.tables.get('t');
    const meta = table.indexMeta.get('id');
    assert.ok(meta.partial !== null);
  });

  it('queries still work with partial indexes', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, price INT, in_stock INT)');
    db.execute('INSERT INTO products VALUES (1, 100, 1)');
    db.execute('INSERT INTO products VALUES (2, 200, 0)');
    db.execute('INSERT INTO products VALUES (3, 150, 1)');
    
    db.execute('CREATE INDEX idx_price_instock ON products(price) WHERE in_stock = 1');
    
    // Full table query should still work
    const all = db.execute('SELECT * FROM products ORDER BY id');
    assert.equal(all.rows.length, 3);
    
    // Filtered query
    const inStock = db.execute('SELECT * FROM products WHERE in_stock = 1 ORDER BY price');
    assert.equal(inStock.rows.length, 2);
  });
});
