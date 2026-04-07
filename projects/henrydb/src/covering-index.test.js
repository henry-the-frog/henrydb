// covering-index.test.js — Covering indexes and index-only scans
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Covering Indexes', () => {
  it('CREATE INDEX with INCLUDE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com', 25)");
    
    db.execute('CREATE INDEX idx_name ON users (name) INCLUDE (email)');
    
    // The index should exist on 'name'
    const table = db.tables.get('users');
    assert.ok(table.indexes.has('name'));
    assert.ok(table.indexMeta);
    const meta = table.indexMeta.get('name');
    assert.deepEqual(meta.include, ['email']);
  });

  it('index-only scan when all columns covered', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, sku TEXT, name TEXT, price INT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'SKU${i}', 'Product ${i}', ${10 + i})`);
    }
    
    db.execute('CREATE INDEX idx_sku ON products (sku) INCLUDE (name, price)');
    
    // Query that only needs sku, name, price — covered by index
    const result = db.execute("SELECT sku, name, price FROM products WHERE sku = 'SKU42'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].sku, 'SKU42');
    assert.equal(result.rows[0].name, 'Product 42');
    assert.equal(result.rows[0].price, 52);
  });

  it('falls back to heap when columns not covered', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 'y', 'z')");
    
    db.execute('CREATE INDEX idx_a ON t (a) INCLUDE (b)');
    
    // Needs c which is NOT in the covering index
    const result = db.execute("SELECT a, b, c FROM t WHERE a = 'x'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].c, 'z');
  });

  it('SELECT * always uses heap access', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    
    db.execute('CREATE INDEX idx_val ON t (val) INCLUDE (id)');
    
    const result = db.execute("SELECT * FROM t WHERE val = 'test'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 1);
  });
});
