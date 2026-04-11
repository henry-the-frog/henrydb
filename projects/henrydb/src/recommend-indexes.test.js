// recommend-indexes.test.js — Tests for RECOMMEND INDEXES SQL command
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RECOMMEND INDEXES command', () => {
  it('returns empty recommendation for fresh database', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    
    const result = db.execute('RECOMMEND INDEXES');
    assert.ok(result.rows);
    assert.equal(result.rows.length, 1);
    assert.ok(result.rows[0].recommendation.includes('No index'));
  });

  it('recommends index after repeated filter queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, '${i % 3 === 0 ? "shipped" : "pending"}', ${i * 1.5})`);
    }
    
    // Run filter queries to build workload
    for (let i = 0; i < 5; i++) {
      db.execute("SELECT * FROM orders WHERE status = 'shipped'");
    }
    
    const result = db.execute('RECOMMEND INDEXES');
    assert.ok(result.rows.length > 0);
    const statusRec = result.rows.find(r => r.columns === 'status');
    assert.ok(statusRec, 'Should recommend index on status');
    assert.ok(statusRec.sql.includes('CREATE INDEX'));
    assert.ok(statusRec.impact);
  });

  it('recommends index for JOIN columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO users VALUES (${i}, 'user${i}')`);
    for (let i = 1; i <= 500; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 100}, ${i * 1.5})`);
    
    for (let i = 0; i < 3; i++) {
      db.execute("SELECT * FROM orders o JOIN users u ON o.user_id = u.id");
    }
    
    const result = db.execute('RECOMMEND INDEXES');
    const joinRec = result.rows.find(r => r.columns === 'user_id');
    assert.ok(joinRec, 'Should recommend index on join column');
    assert.ok(joinRec.reason.includes('JOIN'));
  });

  it('tracks queries automatically via execute()', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL)');
    for (let i = 1; i <= 200; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, '${['electronics', 'books'][i % 2]}', ${i * 4.99})`);
    }
    
    // These queries should be automatically tracked
    db.execute("SELECT * FROM products WHERE category = 'electronics'");
    db.execute("SELECT * FROM products WHERE category = 'books' ORDER BY price DESC");
    db.execute("SELECT * FROM products ORDER BY price LIMIT 10");
    
    const result = db.execute('RECOMMEND INDEXES');
    assert.ok(result.rows.length > 0);
    // Should have recommendations for category and/or price
    const allCols = result.rows.map(r => r.columns);
    assert.ok(
      allCols.includes('category') || allCols.includes('price'),
      `Expected category or price in recommendations, got: ${allCols}`
    );
  });

  it('case insensitive command', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    
    const r1 = db.execute('RECOMMEND INDEXES');
    const r2 = db.execute('recommend indexes');
    assert.ok(r1.rows);
    assert.ok(r2.rows);
  });

  it('includes impact score and reason', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO items VALUES (${i}, 'item${i}', 'cat${i % 5}')`);
    
    for (let i = 0; i < 10; i++) {
      db.execute("SELECT * FROM items WHERE category = 'cat1'");
    }
    
    const result = db.execute('RECOMMEND INDEXES');
    const rec = result.rows.find(r => r.columns === 'category');
    assert.ok(rec);
    assert.ok(rec.score > 0, 'Should have a positive impact score');
    assert.ok(rec.reason, 'Should have a reason');
    assert.ok(['high', 'medium', 'low'].includes(rec.impact));
  });
});
