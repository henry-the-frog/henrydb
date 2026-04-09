// vacuum.test.js — Tests for VACUUM command
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('VACUUM', () => {
  it('VACUUM on empty database succeeds', () => {
    const db = new Database();
    const r = db.execute('VACUUM');
    assert.ok(r.message.includes('VACUUM'));
    assert.strictEqual(r.details.tablesProcessed, 0);
  });

  it('VACUUM processes all tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE c (id INT PRIMARY KEY)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (1)');
    
    const r = db.execute('VACUUM');
    assert.strictEqual(r.details.tablesProcessed, 3);
  });

  it('VACUUM specific table', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    const r = db.execute('VACUUM users');
    assert.strictEqual(r.details.tablesProcessed, 1);
  });

  it('VACUUM after DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO items VALUES (${i}, ${i})`);
    db.execute('DELETE FROM items WHERE id > 50');
    
    const r = db.execute('VACUUM items');
    assert.strictEqual(r.type, 'OK');
    assert.strictEqual(r.details.tablesProcessed, 1);
    
    // Data should still be queryable after vacuum
    const count = db.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(count.rows[0].cnt, 51);
  });

  it('VACUUM after heavy INSERT/DELETE cycle', () => {
    const db = new Database();
    db.execute('CREATE TABLE churn (id INT PRIMARY KEY, data TEXT)');
    
    // Insert 50, delete 30, insert 20
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO churn VALUES (${i}, 'batch1')`);
    db.execute('DELETE FROM churn WHERE id >= 20');
    for (let i = 50; i < 70; i++) db.execute(`INSERT INTO churn VALUES (${i}, 'batch2')`);
    
    const before = db.execute('SELECT COUNT(*) as cnt FROM churn');
    assert.strictEqual(before.rows[0].cnt, 40); // 20 from batch1 + 20 from batch2
    
    db.execute('VACUUM churn');
    
    const after = db.execute('SELECT COUNT(*) as cnt FROM churn');
    assert.strictEqual(after.rows[0].cnt, 40); // Same count after vacuum
  });

  it('VACUUM returns OK type', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    const r = db.execute('VACUUM');
    assert.strictEqual(r.type, 'OK');
    assert.ok(r.message);
    assert.ok(r.details);
  });
});
