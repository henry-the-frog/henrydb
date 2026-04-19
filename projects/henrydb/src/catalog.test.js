// catalog.test.js — SHOW TABLES, SHOW COLUMNS, SHOW INDEXES, information_schema

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Catalog Introspection', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id), total INT DEFAULT 0)');
    db.execute('CREATE INDEX idx_orders_user ON orders(user_id)');
  });

  it('SHOW TABLES', () => {
    const r = db.execute('SHOW TABLES');
    assert.equal(r.rows.length, 2);
    const names = r.rows.map(r => r.table_name).sort();
    assert.deepStrictEqual(names, ['orders', 'users']);
  });

  it('SHOW COLUMNS FROM table', () => {
    const r = db.execute('SHOW COLUMNS FROM users');
    assert.equal(r.rows.length, 3);
    const id = r.rows.find(r => r.column_name === 'id');
    assert.equal(id.primary_key, true);
    assert.equal(id.type, 'INT');
  });

  it('SHOW INDEXES FROM table', () => {
    const r = db.execute('SHOW INDEXES FROM orders');
    assert.ok(r.rows.length >= 1);
    const userIdx = r.rows.find(r => r.index_name === 'idx_orders_user');
    assert.ok(userIdx);
    assert.equal(userIdx.columns, 'user_id');
  });

  it('information_schema.tables', () => {
    const r = db.execute('SELECT table_name FROM information_schema.tables ORDER BY table_name');
    assert.deepStrictEqual(r.rows.map(r => r.table_name), ['orders', 'users']);
  });

  it('information_schema.columns', () => {
    const r = db.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position");
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].column_name, 'id');
  });

  it('DESCRIBE table (alias for SHOW COLUMNS)', () => {
    const r = db.execute('DESCRIBE users');
    assert.equal(r.rows.length, 3);
  });

  it('SHOW CREATE TABLE', () => {
    const r = db.execute('SHOW CREATE TABLE users');
    assert.ok(r.rows.length > 0);
  });
});
