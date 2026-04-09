// show-catalog.test.js — Tests for SHOW TABLES, DESCRIBE, system catalog
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('System Catalog Queries', () => {
  it('SHOW TABLES lists all tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE alpha (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE beta (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE gamma (id INT PRIMARY KEY)');
    
    const r = db.execute('SHOW TABLES');
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows.some(t => t.table_name === 'alpha'));
    assert.ok(r.rows.some(t => t.table_name === 'beta'));
    assert.ok(r.rows.some(t => t.table_name === 'gamma'));
  });

  it('SHOW TABLES includes column and row count', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    
    const r = db.execute('SHOW TABLES');
    const users = r.rows.find(t => t.table_name === 'users');
    assert.ok(users);
    assert.strictEqual(users.columns, 3);
    assert.strictEqual(users.rows, 2);
  });

  it('DESCRIBE shows column details', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, active INT)');
    
    const r = db.execute('DESCRIBE products');
    assert.strictEqual(r.rows.length, 4);
    
    const idCol = r.rows.find(c => c.column_name === 'id');
    assert.ok(idCol);
    assert.strictEqual(idCol.type, 'INT');
    assert.strictEqual(idCol.primary_key, true);
    
    const nameCol = r.rows.find(c => c.column_name === 'name');
    assert.ok(nameCol);
    assert.strictEqual(nameCol.type, 'TEXT');
  });

  it('SHOW COLUMNS FROM works like DESCRIBE', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, label TEXT)');
    
    const r = db.execute('SHOW COLUMNS FROM items');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].column_name, 'id');
    assert.strictEqual(r.rows[1].column_name, 'label');
  });

  it('SHOW TABLES on empty database', () => {
    const db = new Database();
    const r = db.execute('SHOW TABLES');
    assert.strictEqual(r.rows.length, 0);
  });

  it('index count in SHOW TABLES', () => {
    const db = new Database();
    db.execute('CREATE TABLE indexed (id INT PRIMARY KEY, category TEXT, value INT)');
    db.execute('CREATE INDEX idx_cat ON indexed (category)');
    db.execute('CREATE INDEX idx_val ON indexed (value)');
    
    const r = db.execute('SHOW TABLES');
    const t = r.rows.find(t => t.table_name === 'indexed');
    assert.ok(t);
    assert.ok(t.indexes >= 3); // PK + 2 secondary
  });
});
