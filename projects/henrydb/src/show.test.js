// show.test.js — SHOW command tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SHOW Commands', () => {
  it('SHOW TABLES lists all tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)');
    
    const r = db.execute('SHOW TABLES');
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.some(row => row.table_name === 'users'));
    assert.ok(r.rows.some(row => row.table_name === 'orders'));
  });

  it('SHOW TABLES with row counts', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    
    const r = db.execute('SHOW TABLES');
    assert.equal(r.rows[0].rows, 2);
  });

  it('SHOW TABLES empty database', () => {
    const db = new Database();
    const r = db.execute('SHOW TABLES');
    assert.equal(r.rows.length, 0);
  });

  it('SHOW CREATE TABLE generates DDL', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)');
    
    const r = db.execute('SHOW CREATE TABLE users');
    assert.ok(r.rows[0].sql.includes('CREATE TABLE'));
    assert.ok(r.rows[0].sql.includes('id INT PRIMARY KEY'));
    assert.ok(r.rows[0].sql.includes('name TEXT NOT NULL'));
  });

  it('SHOW COLUMNS FROM table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)');
    
    const r = db.execute('SHOW COLUMNS FROM t');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].column_name, 'id');
    assert.equal(r.rows[0].primary_key, true);
    assert.equal(r.rows[1].not_null, true);
  });

  it('SHOW CREATE TABLE for nonexistent table throws', () => {
    const db = new Database();
    assert.throws(() => db.execute('SHOW CREATE TABLE nope'), /not found/);
  });
});
