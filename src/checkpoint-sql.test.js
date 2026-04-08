// checkpoint-sql.test.js — CHECKPOINT SQL command tests
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

test('CHECKPOINT SQL command — basic execution', () => {
  const db = new Database();
  db.execute('CREATE TABLE users (id INT, name TEXT)');
  db.execute('INSERT INTO users VALUES (1, \'Alice\')');
  db.execute('INSERT INTO users VALUES (2, \'Bob\')');
  
  const result = db.execute('CHECKPOINT');
  assert.equal(result.type, 'CHECKPOINT');
  assert.ok(result.message.includes('CHECKPOINT complete'));
  assert.ok(result.details);
  assert.equal(result.details.tables, 1);
});

test('CHECKPOINT — works with empty database', () => {
  const db = new Database();
  const result = db.execute('CHECKPOINT');
  assert.equal(result.type, 'CHECKPOINT');
  assert.equal(result.details.tables, 0);
});

test('CHECKPOINT — works with multiple tables', () => {
  const db = new Database();
  db.execute('CREATE TABLE users (id INT, name TEXT)');
  db.execute('CREATE TABLE orders (id INT, user_id INT)');
  db.execute('CREATE TABLE products (id INT, name TEXT)');
  
  const result = db.execute('CHECKPOINT');
  assert.equal(result.details.tables, 3);
});

test('CHECKPOINT — parser handles keyword correctly', () => {
  const db = new Database();
  // CHECKPOINT should not conflict with any column/table names
  db.execute('CREATE TABLE checkpoint_log (id INT, ts TEXT)');
  db.execute('INSERT INTO checkpoint_log VALUES (1, \'2026-04-07\')');
  
  const result = db.execute('CHECKPOINT');
  assert.equal(result.type, 'CHECKPOINT');
});

test('CHECKPOINT — can be called repeatedly', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (x INT)');
  
  for (let i = 0; i < 5; i++) {
    db.execute(`INSERT INTO t VALUES (${i})`);
    const r = db.execute('CHECKPOINT');
    assert.equal(r.type, 'CHECKPOINT');
  }
});
