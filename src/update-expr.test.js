// update-expr.test.js — UPDATE with expression tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPDATE with Expressions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)');
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
    db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750)");
  });

  it('SET col = col + value', () => {
    db.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 1');
    const result = db.execute('SELECT * FROM accounts WHERE id = 1');
    assert.equal(result.rows[0].balance, 1100);
  });

  it('SET col = col - value', () => {
    db.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 2');
    const result = db.execute('SELECT * FROM accounts WHERE id = 2');
    assert.equal(result.rows[0].balance, 300);
  });

  it('SET col = col * value', () => {
    db.execute('UPDATE accounts SET balance = balance * 2 WHERE id = 3');
    const result = db.execute('SELECT * FROM accounts WHERE id = 3');
    assert.equal(result.rows[0].balance, 1500);
  });

  it('SET col = literal (still works)', () => {
    db.execute('UPDATE accounts SET balance = 999 WHERE id = 1');
    const result = db.execute('SELECT * FROM accounts WHERE id = 1');
    assert.equal(result.rows[0].balance, 999);
  });

  it('SET with UPPER function', () => {
    db.execute("UPDATE accounts SET name = UPPER(name) WHERE id = 1");
    const result = db.execute('SELECT * FROM accounts WHERE id = 1');
    assert.equal(result.rows[0].name, 'ALICE');
  });

  it('SET multiple columns with expressions', () => {
    db.execute("UPDATE accounts SET balance = balance + 50, name = UPPER(name) WHERE id = 2");
    const result = db.execute('SELECT * FROM accounts WHERE id = 2');
    assert.equal(result.rows[0].balance, 550);
    assert.equal(result.rows[0].name, 'BOB');
  });

  it('UPDATE all rows with expression', () => {
    db.execute('UPDATE accounts SET balance = balance + 100');
    const result = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.equal(result.rows[0].balance, 1100);
    assert.equal(result.rows[1].balance, 600);
    assert.equal(result.rows[2].balance, 850);
  });

  it('SET with COALESCE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO items VALUES (1, NULL)');
    db.execute('UPDATE items SET val = COALESCE(val, 42) WHERE id = 1');
    const result = db.execute('SELECT * FROM items WHERE id = 1');
    assert.equal(result.rows[0].val, 42);
  });
});
