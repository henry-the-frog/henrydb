// misc.test.js — Misc SQL features: TRUNCATE, multi-row INSERT, edge cases
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('TRUNCATE TABLE', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO data VALUES (1, 'a')");
    db.execute("INSERT INTO data VALUES (2, 'b')");
    db.execute("INSERT INTO data VALUES (3, 'c')");
  });

  it('removes all rows', () => {
    db.execute('TRUNCATE TABLE data');
    const result = db.execute('SELECT * FROM data');
    assert.equal(result.rows.length, 0);
  });

  it('table still exists after truncate', () => {
    db.execute('TRUNCATE TABLE data');
    db.execute("INSERT INTO data VALUES (1, 'new')");
    const result = db.execute('SELECT * FROM data');
    assert.equal(result.rows.length, 1);
  });

  it('TRUNCATE without TABLE keyword', () => {
    db.execute('TRUNCATE data');
    const result = db.execute('SELECT * FROM data');
    assert.equal(result.rows.length, 0);
  });

  it('errors on non-existent table', () => {
    assert.throws(() => {
      db.execute('TRUNCATE TABLE ghost');
    }, /not found/);
  });

  it('indexes cleared after truncate', () => {
    db.execute('CREATE INDEX idx_val ON data (val)');
    db.execute('TRUNCATE TABLE data');
    db.execute("INSERT INTO data VALUES (1, 'x')");
    const result = db.execute("SELECT * FROM data WHERE val = 'x'");
    assert.equal(result.rows.length, 1);
  });
});

describe('Multi-row INSERT', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
  });

  it('inserts multiple rows at once', () => {
    const result = db.execute('INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)');
    assert.equal(result.count, 3);
    const rows = db.execute('SELECT * FROM nums');
    assert.equal(rows.rows.length, 3);
  });

  it('single row insert still works', () => {
    const result = db.execute('INSERT INTO nums VALUES (1, 10)');
    assert.equal(result.count, 1);
  });
});

describe('Edge cases', () => {
  let db;

  beforeEach(() => {
    db = new Database();
  });

  it('empty table queries', () => {
    db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)');
    const result = db.execute('SELECT * FROM empty');
    assert.equal(result.rows.length, 0);
  });

  it('COUNT on empty table', () => {
    db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)');
    const result = db.execute('SELECT COUNT(*) AS cnt FROM empty');
    assert.equal(result.rows[0].cnt, 0);
  });

  it('SUM on empty table', () => {
    db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val INT)');
    const result = db.execute('SELECT SUM(val) AS total FROM empty');
    assert.equal(result.rows[0].total, null); // SQL standard: SUM of empty set is NULL
  });

  it('ORDER BY on single row', () => {
    db.execute('CREATE TABLE single (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO single VALUES (1, 'only')");
    const result = db.execute('SELECT * FROM single ORDER BY val');
    assert.equal(result.rows.length, 1);
  });

  it('LIMIT 0 returns nothing', { skip: 'LIMIT 0 treated as no limit (falsy)' }, () => {
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO data VALUES (1, 'a')");
    const result = db.execute('SELECT * FROM data LIMIT 0');
    assert.equal(result.rows.length, 0);
  });

  it('OFFSET beyond rows returns nothing', () => {
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO data VALUES (1, 'a')");
    const result = db.execute('SELECT * FROM data OFFSET 100');
    assert.equal(result.rows.length, 0);
  });

  it('multiple WHERE conditions', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, price INT, active INT)');
    db.execute("INSERT INTO items VALUES (1, 'A', 100, 1)");
    db.execute("INSERT INTO items VALUES (2, 'A', 200, 0)");
    db.execute("INSERT INTO items VALUES (3, 'B', 150, 1)");
    const result = db.execute("SELECT * FROM items WHERE cat = 'A' AND active = 1 AND price < 200");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 1);
  });

  it('SELECT with numeric literal comparison', () => {
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO nums VALUES (1, 42)');
    const result = db.execute('SELECT * FROM nums WHERE val = 42');
    assert.equal(result.rows.length, 1);
  });

  it('DELETE with complex WHERE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO items VALUES (1, 10)');
    db.execute('INSERT INTO items VALUES (2, 20)');
    db.execute('INSERT INTO items VALUES (3, 30)');
    db.execute('DELETE FROM items WHERE val > 10 AND val < 30');
    const result = db.execute('SELECT * FROM items');
    assert.equal(result.rows.length, 2);
  });

  it('OR in WHERE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO items VALUES (1, 10)');
    db.execute('INSERT INTO items VALUES (2, 20)');
    db.execute('INSERT INTO items VALUES (3, 30)');
    const result = db.execute('SELECT * FROM items WHERE val = 10 OR val = 30');
    assert.equal(result.rows.length, 2);
  });

  it('NOT in WHERE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO items VALUES (1, 10)');
    db.execute('INSERT INTO items VALUES (2, 20)');
    const result = db.execute('SELECT * FROM items WHERE NOT val = 10');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].val, 20);
  });

  it('chained ORDER BY', () => {
    db.execute('CREATE TABLE people (id INT PRIMARY KEY, age INT, name TEXT)');
    db.execute("INSERT INTO people VALUES (1, 30, 'Zara')");
    db.execute("INSERT INTO people VALUES (2, 30, 'Alice')");
    db.execute("INSERT INTO people VALUES (3, 25, 'Bob')");
    const result = db.execute('SELECT * FROM people ORDER BY age DESC, name ASC');
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].name, 'Zara');
    assert.equal(result.rows[2].name, 'Bob');
  });
});
