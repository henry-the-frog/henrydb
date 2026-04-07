// insert-select.test.js — INSERT INTO ... SELECT and UPDATE expressions tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('INSERT INTO ... SELECT', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE source (id INT PRIMARY KEY, name TEXT, value INT)');
    db.execute("INSERT INTO source VALUES (1, 'Alpha', 100)");
    db.execute("INSERT INTO source VALUES (2, 'Beta', 200)");
    db.execute("INSERT INTO source VALUES (3, 'Gamma', 300)");
  });

  it('basic INSERT SELECT', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, name TEXT, value INT)');
    const result = db.execute('INSERT INTO target SELECT * FROM source');
    assert.equal(result.count, 3);
    const rows = db.execute('SELECT * FROM target');
    assert.equal(rows.rows.length, 3);
  });

  it('INSERT SELECT with WHERE', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, name TEXT, value INT)');
    db.execute('INSERT INTO target SELECT * FROM source WHERE value > 150');
    const rows = db.execute('SELECT * FROM target');
    assert.equal(rows.rows.length, 2);
  });

  it('INSERT SELECT with specific columns', () => {
    db.execute('CREATE TABLE names (id INT PRIMARY KEY, label TEXT)');
    db.execute('INSERT INTO names SELECT id, name FROM source');
    const rows = db.execute('SELECT * FROM names');
    assert.equal(rows.rows.length, 3);
  });

  it('INSERT SELECT preserves data', () => {
    db.execute('CREATE TABLE backup (id INT PRIMARY KEY, name TEXT, value INT)');
    db.execute('INSERT INTO backup SELECT * FROM source');
    const orig = db.execute('SELECT id, name, value FROM source ORDER BY id');
    const copy = db.execute('SELECT id, name, value FROM backup ORDER BY id');
    assert.equal(orig.rows.length, copy.rows.length);
    for (let i = 0; i < orig.rows.length; i++) {
      assert.equal(orig.rows[i].id, copy.rows[i].id);
      assert.equal(orig.rows[i].name, copy.rows[i].name);
      assert.equal(orig.rows[i].value, copy.rows[i].value);
    }
  });

  it('INSERT SELECT with aggregates', () => {
    db.execute('CREATE TABLE stats (total_val INT)');
    db.execute('INSERT INTO stats SELECT SUM(value) AS total_val FROM source');
    const rows = db.execute('SELECT * FROM stats');
    assert.equal(rows.rows[0].total_val, 600);
  });

  it('INSERT SELECT from CTE', () => {
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, name TEXT, value INT)');
    db.execute('INSERT INTO target WITH high AS (SELECT * FROM source WHERE value >= 200) SELECT * FROM high');
    const rows = db.execute('SELECT * FROM target');
    assert.equal(rows.rows.length, 2);
  });
});
