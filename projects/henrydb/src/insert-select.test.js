// insert-select.test.js — INSERT INTO ... SELECT
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('INSERT INTO ... SELECT', () => {
  it('copies rows from one table to another', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, name TEXT)');
    db.execute('CREATE TABLE dst (id INT, name TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'A'), (2, 'B'), (3, 'C')");
    
    db.execute('INSERT INTO dst SELECT * FROM src WHERE id <= 2');
    const r = db.execute('SELECT * FROM dst ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'A');
  });

  it('with explicit column list', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, name TEXT, extra TEXT)');
    db.execute('CREATE TABLE dst (id INT, name TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'Alice', 'x')");
    
    db.execute('INSERT INTO dst (id, name) SELECT id, name FROM src');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('with aggregation in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (region TEXT, amount INT)');
    db.execute('CREATE TABLE summary (region TEXT, total INT)');
    db.execute("INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 30)");
    
    db.execute('INSERT INTO summary SELECT region, SUM(amount) FROM sales GROUP BY region');
    const r = db.execute('SELECT * FROM summary ORDER BY region');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].total, 30);
  });
});
