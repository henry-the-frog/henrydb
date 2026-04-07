// union.test.js — UNION tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UNION', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, city TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'NYC')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob', 'LA')");
    db.execute("INSERT INTO customers VALUES (3, 'Charlie', 'NYC')");

    db.execute('CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT, city TEXT)');
    db.execute("INSERT INTO suppliers VALUES (1, 'SupA', 'NYC')");
    db.execute("INSERT INTO suppliers VALUES (2, 'SupB', 'Chicago')");
    db.execute("INSERT INTO suppliers VALUES (3, 'Alice', 'NYC')"); // duplicate with customers
  });

  it('UNION removes duplicates', () => {
    const result = db.execute('SELECT city FROM customers UNION SELECT city FROM suppliers');
    const cities = result.rows.map(r => r.city).sort();
    assert.deepEqual(cities, ['Chicago', 'LA', 'NYC']); // NYC appears once
  });

  it('UNION ALL keeps duplicates', () => {
    const result = db.execute('SELECT city FROM customers UNION ALL SELECT city FROM suppliers');
    assert.equal(result.rows.length, 6); // 3 + 3
  });

  it('UNION with different data', () => {
    const result = db.execute('SELECT name FROM customers UNION SELECT name FROM suppliers');
    assert.equal(result.rows.length, 5); // Alice appears in both, so 6 - 1 = 5
  });

  it('UNION ALL with different data', () => {
    const result = db.execute('SELECT name FROM customers UNION ALL SELECT name FROM suppliers');
    assert.equal(result.rows.length, 6);
  });

  it('UNION with WHERE', () => {
    const result = db.execute("SELECT name FROM customers WHERE city = 'NYC' UNION SELECT name FROM suppliers WHERE city = 'NYC'");
    const names = result.rows.map(r => r.name).sort();
    assert.deepEqual(names, ['Alice', 'Charlie', 'SupA']); // Alice deduped
  });

  it('UNION with multiple columns', () => {
    const result = db.execute('SELECT name, city FROM customers UNION SELECT name, city FROM suppliers');
    // Alice/NYC appears in both — should deduplicate
    const aliceNYC = result.rows.filter(r => r.name === 'Alice' && r.city === 'NYC');
    assert.equal(aliceNYC.length, 1);
  });

  it('UNION with single table', () => {
    const result = db.execute("SELECT name FROM customers WHERE city = 'NYC' UNION SELECT name FROM customers WHERE city = 'LA'");
    assert.equal(result.rows.length, 3); // Alice, Charlie, Bob
  });

  it('UNION ALL preserves order', () => {
    const result = db.execute('SELECT name FROM customers UNION ALL SELECT name FROM suppliers');
    // First 3 from customers, next 3 from suppliers
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[3].name, 'SupA');
  });
});
